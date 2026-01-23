/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulTask.h"
#include "velox/experimental/stateful/StreamElement.h"

#include <iostream>

namespace facebook::velox::stateful {

void StatefulOperator::initialize() {
  operator_->initialize();
  for (auto& target : targets_) {
    target->initialize();
  }
  combinedWatermarkStatus_ = std::make_unique<CombinedWatermarkStatus>(numInputs());
}
  
bool StatefulOperator::isFinished() {
  return operator_->isFinished();
}

void StatefulOperator::addInput(RowVectorPtr input) {
  operator_->traceInput(input);
  operator_->addInput(std::move(input));
}

bool StatefulOperator::sourceEmpty() {
  return sourceEmpty_;
}

void StatefulOperator::close() {
  operator_->close();
  for (auto& target : targets_) {
    target->close();
  }
  operator_.reset();
  targets_.clear();
}

void StatefulOperator::getOutput() {
  sourceEmpty_ = true;
  auto intermediateResult = operator_->getOutput();
  if (!intermediateResult) {
    return;
  }
  sourceEmpty_ = false;
  pushOutput(std::move(intermediateResult));
}

void StatefulOperator::pushOutput(RowVectorPtr output) {
  if (targets_.empty()) {
    auto outNodeId = operator_->planNodeId();
    auto task = std::static_pointer_cast<StatefulTask>(operator_->operatorCtx()->driverCtx()->task);
    task->addOutput(std::make_shared<StreamRecord>(outNodeId, std::move(output)));
    return;
  }
  for (int i = 0; i < targets_.size() - 1; i++) {
    auto copy = output;
    targets_[i]->addInput(std::move(copy));
    targets_[i]->getOutput();
  }
  targets_[targets_.size() - 1]->addInput(std::move(output));
  targets_[targets_.size() - 1]->getOutput();
}

void StatefulOperator::emitWatermark(long timestamp) {
  // If the current task has only one operator, forward the watermark directly to Flink.
  // Otherwise, forward the watermark to downstream operators.
  if (targets_.empty()) {
    auto outNodeId = operator_->planNodeId();
    auto task = std::static_pointer_cast<StatefulTask>(operator_->operatorCtx()->driverCtx()->task);
    task->addOutput(std::make_shared<Watermark>(outNodeId, timestamp));
    return;
  }
  for (auto& target : targets_) {
    target->processWatermark(timestamp);
  }
}

void StatefulOperator::processWatermark(long timestamp, int index) {
  if (combinedWatermarkStatus_->updateWatermark(index, timestamp)) {
    // If the watermark is updated, we need to advance the timer service.
    long combinedWatermark = combinedWatermarkStatus_->getCombinedWatermark();
    processWatermark(combinedWatermark);
  }
}

void StatefulOperator::processWatermark(long timestamp) {
  emitWatermark(timestamp);
}

void StatefulOperator::initializeState(StateBackend* stateBackend) {
  if (!stateHandler_) {
    KeyedStateBackendParameters parameters(
        op()->operatorCtx()->driverCtx()->task->taskId(), op()->operatorId());
    stateHandler_ = std::make_shared<StreamOperatorStateHandler>(
        op()->operatorId(),
        stateBackend->createKeyedStateBackend(
            KeyedStateBackendParameters(
                op()->operatorCtx()->driverCtx()->task->taskId(),
                op()->operatorId())));
  }
  auto snapshotable = dynamic_cast<Snapshotable*>(op().get());
  if (snapshotable) {
    // TODO: flink restore is a seperated logic
    // snapshotable->initializeState();
  }
  // TODO: flink restore is a seperated logic
  // stateHandler_->initializeState();
  for (auto& target : targets_) {
    target->initializeState(stateBackend);
  }
}

void StatefulOperator::snapshotState() {
  stateHandler_->snapshotState();
  auto snapshotable = dynamic_cast<Snapshotable*>(op().get());
  if (snapshotable) {
    // If the operator is checkpointable, we need to snapshot it.
    snapshotable->snapshot(0, 0, CheckpointOptions::defaultOptions());
  }
  for (auto& target : targets_) {
    target->snapshotState();
  }
}

std::vector<std::string> StatefulOperator::notifyCheckpointComplete(long checkpointId) {
  stateHandler_->notifyCheckpointComplete(checkpointId);
  auto checkpointListener = dynamic_cast<CheckpointListener*>(op().get());
  if (checkpointListener) {
    // If the operator is checkpointable, we need to snapshot it.
    checkpointListener->notifyCheckpointComplete(checkpointId);
  }
  std::vector<std::string> committed = operator_->commit(checkpointId);
  for (auto& target : targets_) {
    std::vector<std::string> otherCommitted = target->notifyCheckpointComplete(checkpointId);
    committed.insert(committed.end(), otherCommitted.begin(), otherCommitted.end());
  }
  return committed;
}

void StatefulOperator::notifyCheckpointAborted(long checkpointId) {
  stateHandler_->notifyCheckpointAborted(checkpointId);
  auto checkpointListener = dynamic_cast<CheckpointListener*>(op().get());
  if (checkpointListener) {
    // If the operator is checkpointable, we need to snapshot it.
    checkpointListener->notifyCheckpointAborted(checkpointId);
  }
  for (auto& target : targets_) {
    target->notifyCheckpointAborted(checkpointId);
  }
}

} // namespace facebook::velox::stateful
