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
#include "velox/experimental/stateful/StatefulTask.h"
#include <experimental/stateful/state/StateBackend.h>
#include <cstdint>
#include <glog/logging.h>
#include "velox/exec/OperatorStats.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/experimental/stateful/StatefulPlanner.h"
#include "velox/experimental/stateful/state/HashMapStateBackend.h"
#include "velox/experimental/stateful/state/RocksDBStateBackend.h"

namespace facebook::velox::stateful {

// static
std::shared_ptr<StatefulTask> StatefulTask::create(
    const std::string& taskId,
    core::PlanFragment planFragment,
    std::shared_ptr<core::QueryCtx> queryCtx) {
  VELOX_CHECK_NOT_NULL(planFragment.planNode);
  auto task = std::shared_ptr<StatefulTask>(
      new StatefulTask(taskId, std::move(planFragment), std::move(queryCtx)));
  task->initTaskPool();
  task->addToTaskList();
  return task;
}

StatefulTask::StatefulTask(
    const std::string& taskId,
    core::PlanFragment planFragment,
    std::shared_ptr<core::QueryCtx> queryCtx)
    : exec::Task(
          taskId,
          std::move(planFragment),
          0,
          std::move(queryCtx),
          exec::Task::ExecutionMode::kSerial,
          nullptr,
          0,
          nullptr) {}

StatefulTask::~StatefulTask() {}

void StatefulTask::init() {
  initOperators();
  operatorChain_->initialize();
}

void StatefulTask::initStateBackend(
    const std::shared_ptr<const KeyedStateBackendParameters> parameters) {
  if (parameters && parameters->getBackendType() == StateBackendType::ROCKSDB) {
    const std::shared_ptr<const RocksDBKeyedStateBackendParameters>
        rocksdbStateParameters =
            std::dynamic_pointer_cast<const RocksDBKeyedStateBackendParameters>(
                parameters);
    statebackend_ =
        std::make_unique<RocksDBStateBackend>(rocksdbStateParameters);
  } else {
    if (!parameters) {
      statebackend_ = std::make_unique<HashMapStateBackend>(std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP, 
          operatorChain_->op()->operatorCtx()->driverCtx()->task->taskId(),
          std::to_string(operatorChain_->op()->operatorId())));
    } else {
      statebackend_ = std::make_unique<HashMapStateBackend>(parameters);
    }
  }
  operatorChain_->initializeStateBackend(statebackend_.get());
}

void StatefulTask::initOperators() {
  auto self = shared_from_this();
  // Create the operators.
  VELOX_CHECK_NULL(operatorChain_);
  auto driverCtx = std::make_unique<exec::DriverCtx>(self, 0, 0, -1, 0);
  driver = exec::Driver::testingCreate(std::move(driverCtx));
  operatorChain_ = std::move(StatefulPlanner::plan(
      planFragment(), driver->driverCtx(), statebackend_.get()));
}

void statefulTaskStatus(
    exec::TaskStats& taskStats,
    const std::unique_ptr<StatefulOperator>& statefulOp) {
  auto statsCopy = statefulOp->op()->stats(false);
  exec::aggregateOperatorRuntimeStats(statsCopy.runtimeStats);
  exec::PipelineStats pipelineStats(false, false);
  pipelineStats.operatorStats.emplace_back(statsCopy);
  taskStats.pipelineStats.emplace_back(pipelineStats);
  std::vector<std::unique_ptr<StatefulOperator>>& targets =
      statefulOp->targets();
  for (const auto& target : targets) {
    statefulTaskStatus(taskStats, target);
  }
}

exec::TaskStats StatefulTask::statefulTaskStats() {
  exec::TaskStats taskStats;
  statefulTaskStatus(taskStats, operatorChain_);
  return taskStats;
}

StreamElementPtr StatefulTask::next(int32_t& retCode) {
  retCode = 0;
  if (!pendings_.empty()) {
    return popOutput();
  } else if (state() == exec::TaskState::kFinished) {
    // If the task is already finished, return null and 1 for retCode.
    retCode = 1;
    return nullptr;
  }

  // Run operators one by one. If an operator has output, run its downstream
  // operators. If the last operator has output, return the output. If source
  // operator has no output, check whether it is finished. If source is
  // finished, return null and 1 for rerCode, else return null and 0 for
  // retCode.
  // TODO: only support operators in a sequence mode.
  VELOX_CHECK_EQ(
      state(),
      exec::TaskState::kRunning,
      "Task has already finished processing.");

  operatorChain_->advance();
  if (pendings_.empty()) {
    if (operatorChain_->isFinished()) {
      finish();
      // finish may trigger window flush and generate output.
      if (pendings_.empty()) {
        retCode = 1;
        return nullptr;
      }
    } else if (operatorChain_->sourceEmpty()) {
      return nullptr;
    } else {
      return nullptr;
    }
  }
  return popOutput();
}

void StatefulTask::addOutput(StreamElementPtr output) {
  pendings_.push_back(std::move(output));
}

void StatefulTask::setNativeCallbackBridge(
    std::shared_ptr<NativeCallbackBridge> callbackBridge) {
  const bool bound = callbackBridge != nullptr;
  {
    std::lock_guard<std::mutex> lock(nativeCallbackBridgeMutex_);
    nativeCallbackBridge_ = std::move(callbackBridge);
  }
  LOG(INFO) << (bound ? "Bound" : "Unbound")
            << " native callback bridge for stateful task: " << taskId();
}

std::shared_ptr<NativeCallbackBridge> StatefulTask::nativeCallbackBridge()
    const {
  std::lock_guard<std::mutex> lock(nativeCallbackBridgeMutex_);
  return nativeCallbackBridge_;
}

void StatefulTask::notifyWatermark(int64_t watermark, int index) {
  operatorChain_->processWatermark(watermark, index);
}

void StatefulTask::notifyWatermark(int64_t watermark) {
  operatorChain_->processWatermark(watermark);
}

void StatefulTask::initializeState(
    const std::shared_ptr<const KeyedStateBackendParameters> parameters) {
  initStateBackend(parameters);
  operatorChain_->initializeState();
}

void StatefulTask::snapshotState() {
  // TODO: this is a synchronous call now, maybe need to use async.
  operatorChain_->snapshotState();
}

std::vector<std::string> StatefulTask::notifyCheckpointComplete(
    int64_t checkpointId) {
  return operatorChain_->notifyCheckpointComplete(checkpointId);
}

void StatefulTask::notifyCheckpointAborted(int64_t checkpointId) {
  operatorChain_->notifyCheckpointAborted(checkpointId);
}

StreamElementPtr StatefulTask::popOutput() {
  auto out = std::move(pendings_.front());
  pendings_.pop_front();
  return out;
}

void StatefulTask::finish() {
  VELOX_CHECK(
      pendings_.empty(),
      "Outputs have {} not been consumed before finishing the task. {} {}",
      pendings_.size(),
      operatorChain_->detail());
  operatorChain_->close();
  // TODO: update operator stats

  // remove operators to release memory.
  operatorChain_.reset();
  driver.reset();
  testingFinish();
}

} // namespace facebook::velox::stateful
