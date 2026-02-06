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
#include "velox/experimental/stateful/WindowJoin.h"
#include "velox/experimental/stateful/join/JoinRecordStateViews.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::stateful {

WindowJoin::WindowJoin(
    std::unique_ptr<exec::Operator> leftInput,
    std::unique_ptr<exec::Operator> rightInput,
    std::unique_ptr<KeySelector> leftKeySelector,
    std::unique_ptr<KeySelector> rightKeySelector,
    std::unique_ptr<exec::Operator> probe,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    int leftWindowEndIndex,
    int rightWindowEndIndex)
    : StatefulOperator(std::move(probe), std::move(targets)),
      leftInput_(std::move(leftInput)),
      rightInput_(std::move(rightInput)),
      leftKeySelector_(std::move(leftKeySelector)),
      rightKeySelector_(std::move(rightKeySelector)),
      probe_(static_cast<exec::NestedLoopJoinProbe*>(op().get())),
      leftWindowEndIndex_(leftWindowEndIndex),
      rightWindowEndIndex_(rightWindowEndIndex) {}

void WindowJoin::initialize() {
  StatefulOperator::initialize();
  leftInput_->initialize();
  rightInput_->initialize();

  StateDescriptor leftStateDesc("left-records");
  leftWindowState_ = stateHandler()->getListState(leftStateDesc);
  StateDescriptor rightStateDesc("right-records");
  rightWindowState_ = stateHandler()->getListState(rightStateDesc);
  timerService_ = stateHandler()->createTimerService(this);
}

bool WindowJoin::isFinished() {
  return leftInput_->isFinished() && rightInput_->isFinished();
}

void WindowJoin::addInput(RowVectorPtr input) {
  VELOX_NYI();
}

void WindowJoin::close() {
  StatefulOperator::close();
  leftInput_->close();
  rightInput_->close();
  leftWindowState_->clear();
  rightWindowState_->clear();
}

void WindowJoin::getOutput() {
  // TODO: use nested loop join logic to produce output now.
  // But it's not equal to Flink's streaming join.
  processData(
      leftInput_.get(),
      leftKeySelector_.get(),
      leftWindowEndIndex_,
      leftWindowState_.get());
  processData(
      rightInput_.get(),
      rightKeySelector_.get(),
      rightWindowEndIndex_,
      rightWindowState_.get());
}

void WindowJoin::processData(
    exec::Operator* input,
    KeySelector* keySelector,
    int windowEndIndex,
    ListState<uint32_t, long, RowVectorPtr>* state) {
  auto result = input->getOutput();
  if (result) {
    auto notFired = filterWindowFiredRows(result);
    auto keyToData = keySelector->partition(notFired);
    for (auto& [key, partitioned] : keyToData) {
      auto windowEndToData = partitionWindowData(partitioned, windowEndIndex);
      for (const auto& [windowEnd, rowData] : windowEndToData) {
        state->add(key, windowEnd, rowData);
        timerService_->registerEventTimeTimer(key, windowEnd, windowEnd - 1);
      }
    }
  }
}

RowVectorPtr WindowJoin::filterWindowFiredRows(RowVectorPtr& input) {
  return input;
}

std::map<long, RowVectorPtr> WindowJoin::partitionWindowData(
    RowVectorPtr& input,
    int windowEndIndex) {
  std::map<long, RowVectorPtr> windowEndToData;
  // TODO: this is just a example,.
  auto row = input->childAt(windowEndIndex);
  long windowEnd = row->asFlatVector<int64_t>()->valueAt(
      0); // Assuming first column is window end
  windowEndToData[windowEnd] = input;
  return windowEndToData;
}

void WindowJoin::onEventTime(
    std::shared_ptr<TimerHeapInternalTimer<uint32_t, long>> timer) {
  join(timer->key(), timer->ns());
}

void WindowJoin::join(uint32_t key, long window) {
  auto leftValues = leftWindowState_->get(key, window);
  auto rightValues = rightWindowState_->get(key, window);
  if (leftValues.empty() || rightValues.empty()) {
    return;
  }
  std::vector<RowVectorPtr> buildVector;
  for (const auto& value : rightValues) {
    buildVector.push_back(value);
  }
  for (auto value : leftValues) {
    probe_->addInput(std::move(value));
    probe_->setBuildData(buildVector);

    auto result = probe_->getOutput();
    pushOutput(result);
  }
  // TODO: clear state
  leftWindowState_->remove(key, window);
  rightWindowState_->remove(key, window);
}

void WindowJoin::processWatermarkInternal(int64_t timestamp) {
  timerService_->advanceWatermark(timestamp);
}

} // namespace facebook::velox::stateful
