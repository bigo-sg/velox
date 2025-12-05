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
#include "velox/experimental/stateful/window/WindowProcessFunction.h"
#include "velox/experimental/stateful/window/MergingWindowSet.h"

namespace facebook::velox::stateful {

MergingWindowProcessFunction::MergingWindowProcessFunction(
    std::shared_ptr<MergingWindowAssigner> windowAssigner,
    exec::Operator* windowAggregator,
    long allowedLateness)
    : WindowProcessFunction<TimeWindow>(windowAssigner, windowAggregator, allowedLateness) {
}

void MergingWindowProcessFunction::open(std::shared_ptr<FunctionContext<TimeWindow>> ctx) {
  WindowProcessFunction<TimeWindow>::open(ctx);
  StateDescriptor mappingStateDescriptor("session-window-mapping");
  std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>> windowMapping =
      std::dynamic_pointer_cast<MapState<uint32_t, int, TimeWindow, TimeWindow>>(
          ctx->getPartitionedState(mappingStateDescriptor));
  mergingWindows_ = std::make_shared<MergingWindowSet>(
      std::dynamic_pointer_cast<MergingWindowAssigner>(windowAssigner_),
      windowMapping);
  mergingFunction_ =
      std::make_shared<MergingFunction>(
          std::make_shared<DefaultAccMergingConsumer>(ctx, windowAggregator_),
          ctx,
          allowedLateness_,
          windowAssigner_->isEventTime());
}

std::vector<TimeWindow> MergingWindowProcessFunction::assignStateNamespace(
    uint32_t key, RowVectorPtr inputRow, long timestamp) {
  std::vector<TimeWindow> elementWindows = windowAssigner_->assignWindows(inputRow, timestamp);
  mergingWindows_->initializeCache(key);
  reuseActualWindows_.clear();
  for (TimeWindow window : elementWindows) {
      // adding the new window might result in a merge, in that case the actualWindow
      // is the merged window and we work with that. If we don't merge then
      // actualWindow == window
      TimeWindow actualWindow = mergingWindows_->addWindow(key, window, mergingFunction_);

      // drop if the window is already late
      if (isWindowLate(actualWindow)) {
          mergingWindows_->retireWindow(key, actualWindow);
      } else {
          reuseActualWindows_.push_back(actualWindow);
      }
  }
  std::vector<TimeWindow> affectedWindows(reuseActualWindows_.size());
  for (TimeWindow actual : reuseActualWindows_) {
      affectedWindows.push_back(mergingWindows_->getStateWindow(key, actual));
  }
  return affectedWindows;
}

std::vector<TimeWindow> MergingWindowProcessFunction::assignActualWindows(
    RowVectorPtr inputRow, long timestamp) {
  return reuseActualWindows_;
}

void MergingWindowProcessFunction::cleanWindowIfNeeded(TimeWindow window, long currentTime) {
  if (isCleanupTime(window, currentTime)) {
    ctx_->clearTrigger(window);
    TimeWindow stateWindow = mergingWindows_->getStateWindow(ctx_->currentKey(), window);
    ctx_->clearWindowState(stateWindow);
    // retire expired window
    mergingWindows_->initializeCache(ctx_->currentKey());
    mergingWindows_->retireWindow(ctx_->currentKey(), window);
    // do not need to clear previous state, previous state is disabled in session window
  }
}

void MergingWindowProcessFunction::prepareAggregateAccumulatorForEmit(
    uint32_t key, TimeWindow window) {
  TimeWindow stateWindow = mergingWindows_->getStateWindow(ctx_->currentKey(), window);
  RowVectorPtr acc = ctx_->getWindowAccumulators(stateWindow);
  if (!acc) {
      acc = windowAggregator_->createAccumulators();
  }
  windowAggregator_->setAccumulators(stateWindow, acc);
}

void MergingWindowProcessFunction::close() {
  WindowProcessFunction<TimeWindow>::close();
  mergingWindows_->close();
  mergingFunction_.reset();
  mergingWindows_.reset();
}

DefaultAccMergingConsumer::DefaultAccMergingConsumer(
    std::shared_ptr<FunctionContext<TimeWindow>> ctx,
    GroupWindowAggsHandler* windowAggregator)
    : ctx_(ctx), windowAggregator_(windowAggregator) {
}

void DefaultAccMergingConsumer::accept(
    TimeWindow stateWindowResult, std::vector<TimeWindow> stateWindowsToBeMerged) {
  RowVectorPtr targetAcc = ctx_->getWindowAccumulators(stateWindowResult);
  if (targetAcc == nullptr) {
    targetAcc = windowAggregator_->createAccumulators();
  }
  windowAggregator_->setAccumulators(stateWindowResult, targetAcc);
  for (TimeWindow w : stateWindowsToBeMerged) {
    RowVectorPtr acc = ctx_->getWindowAccumulators(w);
    if (acc != nullptr) {
      windowAggregator_->merge(w, acc);
    }
    // clear merged window
    ctx_->clearWindowState(w);
    ctx_->clearPreviousState(w);
  }
  targetAcc = windowAggregator_->getAccumulators();
  ctx_->setWindowAccumulators(stateWindowResult, targetAcc);
}
} // namespace facebook::velox::stateful
