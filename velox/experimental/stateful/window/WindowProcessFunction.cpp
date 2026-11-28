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
#include <cstdint>
#include "velox/experimental/stateful/window/GroupWindowAggsHandler.h"
#include "velox/experimental/stateful/window/MergingWindowSet.h"

namespace facebook::velox::stateful {

MergingWindowProcessFunction::MergingWindowProcessFunction(
    std::shared_ptr<MergingWindowAssigner> windowAssigner,
    GroupWindowAggsHandler* windowAggregator,
    int64_t allowedLateness,
    std::shared_ptr<SessionWindowMappingState> mapping)
    : WindowProcessFunction<TimeWindow>(
          windowAssigner,
          windowAggregator,
          allowedLateness),
      mapping_(std::move(mapping)) {}

void MergingWindowProcessFunction::open(
    std::shared_ptr<FunctionContext<TimeWindow>> ctx) {
  WindowProcessFunction<TimeWindow>::open(ctx);
  // The operator injects the typed mapping handle (no
  // getPartitionedState round trip through the state handler).
  mergingWindows_ = std::make_shared<MergingWindowSet>(
      std::dynamic_pointer_cast<MergingWindowAssigner>(windowAssigner_),
      mapping_);
  mergingFunction_ = std::make_shared<MergingFunction>(
      std::make_shared<DefaultAccMergingConsumer>(ctx, windowAggregator_),
      ctx,
      allowedLateness_,
      windowAssigner_->isEventTime());
}

std::vector<TimeWindow> MergingWindowProcessFunction::assignStateNamespace(
    const RowContainerStateKey& key,
    int64_t timestamp) {
  std::vector<TimeWindow> elementWindows =
      windowAssigner_->assignWindows(nullptr, timestamp);
  mergingWindows_->initializeCache(key);
  reuseActualWindows_.clear();
  for (TimeWindow window : elementWindows) {
    // adding the new window might result in a merge, in that case the
    // actualWindow is the merged window and we work with that. If we don't
    // merge then actualWindow == window
    TimeWindow actualWindow =
        mergingWindows_->addWindow(key, window, mergingFunction_);

    // drop if the window is already late
    if (isWindowLate(actualWindow)) {
      mergingWindows_->retireWindow(key, actualWindow);
    } else {
      reuseActualWindows_.push_back(actualWindow);
    }
  }
  // The state windows of the kept actual windows, in the same order; the
  // vector is reserved empty (no pre-filled default TimeWindow(-1, -1)
  // entries - the ghost-window bug: a default-constructed window must never
  // reach state resolution).
  std::vector<TimeWindow> affectedWindows;
  affectedWindows.reserve(reuseActualWindows_.size());
  for (TimeWindow actual : reuseActualWindows_) {
    affectedWindows.push_back(mergingWindows_->getStateWindow(key, actual));
  }
  return affectedWindows;
}

std::vector<TimeWindow> MergingWindowProcessFunction::assignActualWindows() {
  return reuseActualWindows_;
}

void MergingWindowProcessFunction::cleanWindowIfNeeded(
    TimeWindow window,
    int64_t currentTime) {
  if (isCleanupTime(window, currentTime)) {
    ctx_->clearTrigger(window);
    TimeWindow stateWindow =
        mergingWindows_->getStateWindow(ctx_->currentKey(), window);
    ctx_->clearWindowState(stateWindow);
    // retire expired window
    mergingWindows_->initializeCache(ctx_->currentKey());
    mergingWindows_->retireWindow(ctx_->currentKey(), window);
    // do not need to clear previous state, previous state is disabled in
    // session window
  }
}

char* MergingWindowProcessFunction::prepareAggregateAccumulatorForEmit(
    const RowContainerStateKey& key,
    TimeWindow window) {
  TimeWindow stateWindow = mergingWindows_->getStateWindow(key, window);
  // Existence read: an empty window reports nullptr and never materializes
  // state (the empty-window suppression).
  return ctx_->getWindowAccumulators(stateWindow);
}

void MergingWindowProcessFunction::clearCache() {
  if (mergingWindows_) {
    mergingWindows_->clearCache();
  }
}

void MergingWindowProcessFunction::close() {
  WindowProcessFunction<TimeWindow>::close();
  if (mergingWindows_) {
    mergingWindows_->close();
  }
  mergingFunction_.reset();
  mergingWindows_.reset();
}

DefaultAccMergingConsumer::DefaultAccMergingConsumer(
    std::shared_ptr<FunctionContext<TimeWindow>> ctx,
    GroupWindowAggsHandler* windowAggregator)
    : ctx_(std::move(ctx)), windowAggregator_(windowAggregator) {}

void DefaultAccMergingConsumer::accept(
    TimeWindow stateWindowResult,
    std::vector<TimeWindow> stateWindowsToBeMerged) {
  // The merge result's state window hosts the merged accumulators; a miss
  // materializes a fresh initialized row.
  char* target = ctx_->getOrCreateWindowAccumulators(stateWindowResult);
  for (TimeWindow w : stateWindowsToBeMerged) {
    char* acc = ctx_->getWindowAccumulators(w);
    if (acc != nullptr) {
      windowAggregator_->mergeAccumulators(target, acc);
    }
    // clear merged window
    ctx_->clearWindowState(w);
    ctx_->clearPreviousState(w);
  }
}

std::vector<TimeWindow> GeneralWindowProcessFunction::assignStateNamespace(
    const RowContainerStateKey& key,
    int64_t timestamp) {
  // Non-merging path: the assigned windows are both the actual and the
  // state windows (the namespace IS the window). Late windows are dropped
  // here, exactly like the merging path — the caller only sees kept
  // windows, in the same order for both assignStateNamespace and
  // assignActualWindows.
  std::vector<TimeWindow> windows =
      windowAssigner_->assignWindows(nullptr, timestamp);
  reuseWindows_.clear();
  for (const auto& window : windows) {
    if (!isWindowLate(window)) {
      reuseWindows_.push_back(window);
    }
  }
  return reuseWindows_;
}

void GeneralWindowProcessFunction::cleanWindowIfNeeded(
    TimeWindow window,
    int64_t currentTime) {
  if (isCleanupTime(window, currentTime)) {
    ctx_->clearTrigger(window);
    ctx_->clearWindowState(window);
  }
}

char* GeneralWindowProcessFunction::prepareAggregateAccumulatorForEmit(
    const RowContainerStateKey& key,
    TimeWindow window) {
  // Existence read under the window's own namespace (the empty-window
  // suppression — a miss stays a miss).
  return ctx_->getWindowAccumulators(window);
}

} // namespace facebook::velox::stateful
