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
#pragma once
#include <cstdint>

#include <algorithm>
#include <memory>
#include <type_traits>
#include <vector>
#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/experimental/stateful/window/GroupWindowAssigner.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

class GroupWindowAggsHandler;
class MergingWindowSet;
class MergingFunction;

// This class is relevant to Flink InternalWindowProcessFunction::Context.
// The accumulator access is row-based over the typed AggregatingState
// (char* accumulator rows in the value RowContainer): getWindowAccumulators
// is the non-materializing existence read (nullptr on a miss, the Flink
// acc == null probe), getOrCreateWindowAccumulators materializes a fresh row
// through the descriptor init callback on a miss. The pre-generic
// getPartitionedState entry of the old stateHandler plane is gone: the
// operator injects the typed handles directly.
template <typename W, typename = std::enable_if_t<std::is_base_of_v<Window, W>>>
class FunctionContext {
 public:
  virtual ~FunctionContext() = default;

  virtual const RowContainerStateKey& currentKey() = 0;

  virtual int64_t currentProcessingTime() = 0;

  /// The last watermark the operator has advanced through (C5: not the
  /// timer service's earliest-pending-timer view).
  virtual int64_t currentWatermark() = 0;

  virtual int getShiftTimeZone() = 0;

  virtual char* getWindowAccumulators(W window) = 0;

  virtual char* getOrCreateWindowAccumulators(W window) = 0;

  virtual void clearWindowState(W window) = 0;

  virtual void clearPreviousState(W window) = 0;

  virtual void clearTrigger(W window) = 0;

  virtual void onMerge(W newWindow, std::vector<W>& mergedWindows) = 0;

  virtual void deleteCleanupTimer(W window) = 0;
};

// This class is relevant to Flink InternalWindowProcessFunction.
template <typename W, typename = std::enable_if_t<std::is_base_of_v<Window, W>>>
class WindowProcessFunction {
 public:
  WindowProcessFunction(
      std::shared_ptr<GroupWindowAssigner<W>> windowAssigner,
      GroupWindowAggsHandler* windowAggregator,
      int64_t allowedLateness)
      : windowAssigner_(std::move(windowAssigner)),
        windowAggregator_(windowAggregator),
        allowedLateness_(allowedLateness) {}

  virtual ~WindowProcessFunction() = default;

  virtual void open(std::shared_ptr<FunctionContext<W>> ctx) {
    ctx_ = std::move(ctx);
  }

  /// Assigns the state namespace of one input row: the merging family merges
  /// the row's window into the in-flight set (mapping mutations and
  /// accumulator merges happen here) and reports the state windows the row's
  /// actual windows resolve to. Returns the state windows in the same order
  /// as assignActualWindows() reports the actual windows.
  virtual std::vector<W> assignStateNamespace(
      const RowContainerStateKey& key,
      int64_t timestamp) = 0;

  /// The actual (merged-result) windows of the row assigned by the last
  /// assignStateNamespace call.
  virtual std::vector<W> assignActualWindows() = 0;

  /// Resolves the state window the actual 'window' maps to and reads its
  /// accumulator row without materializing one: nullptr when the window
  /// has no state (the empty-window suppression probe).
  virtual char* prepareAggregateAccumulatorForEmit(
      const RowContainerStateKey& key,
      W window) = 0;

  virtual void cleanWindowIfNeeded(W window, int64_t currentTime) = 0;

  /// Drops the per-key in-flight window cache at the batch boundary: the
  /// cached keys are row views into the backend's key table, and a key whose
  /// last state entry is gone may have its row reclaimed by advance(); the
  /// mapping state is the source of truth either way.
  virtual void clearCache() {}

  virtual void close() {}

 protected:
  bool isWindowLate(W window) {
    return (
        windowAssigner_->isEventTime() &&
        (TimeWindowUtil::toEpochMillsForTimer(
             TimeWindowUtil::cleanupTime(
                 window.maxTimestamp(),
                 allowedLateness_,
                 windowAssigner_->isEventTime()),
             ctx_->getShiftTimeZone()) <= ctx_->currentWatermark()));
  }

  bool isCleanupTime(W window, int64_t time) {
    return time ==
        TimeWindowUtil::toEpochMillsForTimer(
               TimeWindowUtil::cleanupTime(
                   window.maxTimestamp(),
                   allowedLateness_,
                   windowAssigner_->isEventTime()),
               ctx_->getShiftTimeZone());
  }

  std::shared_ptr<GroupWindowAssigner<W>> windowAssigner_;
  GroupWindowAggsHandler* windowAggregator_;
  int64_t allowedLateness_;
  std::shared_ptr<FunctionContext<W>> ctx_;
};

using WindowProcessFunctionPtr =
    std::shared_ptr<WindowProcessFunction<TimeWindow>>;

/// The typed mapping handle of the session-window in-flight set: one map of
/// actual window -> state window per key, under the fixed VoidNamespace (the
/// map is the per-key window table, not a window-indexed state - the state
/// windows live under the TimeWindow namespace of the aggregating state).
using SessionWindowMappingState =
    MapState<RowContainerStateKey, VoidNamespace, TimeWindow, TimeWindow>;

// This class is relevant to Flink MergingWindowProcessFunction. The mapping
// handle is injected by the operator at construction (the old
// getPartitionedState entry is gone).
class MergingWindowProcessFunction : public WindowProcessFunction<TimeWindow> {
 public:
  MergingWindowProcessFunction(
      std::shared_ptr<MergingWindowAssigner> windowAssigner,
      GroupWindowAggsHandler* windowAggregator,
      int64_t allowedLateness,
      std::shared_ptr<SessionWindowMappingState> mapping);

  void open(std::shared_ptr<FunctionContext<TimeWindow>> ctx) override;

  std::vector<TimeWindow> assignStateNamespace(
      const RowContainerStateKey& key,
      int64_t timestamp) override;

  std::vector<TimeWindow> assignActualWindows() override;

  void cleanWindowIfNeeded(TimeWindow window, int64_t currentTime) override;

  char* prepareAggregateAccumulatorForEmit(
      const RowContainerStateKey& key,
      TimeWindow window) override;

  void clearCache() override;

  void close() override;

 private:
  using WindowProcessFunction<TimeWindow>::windowAssigner_;
  using WindowProcessFunction<TimeWindow>::windowAggregator_;
  using WindowProcessFunction<TimeWindow>::ctx_;
  std::shared_ptr<SessionWindowMappingState> mapping_;
  std::shared_ptr<MergingWindowSet> mergingWindows_;
  std::shared_ptr<MergingFunction> mergingFunction_;
  std::vector<TimeWindow> reuseActualWindows_;
};

// This class is relevant to Flink GeneralWindowProcessFunction (the
// InternalWindowProcessFunction branch for non-merging assigners): TUMBLE
// and HOP windows. The state namespace IS the actual window — no in-flight
// mapping, no merging, no per-key cache; every event accumulates into each
// of its assigned windows under that window's own namespace, and window
// cleanup drops the accumulator directly.
class GeneralWindowProcessFunction : public WindowProcessFunction<TimeWindow> {
 public:
  GeneralWindowProcessFunction(
      std::shared_ptr<GroupWindowAssigner<TimeWindow>> windowAssigner,
      GroupWindowAggsHandler* windowAggregator,
      int64_t allowedLateness)
      : WindowProcessFunction<TimeWindow>(
            std::move(windowAssigner),
            windowAggregator,
            allowedLateness) {}

  std::vector<TimeWindow> assignStateNamespace(
      const RowContainerStateKey& key,
      int64_t timestamp) override;

  std::vector<TimeWindow> assignActualWindows() override {
    return reuseWindows_;
  }

  void cleanWindowIfNeeded(TimeWindow window, int64_t currentTime) override;

  char* prepareAggregateAccumulatorForEmit(
      const RowContainerStateKey& key,
      TimeWindow window) override;

 private:
  using WindowProcessFunction<TimeWindow>::windowAssigner_;
  using WindowProcessFunction<TimeWindow>::windowAggregator_;
  using WindowProcessFunction<TimeWindow>::ctx_;
  std::vector<TimeWindow> reuseWindows_;
};

// This class is relevant to Flink MergingWindowProcessFunction's
// DefaultAccMergingConsumer: merges the accumulator rows of the merged state
// windows into the merge result's state window.
class DefaultAccMergingConsumer {
 public:
  DefaultAccMergingConsumer(
      std::shared_ptr<FunctionContext<TimeWindow>> ctx,
      GroupWindowAggsHandler* windowAggregator);

  void accept(
      TimeWindow stateWindowResult,
      std::vector<TimeWindow> stateWindowsToBeMerged);

 private:
  std::shared_ptr<FunctionContext<TimeWindow>> ctx_;
  GroupWindowAggsHandler* windowAggregator_;
};
} // namespace facebook::velox::stateful
