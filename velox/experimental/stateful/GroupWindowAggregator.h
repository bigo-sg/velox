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
#include <optional>
#include <vector>

#include "velox/experimental/stateful/EventTimeTimerService.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/Triggerable.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/experimental/stateful/window/GroupWindowAggsHandler.h"
#include "velox/experimental/stateful/window/GroupWindowAssigner.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/experimental/stateful/window/WindowProcessFunction.h"
#include "velox/experimental/stateful/window/WindowTrigger.h"

namespace facebook::velox::stateful {

/// Timer-heap adaptation for the typed state key. RowContainerStateKey is an
/// immutable view with const members: it has no operator== (the generic
/// HeapTimerEquals would need one) and no implicit conversion into the
/// uint64_t the generic HeapTimerHasher mixes in. Both route through the
/// key's value identity instead - the cached hash and the StateKey::equals
/// virtual - the same adaptation MergingWindowSet applies for its in-flight
/// window cache (RowContainerStateKeyHash / RowContainerStateKeyEqual).
/// Declared before the service instantiation below (specializations must
/// precede the first implicit instantiation).
template <>
struct HeapTimerHasher<RowContainerStateKey, TimeWindow> {
  size_t operator()(
      const TimerHeapInternalTimer<RowContainerStateKey, TimeWindow>& a) const {
    return bits::hashMix(a.timestamp(), a.key().hash());
  }
};

template <>
struct HeapTimerEquals<RowContainerStateKey, TimeWindow> {
  bool operator()(
      const TimerHeapInternalTimer<RowContainerStateKey, TimeWindow>& a,
      const TimerHeapInternalTimer<RowContainerStateKey, TimeWindow>& b) const {
    return a.timestamp() == b.timestamp() && a.key().equals(b.key()) &&
        a.ns() == b.ns();
  }
};

/// Group-window aggregation with Flink session-window semantics (relevant
/// to Flink AggregateWindowOperator on the MergingWindowProcessFunction
/// path): per (key, state-window) one accumulator row in the typed
/// AggregatingState under the TimeWindow namespace, the session in-flight
/// set in a typed MapState (actual window -> state window, VoidNamespace),
/// and watermark-driven firing - elements accumulate immediately, each
/// element's window registers its end-of-window fire / cleanup timer, and
/// the watermark advancement pops the due timers and emits one INSERT row
/// per non-empty window. Late elements (window end at or before the last
/// watermark) are dropped with a counter; empty windows are suppressed
/// through the non-materializing existence read (user gate 1 decision #11,
/// scheme A); merging windows never let the merge result slip past the
/// watermark (the retained VELOX_CHECK guard, decision #15).
class GroupWindowAggregator
    : public StatefulOperator,
      public Triggerable<RowContainerStateKey, TimeWindow> {
 public:
  GroupWindowAggregator(
      std::unique_ptr<GroupWindowAggsHandler> windowAggsHandler,
      std::vector<StatefulOperatorPtr> targets,
      const std::shared_ptr<const GroupWindowAggregationNode>& node);

  void initialize() override;

  void initializeStateBackend(StateBackend* stateBackend) override;

  void addInput(StreamElementPtr input) override;

  /// Event-time operator: advances the operator-held last watermark first
  /// (the isWindowLate / trigger decisions read it - C5), then pops the due
  /// timers, then cascades the watermark downstream.
  void processWatermark(int64_t timestamp) override;

  void onEventTime(
      std::shared_ptr<TimerHeapInternalTimer<RowContainerStateKey, TimeWindow>>
          timer) override;

  void close() override;

  std::string name() const override {
    return "GroupWindowAggregator";
  }

  /// Checkpoint plane: snapshot of this operator's keyed state (one opaque
  /// record per call), driven through the wrapped exec::Operator.
  std::vector<std::string> snapshotKeyedState();

  /// Restores every delivered record that overlaps this backend's key-group
  /// range (union-list restore; out-of-range records are skipped inside
  /// HeapKeyedStateBackend::restore).
  void restoreKeyedState(const std::vector<std::string>& records);

  /// Test / observability hook: whether the in-memory timer queue is empty
  /// (the queue lives outside the typed snapshot plane - C3).
  bool timersEmpty() const {
    return windowTimerService_ == nullptr || windowTimerService_->empty();
  }

  /// Test / observability hook: the number of pending timers. The restore
  /// rebuild (A2) repopulates the queue from state, so this count survives
  /// a snapshot -> restore round trip: one deduped entry per live (key,
  /// actual window) before the snapshot, the same after the rebuild.
  size_t timersSize() const {
    return windowTimerService_ == nullptr ? 0 : windowTimerService_->size();
  }

  /// Test / observability hook: the late-element counter (equivalent of
  /// Flink's lateRecordsDroppedRate, carried on the operator).
  uint64_t numLateRecordsDropped() const {
    return numLateRecordsDropped_;
  }

 private:
  /// Step 1: cast + rowKind validation (P0 accepts append-only and INSERT
  /// only; every other kind is a fail-loud L6 rejection).
  void checkInputRowKinds(const StreamRecord& record) const;

  /// The TriggerContext of the AfterEndOfWindow trigger.
  class WindowTriggerContext : public TriggerContext {
   public:
    explicit WindowTriggerContext(GroupWindowAggregator* owner)
        : owner_(owner) {}

    void open() override;

    bool onElement(
        const RowContainerStateKey& key,
        RowVectorPtr row,
        int64_t timestamp,
        TimeWindow window) override;

    bool onProcessingTime(TimeWindow window, int64_t time) override;

    bool onEventTime(TimeWindow window, int64_t time) override;

    void onMerge(const RowContainerStateKey& key, TimeWindow window) override;

    int64_t getCurrentProcessingTime() override;

    /// C5 compensation: the operator-held last watermark, not the timer
    /// service's earliest-pending-timer view.
    int64_t getCurrentWatermark() override;

    void registerProcessingTimeTimer(
        const RowContainerStateKey& key,
        TimeWindow window,
        int64_t time) override;

    void registerEventTimeTimer(
        const RowContainerStateKey& key,
        TimeWindow window,
        int64_t time) override;

    void deleteProcessingTimeTimer(
        const RowContainerStateKey& key,
        TimeWindow window,
        int64_t time) override;

    void deleteEventTimeTimer(
        const RowContainerStateKey& key,
        TimeWindow window,
        int64_t time) override;

    int getShiftTimeZone() override;

    void clear(const RowContainerStateKey& key, TimeWindow window) override;

    StatePtr getPartitionedState(StateDescriptor& stateDescriptor) override;

    void mergePartitionedState(StateDescriptor& stateDescriptor) override;

   private:
    GroupWindowAggregator* owner_;
  };

  /// The FunctionContext of the window process function: routes the
  /// context's state access to the operator's typed handles.
  class WindowContext : public FunctionContext<TimeWindow> {
   public:
    explicit WindowContext(GroupWindowAggregator* owner) : owner_(owner) {}

    const RowContainerStateKey& currentKey() override;

    int64_t currentProcessingTime() override;

    int64_t currentWatermark() override;

    int getShiftTimeZone() override;

    char* getWindowAccumulators(TimeWindow window) override;

    char* getOrCreateWindowAccumulators(TimeWindow window) override;

    void clearWindowState(TimeWindow window) override;

    void clearPreviousState(TimeWindow window) override;

    void clearTrigger(TimeWindow window) override;

    void onMerge(TimeWindow newWindow, std::vector<TimeWindow>& mergedWindows)
        override;

    void deleteCleanupTimer(TimeWindow window) override;

   private:
    GroupWindowAggregator* owner_;
  };

  void registerCleanupTimer(const RowContainerStateKey& key, TimeWindow window);

  /// A2 restore-rebuild: rebuilds every live window's fire / cleanup timer
  /// pair from the restored state. Called when the restore dispatch is
  /// complete, before the first input or watermark reaches the operator
  /// (the handles are registered first, then restore, then this - both
  /// handles are in place). The timer queue rides outside the typed
  /// snapshot plane (C3); the timers are derived back from the restored
  /// state - fire = the window's end - 1 (the AfterEndOfWindow trigger
  /// time), cleanup at the same point at allowedLateness = 0, deduped into
  /// one queue entry. SESSION reads the live windows from the session
  /// mapping (the actual -> state window pairs); TUMBLE / HOP read them
  /// from the aggregating state's (key, window) entries, where the window
  /// IS the namespace. The last watermark starts from its initial value,
  /// so rebuilt timers due past it fire as the watermark advances, with or
  /// without new input.
  void rebuildTimersFromState();

  /// Emits the window's result row: key columns from the key row container,
  /// aggregate values from the state row, and every declared window
  /// property column from the window (the start / end boundaries; rowtime =
  /// the window's maxTimestamp; the proctime attribute null, the Flink
  /// semantics). Suppressed when the window has no state entry (the D1
  /// existence read).
  void emitWindowResult(const RowContainerStateKey& key, TimeWindow window);

  const std::shared_ptr<const GroupWindowAggregationNode> node_;
  GroupWindowAggsHandler* windowAggsHandler_;

  // Session window facilities, built in initialize() / wired in
  // initializeStateBackend().
  std::shared_ptr<GroupWindowAssigner<TimeWindow>> windowAssigner_;
  std::unique_ptr<WindowProcessFunction<TimeWindow>> windowFunction_;
  std::shared_ptr<WindowTrigger> trigger_;
  std::shared_ptr<WindowTriggerContext> triggerContext_;
  std::shared_ptr<WindowContext> windowContext_;
  std::shared_ptr<EventTimeTimerService<RowContainerStateKey, TimeWindow>>
      windowTimerService_;

  // Typed keyed state, five steps of initializeStateBackend.
  std::unique_ptr<HeapKeyedStateBackend<RowContainerStateKey>> keyedBackend_;
  std::unique_ptr<KeySelector> keySelector_;
  std::shared_ptr<AggregatingState<RowContainerStateKey, TimeWindow>> state_;
  std::shared_ptr<SessionWindowMappingState> mappingState_;

  // The last watermark this operator has processed (C5: the isWindowLate
  // and trigger decisions read it, never the timer service's view).
  // INT64_MIN is Flink InternalTimerServiceImpl's initial watermark: no
  // window is late before the first watermark arrives.
  int64_t lastWatermark_ = INT64_MIN;
  // True while this operator processes a popped timer inside
  // EventTimeTimerService::advanceWatermark: the service serializes queue
  // access with the Triggerable mutex and holds it across the onEventTime
  // callback, so a queue call that re-enters during the callback would
  // deadlock the plain mutex. The cleanup chain of the fired window
  // (clearTrigger -> deleteEventTimeTimer) targets exactly the popped
  // registration - firedTimer_ carries it, and the matching delete is the
  // no-op skip; any other re-entrant queue mutation fails loud (see
  // WindowTriggerContext::registerEventTimeTimer /
  // deleteEventTimeTimer).
  bool inTimerCallback_ = false;
  // The registration whose onEventTime callback is running (see
  // inTimerCallback_); null outside the callback.
  std::shared_ptr<TimerHeapInternalTimer<RowContainerStateKey, TimeWindow>>
      firedTimer_;
  // The key whose row / windows the process function operates on (engaged
  // before the first assignStateNamespace / onEventTime of every row and
  // timer).
  std::optional<RowContainerStateKey> currentKey_;
  // The late-element counter (Flink lateRecordsDroppedRate equivalent).
  uint64_t numLateRecordsDropped_ = 0;
};

} // namespace facebook::velox::stateful
