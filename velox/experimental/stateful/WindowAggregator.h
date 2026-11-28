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
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "velox/exec/AggregateInfo.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/EmptyOperator.h"
#include "velox/experimental/stateful/InternalTimerService.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/ProcessingTimeScheduler.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"
#include "velox/experimental/stateful/WindowAggregatorHopMerge.h"
#include "velox/experimental/stateful/state/HeapAggregatingState.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/window/WindowNamespace.h"
#include "velox/experimental/stateful/window/WindowSliceAssigner.h"

namespace facebook::velox::stateful {

/// Operator-owned timer key (full definition after the operator class):
/// a trivially copyable snapshot of the RowContainerStateKey fields.
struct WindowTimerKey;

/// Streaming window TVF aggregation (TUMBLE / HOP) over the keyed state
/// backend, the counterpart of Flink's WindowAggOperator +
/// SliceWindowAggProcessor. A HOP window's covered slices merge on the
/// heap at fire time (fireHopWindow, Flink SharedSliceAssigner
/// mergeSlices/expiredSlices/nextTriggerWindow). SESSION / CUMULATE and
/// the two-stage local-global split are planned follow-up PRs; the plan
/// translation rejects them (see StatefulPlanner).
///
/// State model: AggregatingState<RowContainerStateKey, SliceEndNs> -
/// the grouping key row is the key, the window slice end (Flink's Long
/// namespace for slicing window processors) is the namespace. Window
/// results are emitted when the watermark (event time) or the
/// processing-time tick crosses window_end - 1ms. Output is append-only
/// (+I); empty windows are not emitted (state exists only for windows
/// that received data). Late elements are dropped only when every
/// window owning the slice has already fired; otherwise they are
/// accumulated into an unfired window.
///
/// Timers: event-time window fires are managed through the framework's
/// InternalTimerService (this operator implements Triggerable; the due
/// timers dispatch in onEventTime exactly like Flink's. The timer key
/// is WindowTimerKey (operator-owned: a trivially copyable snapshot of
/// the RowContainerStateKey fields) so the framework TimerHeapInternalTimer
/// gets its required equality/assignability without touching the
/// framework's StateKey class.
/// InternalTimerServiceImpl). The timer key is the RowContainerStateKey
/// (the timer carries the key copy; Hasher specialization below adapts
/// the framework's hashMix to StateKey::hash()). Processing-time
/// windows keep the wake-up + task-thread tick pattern: the framework's
/// ProcessingTimeTimerService dispatches on the scheduler thread, which
/// cannot touch operator state (Flink defers processing-time callbacks
/// into the task mailbox; our NativeCallbackBridge wake-up is that
/// mailbox hop), so the proc-time due index stays operator-local and
/// fires on the task thread in checkWatermarkStatus.
///
/// Limitations (fail loud): append-only input only (a non-+I rowKind
/// fails at runtime, L2; gluten rejects needRetraction at translation
/// time); DISTINCT/sorted aggregates are rejected by makeAggregateInfos
/// (L1); keyless windows (empty grouping) are rejected at initialize
/// (the keyed state backend requires at least one key channel; empty-key
/// support is a planned follow-up).
class WindowAggregator : public StatefulOperator,
                         public Triggerable<WindowTimerKey, SliceEndNs> {
 public:
  WindowAggregator(
      std::vector<StatefulOperatorPtr> targets,
      std::shared_ptr<const StreamWindowAggregationNode> node,
      exec::DriverCtx* driverCtx);

  void initialize() override;

  /// Builds the keyed state backend from the plan's KeyedStateSpec,
  /// binds the key selector, creates the AggregatingState with the
  /// window namespace, and wires the aggregate function offsets.
  void initializeStateBackend(StateBackend* stateBackend) override;

  /// Production restore path: StatefulTask::initializeState runs
  /// restoreState before the chain's initializeState, so by the time
  /// this hook runs the keyed backend carries the restored entries -
  /// re-register the window timers from them or the restored in-flight
  /// windows never fire (timers are not persisted, L3). A no-op on
  /// fresh start (the state is empty).
  void initializeState() override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  std::string name() const override {
    return "WindowAggregator";
  }

  /// Fires all windows whose dueTs <= watermark, then propagates the
  /// watermark downstream. Fire precedes propagation (Flink semantics).
  void processWatermark(int64_t timestamp) override;

  /// Event-time timer dispatch: one due (key, window) timer from the
  /// InternalTimerService (advanceWatermark polls and dispatches here).
  void onEventTime(
      std::shared_ptr<TimerHeapInternalTimer<WindowTimerKey, SliceEndNs>> timer)
      override;

  /// Processing-time trigger: fires all proc-time windows whose dueTs <=
  /// timestamp, deduplicated by lastTriggeredProcessingTime_. Called
  /// from the NativeCallbackBridge proc-time callback path.
  void onProcessingTime(int64_t timestamp);

  /// Processing-time tick on the task thread: StatefulTask::next invokes
  /// checkWatermarkStatus(now) on idle drains, which is where proc-time
  /// windows actually fire (the background wake-up timer only asks Java
  /// to drain the task; all firing happens on the task thread).
  void checkWatermarkStatus(int64_t now) override;

  /// Rows dropped by the late check (every owning window fired).
  uint64_t lateDroppedRows() const {
    return lateDroppedRows_;
  }

  /// Test/diagnostic: the due time of the currently armed proc-time
  /// wake-up timer (int64 max when none is armed). Observes the one-shot
  /// re-arm behavior (a fired timer must not block later re-arms).
  int64_t armedWakeUpDueTs() const {
    return armedDueTs_;
  }

  /// Re-registers the window timers from the live state entries after a
  /// backend restore (timers are not persisted): one timer per (key,
  /// window). The production checkpoint wiring (L3) calls this once the
  /// restore finishes; tests call it directly after
  /// keyedStateBackend().restore(bytes).
  void rebuildFireIndex();

  HeapKeyedStateBackend<RowContainerStateKey>& keyedStateBackend() {
    return *keyedBackend_;
  }

 private:
  /// A due (key, window) pair pending a processing-time fire (the
  /// proc-time wake-up index; event-time timers live in the
  /// InternalTimerService). The key is a copy (row pointer + cached
  /// hash + keyGroup), re-probed at fire time: the KeyTable reclaims
  /// zero-reference key rows at batch boundaries, and an entry
  /// registered in batch N may be consumed in batch N+M. Invariant: an
  /// entry (key, W) exists -> W's state entry exists -> the key row has
  /// not been reclaimed.
  struct FireEntry {
    RowContainerStateKey key;
    SliceEndNs ns;
    int64_t windowStart = 0;
  };

  /// Fires all proc-time fire-index entries with dueTs <= progress.
  /// (Event-time fires dispatch through the InternalTimerService in
  /// processWatermark -> advanceWatermark -> onEventTime.)
  void fireDue(int64_t progress);

  /// Fires one (key, windowEnd) window: reads the accumulator
  /// read-only, emits the result row (+I), clears the window state.
  void fireWindow(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd);

  /// Fires one HOP window: heap-merges the covered slices' accumulators
  /// into the first slice, emits, clears the first slice only, and chains
  /// the next window (Flink SharedSliceAssigner#mergeSlices/expiredSlices/
  /// nextTriggerWindow).
  void fireHopWindow(const RowContainerStateKey& key, int64_t windowEnd);

  /// Emits the result row: [key columns, window_start, window_end, agg
  /// values] extracted from 'row'.
  void emitWindowResult(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd,
      char* row);

  /// Window start from window end (TUMBLE: end - size).
  int64_t windowStart(int64_t windowEnd) const;

  /// Registers the window fire: event time goes to the
  /// InternalTimerService (due time = window_end - 1, Flink's
  /// maxTimestamp; the queue deduplicates (key, ns, ts) registrations),
  /// processing time to the operator-local wake-up index.
  void registerFire(
      const RowContainerStateKey& key,
      char* stateRow,
      int64_t windowStart,
      int64_t windowEnd);

  /// Flushes the timer registrations deferred during an event-time
  /// dispatch (the HOP fire chain re-registers the next window from
  /// onEventTime; the dispatch holds the Triggerable mutex, which a
  /// std::mutex cannot re-enter), then re-advances the watermark once
  /// so a chain entry that is already due fires in the same
  /// processWatermark call (Flink's dispatch loop re-checks the queue
  /// head the same way).
  void flushPendingTimers(int64_t watermark);

  /// Processing-time mode only: (re-)arms the one-shot background wake-up
  /// timer at the proc-time fire-index head so the Java mailbox drains
  /// the task and checkWatermarkStatus fires due windows on time even
  /// without input.
  void armProcTimeWakeUp();

  /// Shuts the proc-time wake-up scheduler down (close path).
  void shutdownProcTimeScheduler();

  /// Batch accumulator initialization for freshly materialized state rows.
  void initializeRows(char** rows, size_t numRows);

  /// Merges intermediate columns into a state row (restore path).
  void mergeRow(char* row, const std::vector<VectorPtr>& columns);

  /// Asserts the input record carries only +I rowKinds.
  void checkInputRowKinds(const StreamRecord& record) const;

  const std::shared_ptr<const StreamWindowAggregationNode> node_;
  std::vector<exec::AggregateInfo> aggregates_;
  std::unique_ptr<HeapKeyedStateBackend<RowContainerStateKey>> keyedBackend_;
  std::unique_ptr<KeySelector> keySelector_;
  std::unique_ptr<WindowSliceAssigner> sliceAssigner_;
  /// The aggregating state downcast to the heap implementation once at
  /// creation: the fire and restore paths need the read-only get() and
  /// forEachEntry() walk, which stay off the AggregatingState interface
  /// (the framework interface is unchanged; the methods are concrete on
  /// the heap state).
  HeapAggregatingState<RowContainerStateKey, SliceEndNs>* state_ = nullptr;
  /// Operator-owned fire-row cache: (key row, windowEnd) -> the window's
  /// accumulator row, captured at registerFire. The fire path reads the
  /// window through the cache instead of a state peek (an operator-owned
  /// replacement for a read-only state get(): a miss means the window
  /// was already fired/cleared, and the R1 key-liveness invariant keeps
  /// the row pointer valid while the registration exists).
  std::map<std::pair<const char*, int64_t>, char*> fireRowCache_;
  /// The max parallelism the keyed backend derives key groups from
  /// (StateKey::keyGroup() = hash % maxParallelism); captured from the
  /// state backend parameters so onEventTime can rebuild a full
  /// RowContainerStateKey from the timer's WindowTimerKey.
  uint32_t maxParallelism_ = 1;
  /// Ownership of state_.
  std::shared_ptr<AggregatingState<RowContainerStateKey, SliceEndNs>>
      stateHandle_;
  std::unique_ptr<InternalTimerService<WindowTimerKey, SliceEndNs>>
      timerService_;
  /// Deferred (timer key, ns, due) registrations made during an
  /// event-time dispatch (see registerFire); flushed under no lock.
  std::vector<std::tuple<WindowTimerKey, SliceEndNs, int64_t>> pendingTimers_;
  /// Windows fired in the current processWatermark pass (key row,
  /// window end): the HOP fire chain defers its next-window
  /// registration to the post-pass flush, by which time the next
  /// window's own timer may already have been polled and fired in the
  /// same pass - re-registering it would duplicate the fire (the
  /// queue only deduplicates against pending entries). Flink's
  /// dispatch loop instead re-checks the queue head, so its
  /// registration deduplicates against the still-pending timer; the
  /// fired set is the deferred-flush equivalent of that invariant.
  std::set<std::pair<const char*, int64_t>> firedThisPass_;
  /// True while an onEventTime dispatch is on the call stack.
  bool inEventTimeDispatch_ = false;
  /// Reads the slice row cache; nullptr means the slice has no live
  /// state (the caller must treat it as an empty slice, never
  /// materialize on this path).
  char* lookupSliceRow(const char* keyRow, int64_t sliceEnd) {
    auto it = sliceRowCache_.find({keyRow, sliceEnd});
    return it == sliceRowCache_.end() ? nullptr : it->second;
  }

  /// Slice-level row cache: (key row, sliceEnd) -> the slice's
  /// accumulator row, mirrored at every slice mutation (accumulate /
  /// materialize / remove). The HOP fire path reads a window's
  /// covered slices through this cache (an operator-owned replacement
  /// for a read-only state get()); a miss means the slice has no live
  /// state.
  std::map<std::pair<const char*, int64_t>, char*> sliceRowCache_;

  /// Proc-time fire index (wake-up + task-thread firing only; see the
  /// class comment for why proc timers stay out of the framework's
  /// ProcessingTimeTimerService): ordered map from due timestamp to due
  /// (key, window) pairs.
  std::multimap<int64_t, FireEntry> procFireIndex_;

  /// Deduplication set for the proc-time fire index, keyed by (key row,
  /// windowEnd); the event-time side dedups inside the timer queue.
  std::set<std::pair<const char*, int64_t>> registeredFires_;

  /// Processing-time wake-up: a background one-shot timer at the proc
  /// fire-index head that asks Java (via NativeCallbackBridge::
  /// onProcessingTime) to drain the task; the actual fire runs on the
  /// task thread in checkWatermarkStatus. Only created for
  /// processing-time windows.
  std::unique_ptr<ProcessingTimeScheduler> procTimeScheduler_;
  std::optional<std::string> procTimerName_;
  int64_t armedDueTs_ = std::numeric_limits<int64_t>::max();

  /// Keyless mode (empty grouping): detected from the planner's legacy
  /// "$keyless_key" channel and rejected at initialize (empty-key state
  /// backend support is a planned follow-up).
  bool keyless_ = false;
  /// The key column count in the OUTPUT projection.
  int32_t outputKeyCount_ = 0;

  /// Output positions of window_start/window_end in the projection,
  /// resolved from windowStartIndex/windowEndIndex at initialize (-1 in
  /// the plan falls back to right after the key columns).
  int32_t windowStartIdx_ = 0;
  int32_t windowEndIdx_ = 1;

  /// The shift time zone resolved once from the plan node (nullptr =
  /// UTC): drives input-side window alignment, late checks and the
  /// DST-corrected fire due times.
  const tzdb::time_zone* tz_ = nullptr;

  /// Event-time progress starts at Long.MIN_VALUE (Flink currentProgress):
  /// the first watermark, including 0 or a negative one, triggers due
  /// windows and advances the progress.
  int64_t currentProgress_ = std::numeric_limits<int64_t>::min();
  int64_t lastTriggeredProcessingTime_ = 0;
  uint64_t lateDroppedRows_ = 0;
  int32_t numKeys_ = 0;
  int32_t numLeadingColumns_ = 0;
  RowVectorPtr input_;
  std::vector<char*> stateRows_;
  std::vector<char*> rowGroups_;
  std::unordered_map<const char*, vector_size_t> rowToDistinct_;

  /// The zero-argument COUNT call in the aggregate list (Flink's
  /// "count star"), used for empty-window suppression: a window whose
  /// count star reads 0 emits nothing (Flink WindowIsEmptySupplier).
  /// -1 when the query carries no count star.
  int32_t countStarIndex_ = -1;
  /// Accumulator-row offset of the count star column (set during the
  /// setOffsets wiring); the count accumulator is a never-null inline
  /// int64 (SimpleNumericAggregate<bool, int64_t, int64_t>), so a raw
  /// read at this offset is the live count.
  int32_t countStarOffset_ = -1;

  /// Batched HOP fire merge scratch (see WindowAggregatorHopMerge.h):
  /// per-aggregate intermediate columns sized to the widest window's
  /// covered-slice count, and the source row pointer gather.
  std::vector<VectorPtr> hopMergeScratchColumns_;
  std::vector<const char*> hopMergeScratchRows_;

  /// Reused single-argument vector for the per-aggregate merge call
  /// (see mergeRow; the restore path consumes the input
  /// synchronously).
  std::vector<VectorPtr> singleIntermediateArg_{1};

  static constexpr vector_size_t kZeroIndex = 0;
};

/// Operator-owned timer key: a trivially copyable snapshot of the
/// RowContainerStateKey fields the fire path needs (the framework's
/// TimerHeapInternalTimer stores the key by value, requires equality
/// and assignability - RowContainerStateKey has const members and no
/// operator==, and both stay untouched on the framework side). Equality
/// is by row pointer: the KeyTable canonicalizes keys by content, so
/// one row pointer corresponds to exactly one key value.
struct WindowTimerKey {
  const RowContainerKeySchema* schema;
  const char* row;
  uint64_t hash;
  uint32_t keyGroup;

  bool operator==(const WindowTimerKey& other) const {
    return row == other.row;
  }
};

/// Timer hashing for the operator-owned key: the framework's
/// HeapTimerHasher hashMixes (timestamp, key) and is written for
/// uint64-convertible keys; WindowTimerKey hashes through its cached
/// StateKey hash. The specialization lives with the operator, not the
/// framework headers.
template <typename N>
struct HeapTimerHasher<WindowTimerKey, N> {
  size_t operator()(const TimerHeapInternalTimer<WindowTimerKey, N>& a) const {
    return bits::hashMix(a.timestamp(), a.key().hash);
  }
};
/// Count-star helpers implemented in WindowAggregatorCountStar.cpp.
/// Kept out of the WindowAggregator translation unit so the cold
/// detection/check code does not perturb the hot-path codegen there.
int32_t findCountStarIndex(
    const std::vector<core::AggregationNode::Aggregate>& aggregates,
    bool requireCountStar);
bool countStarIsZero(const char* row, int32_t countStarOffset);
int32_t countStarColumnOffset(TypeKind intermediateKind, int32_t offset);

} // namespace facebook::velox::stateful
