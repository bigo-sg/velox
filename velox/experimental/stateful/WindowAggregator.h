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
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/ProcessingTimeScheduler.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/WindowNamespace.h"

namespace facebook::velox::stateful {

/// Streaming window TVF aggregation (TUMBLE / HOP / CUMULATE / SESSION)
/// over the keyed state backend, the counterpart of Flink's
/// WindowAggOperator + XXXWindowAggProcessor family.
///
/// State model: AggregatingState<K, Ns> where K is the grouping key row
/// and Ns is the window namespace (SliceEndNs for aligned windows,
/// TimeWindowNs for SESSION). Window results are emitted when the
/// watermark (event-time) or the processing-time clock crosses
/// window_end - 1ms; fire is driven by a fire index (an ordered map from
/// due timestamp to dirty (key, window) pairs) rather than per-(key, ns)
/// timers. Output is append-only (+I); empty windows are not emitted
/// (state exists only for windows that received data). Late elements are
/// dropped only when every window owning the slice has already fired;
/// otherwise they are accumulated into an unfired window.
///
/// Limitations (fail loud): append-only input only (a non-+I rowKind
/// fails at runtime, L2; gluten rejects needRetraction at translation
/// time); DISTINCT/sorted aggregates are rejected by makeAggregateInfos
/// (L1); CUMULATE windows are rejected at operator init (L5, TODO:
/// separate follow-up task) — they must not silently run the
/// TUMBLE logic.
class WindowAggregator : public StatefulOperator {
 public:
  WindowAggregator(
      std::unique_ptr<exec::Operator> localAggregator,
      std::vector<StatefulOperatorPtr> targets,
      std::shared_ptr<const StreamWindowAggregationNode> node,
      exec::DriverCtx* driverCtx);

  void initialize() override;

  /// Builds the keyed state backend from the plan's KeyedStateSpec,
  /// binds the key selector, creates the AggregatingState with the
  /// window namespace, and wires the aggregate function offsets.
  void initializeStateBackend(StateBackend* stateBackend) override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  std::string name() const override {
    return "WindowAggregator";
  }

  /// Fires all windows whose dueTs <= watermark, then propagates the
  /// watermark downstream. Fire precedes propagation (Flink semantics).
  void processWatermark(int64_t timestamp) override;

  /// Processing-time trigger: fires all windows whose dueTs <= timestamp,
  /// deduplicated by lastTriggeredProcessingTime_. Called from the
  /// NativeCallbackBridge proc-time callback path.
  void onProcessingTime(int64_t timestamp);

  /// Processing-time tick on the task thread: StatefulTask::next invokes
  /// checkWatermarkStatus(now) on idle drains, which is where proc-time
  /// windows actually fire (the background wake-up timer only asks Java
  /// to drain the task; all firing happens on the task thread).
  void checkWatermarkStatus(int64_t now) override;

  /// Rows dropped by the two-phase late check (every owning window fired).
  uint64_t lateDroppedRows() const {
    return lateDroppedRows_;
  }

  /// Rebuilds the fire index from the live state entries after a backend
  /// restore (the index is not persisted): one fire entry per (key,
  /// window). The production checkpoint wiring (L3) calls this once the
  /// restore finishes; tests call it directly after
  /// keyedStateBackend().restore(bytes).
  void rebuildFireIndex();

  HeapKeyedStateBackend<RowContainerStateKey>& keyedStateBackend() {
    return *keyedBackend_;
  }

 private:
  /// A dirty (key, window) pair pending fire. The key is a copy (row
  /// pointer + cached hash + keyGroup), re-probed at fire time: the
  /// KeyTable reclaims zero-reference key rows at batch boundaries, and
  /// a fire entry registered in batch N may be consumed in batch N+M.
  /// Invariant: a fire entry (key, W) exists -> W's state entry exists
  /// -> the key row has not been reclaimed.
  struct FireEntry {
    RowContainerStateKey key;
    SliceEndNs ns;
    /// The owning window's start: the merged window's start for SESSION
    /// (emission needs the full range), the group/window start for the
    /// aligned types (kept uniform; the aligned fire paths recompute
    /// theirs).
    int64_t windowStart = 0;
  };

  /// Fires all fire-index entries with dueTs <= progress, per window type.
  void fireDue(int64_t progress);

  /// Fires one (key, windowEnd) of an unshared window (TUMBLE): reads the
  /// accumulator read-only, emits the result row (+I), clears the slice.
  void fireWindow(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd);

  /// Fires one HOP window: heap-merges the covered slices' accumulators
  /// into the first slice, emits, clears the first slice only, and chains
  /// the next window (Flink SharedSliceAssigner#mergeSlices/expiredSlices/
  /// nextTriggerWindow).
  void fireHopWindow(const RowContainerStateKey& key, int64_t windowEnd);

  /// Merges one source accumulator row into a target accumulator row via
  /// the intermediate (extractAccumulators -> addSingleGroupIntermediate
  /// Results) path; the source row is left untouched. Reuses
  /// mergeScratchColumns_ across calls (see the .cpp for the reuse
  /// safety argument). Used by the SESSION merge; the HOP fire path
  /// uses the batched mergeAccumulatorRowsBatch instead.
  void mergeAccumulatorRows(char* targetRow, char* sourceRow);


  /// Lazily-built single-row intermediate columns backing
  /// mergeAccumulatorRows (SESSION merge); one column per aggregate.
  std::vector<VectorPtr> mergeScratchColumns_;

  /// Batched HOP fire merge scratch (see WindowAggregatorHopMerge.h):
  /// per-aggregate intermediate columns sized to the widest window's
  /// covered-slice count, and the source row pointer gather.
  std::vector<VectorPtr> hopMergeScratchColumns_;
  std::vector<const char*> hopMergeScratchRows_;

  /// Reused single-element argument vector for the per-aggregate merge
  /// call (see mergeRow).
  std::vector<VectorPtr> singleIntermediateArg_{1};

  /// Emits the result row: [key columns, window_start, window_end, agg
  /// values] extracted from 'row'.
  void emitWindowResult(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd,
      char* row);

  /// Window start from window end (all window types: end - size).
  int64_t windowStart(int64_t windowEnd) const;

  /// Adds a (key, windowStart, windowEnd) entry to the fire index at
  /// dueTs = windowEnd - 1, deduplicated per (key row, windowEnd):
  /// Flink registers each (key, namespace) timer once; a duplicate
  /// registration would fire an already-cleared window and emit an
  /// empty result row.
  void registerFire(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd);

  /// Removes the (key, windowEnd) fire registration - the SESSION merge
  /// path's timer re-hang (Flink clears the merged-away windows'
  /// triggers in the merge callback,
  /// MergingWindowProcessFunction.java:178-182).
  void unregisterFire(
      const RowContainerStateKey& key,
      int64_t windowEnd);

  /// The two-stage global advance: input is the local partials
  /// [keys, intermediates, windowEnd]; the slice end comes from the
  /// attached column and intermediates merge via
  /// addIntermediateResults. Separate from the single-stage advance()
  /// so the raw accumulation path keeps its codegen (W1 lesson).
  void advancePartialInput();

  /// SESSION advance: per row opens [ts, ts+gap), merges intersecting
  /// in-flight windows (accumulators pinned to the surviving state
  /// window, Flink MergingWindowSet), accumulates into that state
  /// window and re-hangs fire registrations to the merged window end
  /// (Flink UnsliceWindowAggProcessor#processElement).
  void advanceSession();

  /// Fires one merged session window [windowStart, windowEnd): resolves
  /// the state window through the persistent mapping (a miss is a stale
  /// registration - merged-away or retired - and fires nothing), emits
  /// from the state window's accumulator, then clears the accumulator
  /// and retires the mapping entry (Flink cleanWindowIfNeeded).
  void fireSessionWindow(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd);

  /// Processing-time mode only: (re-)arms the one-shot background wake-up
  /// timer at the fire-index head so the Java mailbox drains the task and
  /// checkWatermarkStatus fires due windows on time even without input.
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
  std::unique_ptr<exec::Operator> localAggregator_;
  std::vector<exec::AggregateInfo> aggregates_;
  std::unique_ptr<HeapKeyedStateBackend<RowContainerStateKey>> keyedBackend_;
  std::unique_ptr<KeySelector> keySelector_;
  std::unique_ptr<SliceAssigner> sliceAssigner_;
  std::shared_ptr<AggregatingState<RowContainerStateKey, SliceEndNs>> state_;

  /// Fire index: ordered map from due timestamp to dirty (key, window)
  /// pairs. begin()->first is the next trigger watermark.
  std::multimap<int64_t, FireEntry> fireIndex_;

  /// Deduplication set for the fire index, keyed by (key row, windowEnd).
  /// Entries are inserted in registerFire() and erased when the
  /// corresponding multimap entry is popped in fireDue() (or re-hung by
  /// a SESSION merge in unregisterFire()).
  std::set<std::pair<const char*, int64_t>> registeredFires_;

  /// SESSION only: accumulators keyed by the pinned state-window
  /// namespace (Flink MergingWindowSet's "state window"); null for the
  /// aligned types, which use state_.
  std::shared_ptr<AggregatingState<RowContainerStateKey, TimeWindowNs>>
      sessionState_;

  /// SESSION only: the persistent actual-window -> state-window mapping
  /// (Flink's "session-window-mapping" MapState; the in-flight window
  /// set is rebuilt from it per key per batch). Null for the aligned
  /// types.
  std::shared_ptr<MapState<RowContainerStateKey, SliceEndNs, TimeWindowNs, TimeWindowNs>>
      mappingState_;

  /// Processing-time wake-up: a background one-shot timer at the fire
  /// index head that asks Java (via NativeCallbackBridge::onProcessingTime)
  /// to drain the task; the actual fire runs on the task thread in
  /// checkWatermarkStatus. Only created for processing-time windows.
  std::unique_ptr<ProcessingTimeScheduler> procTimeScheduler_;
  std::optional<std::string> procTimerName_;
  int64_t armedDueTs_ = std::numeric_limits<int64_t>::max();

  /// Keyless mode (empty grouping): gluten appends a constant BIGINT
  /// "$keyless_key" column to the declared input type and keys on it; the
  /// runtime batches are one column narrower and get the column wrapped
  /// on in addInput.
  bool keyless_ = false;
  RowTypePtr declaredInputType_;
  /// The key column count in the OUTPUT projection - zero for keyless
  /// (the wrap column exists in the input type only).
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
  /// -1 when the query carries no count star; hopping windows require
  /// one (Flink checkArgument in WindowIsEmptySupplier).
  int32_t countStarIndex_ = -1;

  /// The two-stage global side: the input is the partial row [keys,
  /// intermediates, windowEnd] (keyless: [intermediates, windowEnd]
  /// plus the wrapped "$keyless_key" appended after windowEnd) and the
  /// accumulation merges intermediate columns into the state rows
  /// (kFinal step) instead of accumulating raw input (kSingle).
  bool isPartialInput_ = false;
  /// Accumulator-row offset of the count star column (set during the
  /// setOffsets wiring); the count accumulator is a never-null inline
  /// int64 (SimpleNumericAggregate<bool, int64_t, int64_t>), so a raw
  /// read at this offset is the live count.
  int32_t countStarOffset_ = -1;

  static constexpr vector_size_t kZeroIndex = 0;
};

/// Count-star helpers implemented in WindowAggregatorCountStar.cpp.
/// Kept out of the WindowAggregator translation unit so the cold
/// detection/check code does not perturb the hot-path codegen there.
int32_t findCountStarIndex(
    const std::vector<core::AggregationNode::Aggregate>& aggregates,
    bool requireCountStar);
bool countStarIsZero(const char* row, int32_t countStarOffset);
int32_t countStarColumnOffset(TypeKind intermediateKind, int32_t offset);

/// Keyless mode: appends the "$keyless_key" constant BIGINT column the
/// declared input type carries (gluten adds it for empty grouping) to a
/// runtime batch, which is one column narrower. Shared by the window
/// aggregators; implemented in the cold count-star TU.
RowVectorPtr wrapKeylessColumn(
    memory::MemoryPool* pool,
    const RowTypePtr& declaredInputType,
    const RowVectorPtr& input);

} // namespace facebook::velox::stateful
