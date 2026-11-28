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
struct WindowTimerKey;

/// Streaming window TVF aggregation (TUMBLE / HOP) over the keyed state
/// backend, the counterpart of Flink's WindowAggOperator +
/// heap at fire time (fireHopWindow, Flink SharedSliceAssigner
/// the grouping key row is the key, the window slice end (Flink's Long
/// timers dispatch in onEventTime exactly like Flink's. The timer key
/// cannot touch operator state (Flink defers processing-time callbacks
class WindowAggregator : public StatefulOperator,
                         public Triggerable<WindowTimerKey, SliceEndNs> {
 public:
  WindowAggregator(
      std::unique_ptr<exec::Operator> localAggregator,
      std::vector<StatefulOperatorPtr> targets,
      std::shared_ptr<const StreamWindowAggregationNode> node,
      exec::DriverCtx* driverCtx);

  void initialize() override;

  /// Builds the keyed state backend from the plan's KeyedStateSpec,
  void initializeStateBackend(StateBackend* stateBackend) override;

  /// Production restore path: StatefulTask::initializeState runs
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
  void onEventTime(
      std::shared_ptr<TimerHeapInternalTimer<WindowTimerKey, SliceEndNs>> timer)
      override;

  /// Processing-time trigger: fires all proc-time windows whose dueTs <=
  void onProcessingTime(int64_t timestamp);

  /// Processing-time tick on the task thread: StatefulTask::next invokes
  void checkWatermarkStatus(int64_t now) override;

  /// Rows dropped by the late check (every owning window fired).
  uint64_t lateDroppedRows() const {
    return lateDroppedRows_;
  }

  /// Test/diagnostic: the due time of the currently armed proc-time
  int64_t armedWakeUpDueTs() const {
    return armedDueTs_;
  }

  /// Re-registers the window timers from the live state entries after a
  void rebuildFireIndex();

  HeapKeyedStateBackend<RowContainerStateKey>& keyedStateBackend() {
    return *keyedBackend_;
  }

 private:
  /// A due (key, window) pair pending a processing-time fire (the
  struct FireEntry {
    RowContainerStateKey key;
    SliceEndNs ns;
    int64_t windowStart = 0;
  };

  /// Fires all proc-time fire-index entries with dueTs <= progress.
  void fireDue(int64_t progress);

  /// Fires one (key, windowEnd) window: reads the accumulator
  void fireWindow(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd);

  /// Fires one HOP window: heap-merges the covered slices' accumulators
  /// the next window (Flink SharedSliceAssigner#mergeSlices/expiredSlices/
  void fireHopWindow(const RowContainerStateKey& key, int64_t windowEnd);

  /// Emits the result row: [key columns, window_start, window_end, agg
  void emitWindowResult(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd,
      char* row);

  /// Window start from window end (TUMBLE: end - size).
  int64_t windowStart(int64_t windowEnd) const;

  /// Registers the window fire: event time goes to the
  /// InternalTimerService (due time = window_end - 1, Flink's
  void registerFire(
      const RowContainerStateKey& key,
      char* stateRow,
      int64_t windowStart,
      int64_t windowEnd);

  /// The two-stage global advance: input is the local partials
  /// so the raw accumulation path keeps its codegen (W1 lesson).
  void advancePartialInput();

  /// Flushes the timer registrations deferred during an event-time
  /// processWatermark call (Flink's dispatch loop re-checks the queue
  void flushPendingTimers(int64_t watermark);

  /// Processing-time mode only: (re-)arms the one-shot background wake-up
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
  std::unique_ptr<WindowSliceAssigner> sliceAssigner_;
  /// The aggregating state downcast to the heap implementation once at
  HeapAggregatingState<RowContainerStateKey, SliceEndNs>* state_ = nullptr;
  /// Operator-owned fire-row cache: (key row, windowEnd) -> the window's
  /// was already fired/cleared, and the R1 key-liveness invariant keeps
  std::map<std::pair<const char*, int64_t>, char*> fireRowCache_;
  /// The max parallelism the keyed backend derives key groups from
  uint32_t maxParallelism_ = 1;
  /// Ownership of state_.
  std::shared_ptr<AggregatingState<RowContainerStateKey, SliceEndNs>>
      stateHandle_;
  std::unique_ptr<InternalTimerService<WindowTimerKey, SliceEndNs>>
      timerService_;
  /// Deferred (timer key, ns, due) registrations made during an
  std::vector<std::tuple<WindowTimerKey, SliceEndNs, int64_t>> pendingTimers_;
  /// Windows fired in the current processWatermark pass (key row,
  /// queue only deduplicates against pending entries). Flink's
  std::set<std::pair<const char*, int64_t>> firedThisPass_;
  /// True while an onEventTime dispatch is on the call stack.
  bool inEventTimeDispatch_ = false;
  /// Reads the slice row cache; nullptr means the slice has no live
  char* lookupSliceRow(const char* keyRow, int64_t sliceEnd) {
    auto it = sliceRowCache_.find({keyRow, sliceEnd});
    return it == sliceRowCache_.end() ? nullptr : it->second;
  }

  /// Slice-level row cache: (key row, sliceEnd) -> the slice's
  std::map<std::pair<const char*, int64_t>, char*> sliceRowCache_;

  /// Proc-time fire index (wake-up + task-thread firing only; see the
  std::multimap<int64_t, FireEntry> procFireIndex_;

  /// Deduplication set for the proc-time fire index, keyed by (key row,
  std::set<std::pair<const char*, int64_t>> registeredFires_;

  /// Processing-time wake-up: a background one-shot timer at the proc
  std::unique_ptr<ProcessingTimeScheduler> procTimeScheduler_;
  std::optional<std::string> procTimerName_;
  int64_t armedDueTs_ = std::numeric_limits<int64_t>::max();

  /// Keyless mode (empty grouping): detected from the planner's legacy
  bool keyless_ = false;
  /// The key column count in the OUTPUT projection.
  int32_t outputKeyCount_ = 0;

  /// Output positions of window_start/window_end in the projection,
  int32_t windowStartIdx_ = 0;
  int32_t windowEndIdx_ = 1;

  /// The shift time zone resolved once from the plan node (nullptr =
  const tzdb::time_zone* tz_ = nullptr;

  /// Event-time progress starts at Long.MIN_VALUE (Flink currentProgress):
  int64_t currentProgress_ = std::numeric_limits<int64_t>::min();
  int64_t lastTriggeredProcessingTime_ = 0;
  uint64_t lateDroppedRows_ = 0;
  int32_t numKeys_ = 0;
  int32_t numLeadingColumns_ = 0;
  RowVectorPtr input_;
  std::vector<char*> stateRows_;
  std::vector<char*> rowGroups_;
  std::unordered_map<const char*, vector_size_t> rowToDistinct_;

  /// The two-stage global side: the input is the partial row [keys,
  bool isPartialInput_ = false;

  /// The zero-argument COUNT call in the aggregate list (Flink's
  /// count star reads 0 emits nothing (Flink WindowIsEmptySupplier).
  int32_t countStarIndex_ = -1;
  /// Accumulator-row offset of the count star column (set during the
  int32_t countStarOffset_ = -1;

  /// Batched HOP fire merge scratch (see WindowAggregatorHopMerge.h):
  std::vector<VectorPtr> hopMergeScratchColumns_;
  std::vector<const char*> hopMergeScratchRows_;

  /// Reused single-argument vector for the per-aggregate merge call
  std::vector<VectorPtr> singleIntermediateArg_{1};

  static constexpr vector_size_t kZeroIndex = 0;
};

/// Operator-owned timer key: a trivially copyable snapshot of the
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
template <typename N>
struct HeapTimerHasher<WindowTimerKey, N> {
  size_t operator()(const TimerHeapInternalTimer<WindowTimerKey, N>& a) const {
    return bits::hashMix(a.timestamp(), a.key().hash);
  }
};
/// Count-star helpers implemented in WindowAggregatorCountStar.cpp.
int32_t findCountStarIndex(
    const std::vector<core::AggregationNode::Aggregate>& aggregates,
    bool requireCountStar);
bool countStarIsZero(const char* row, int32_t countStarOffset);
int32_t countStarColumnOffset(TypeKind intermediateKind, int32_t offset);

} // namespace facebook::velox::stateful
