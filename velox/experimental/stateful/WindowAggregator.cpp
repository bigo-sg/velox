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

#include "velox/experimental/stateful/WindowAggregator.h"

#include <cstdlib>

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregationMasks.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/agg/AggregateInfos.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/experimental/stateful/window/WindowTimezoneUtil.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

namespace {

/// The InternalTimerService facade's processing-time slot for
class NoopProcessingTimeScheduler : public ProcessingTimeScheduler {
 public:
  std::optional<std::string> registerTimer(
      int64_t /*timestamp*/,
      ProcessingTimerTask /*task*/) override {
    VELOX_UNREACHABLE(
        "WindowAggregator registers no processing timer with the "
        "InternalTimerService (proc-time windows use the wake-up path)");
  }
};

} // namespace

WindowAggregator::WindowAggregator(
    std::vector<StatefulOperatorPtr> targets,
    std::shared_ptr<const StreamWindowAggregationNode> node,
    exec::DriverCtx* driverCtx)
    : StatefulOperator(
          std::make_unique<EmptyOperator>(0, driverCtx, node),
          std::move(targets)),
      node_(std::move(node)) {}

void WindowAggregator::initialize() {
  const WindowType wt = static_cast<WindowType>(node_->windowType());
  // SESSION / CUMULATE and the two-stage local-global split are
  if (wt != WindowType::TUMBLE && wt != WindowType::HOP &&
      wt != WindowType::CUMULATIVE) {
    VELOX_UNSUPPORTED(
        "WindowAggregator supports TUMBLE / HOP / CUMULATE windows in "
        "this PR; SESSION and the two-stage split are planned "
        "follow-ups");
  }
  VELOX_CHECK_NOT_NULL(
      node_->keyedStateSpec(),
      "StreamWindowAggregationNode must carry a KeyedStateSpec");
  VELOX_CHECK_GT(
      node_->size(), 0, "window size must be positive, got {}", node_->size());
  if (wt == WindowType::HOP || wt == WindowType::CUMULATIVE) {
    const char* kind =
        wt == WindowType::HOP ? "HOP window slide" : "CUMULATE window step";
    VELOX_CHECK_GT(
        node_->step(), 0, "{} must be positive, got {}", kind, node_->step());
    VELOX_CHECK_EQ(
        node_->size() % node_->step(),
        0,
        "{} size must be a multiple of step (size={}, step={})",
        wt == WindowType::HOP ? "HOP" : "CUMULATE",
        node_->size(),
        node_->step());
  }
  VELOX_CHECK_LT(
      std::abs(node_->offset()),
      node_->size(),
      "window offset must be in (-size, size)");
  const auto* aggNode = std::dynamic_pointer_cast<const core::AggregationNode>(
                            node_->aggregation())
                            .get();
  VELOX_CHECK_NOT_NULL(
      aggNode, "StreamWindowAggregationNode must carry an AggregationNode");
  auto inputType = std::dynamic_pointer_cast<const RowType>(
      aggNode->sources()[0]->outputType());
  VELOX_CHECK_NOT_NULL(
      inputType, "AggregationNode input type must be a RowType");
  // Keyless windows (empty grouping): gluten appends a constant BIGINT
  const auto& keyChannels = node_->keyedStateSpec()->keyChannels();
  keyless_ = keyChannels.size() == 1 &&
      std::string(inputType->nameOf(keyChannels[0])) == "$keyless_key";
  numKeys_ = static_cast<int32_t>(keyChannels.size());
  if (keyless_) {
    VELOX_UNSUPPORTED(
        "WindowAggregator does not support keyless windows (empty "
        "grouping) yet: the keyed state backend requires at least one "
        "key channel, and a synthetic key column is not the right "
        "substitute for Flink's empty key (EmptyRowDataKeySelector - a "
        "zero-field key row, every record in one key group). Empty-key "
        "support in the keyed state backend is planned as a follow-up "
        "and will re-enable keyless windows without any synthetic "
        "column, matching Flink's [accs, windowEnd] layout.");
  }
  outputKeyCount_ = numKeys_;
  // Window columns carry windowStartIndex/windowEndIndex; -1 falls back
  windowStartIdx_ = node_->windowStartIndex() >= 0 ? node_->windowStartIndex()
                                                   : outputKeyCount_;
  windowEndIdx_ = node_->windowEndIndex() >= 0 ? node_->windowEndIndex()
                                               : outputKeyCount_ + 1;
  // The aggregate output columns sit where the plan's projection puts
  const auto& outputType = node_->outputType();
  std::vector<bool> taken(outputType->size(), false);
  for (int32_t k = 0; k < outputKeyCount_; ++k) {
    taken[k] = true;
  }
  taken[windowStartIdx_] = true;
  taken[windowEndIdx_] = true;
  const int32_t freeColumns =
      static_cast<int32_t>(outputType->size()) - outputKeyCount_ - 2;
  VELOX_CHECK_EQ(
      freeColumns,
      static_cast<int32_t>(aggNode->aggregates().size()),
      "Output schema must be keys + aggregates + window_start/end in some "
      "order, got {}",
      outputType->toString());
  numLeadingColumns_ = -1;
  for (int32_t c = 0; c < static_cast<int32_t>(outputType->size()); ++c) {
    if (!taken[c]) {
      numLeadingColumns_ = c;
      break;
    }
  }
  // A zero-aggregate window TVF (GROUP BY key, window_start, window_end
  VELOX_CHECK(
      numLeadingColumns_ >= 0 || aggNode->aggregates().empty(),
      "No output column left for aggregates in {}",
      outputType->toString());
  // Count star detection (Flink indexOfCountStar): the first
  countStarIndex_ =
      findCountStarIndex(aggNode->aggregates(), wt == WindowType::HOP);
  aggregates_ = makeAggregateInfos(
      aggNode->aggregates(),
      inputType,
      node_->outputType(),
      numLeadingColumns_,
      op()->operatorCtx()->driverCtx()->queryConfig(),
      memoryPool(),
      core::AggregationNode::Step::kSingle);
}

void WindowAggregator::initializeState() {
  // StatefulTask::initializeState: initStateBackend -> restoreState ->
  rebuildFireIndex();
  StatefulOperator::initializeState();
}

void WindowAggregator::initializeStateBackend(StateBackend* stateBackend) {
  tz_ = WindowTimezoneUtil::locateTimeZone(node_->shiftTimeZone());
  maxParallelism_ = stateBackend->parameters()->getMaxParallelism();
  const auto& spec = *node_->keyedStateSpec();
  keyedBackend_ = createKeyedStateBackend<RowContainerStateKey>(
      *stateBackend, spec, memoryPool());
  keySelector_ = std::make_unique<KeySelector>(
      keyedBackend_->keyIndex(), spec.keyChannels());

  sliceAssigner_ = std::make_unique<WindowSliceAssigner>(
      node_->size(),
      node_->step(),
      node_->offset(),
      Window::getType(node_->windowType()),
      node_->rowtimeIndex(),
      tz_);

  // Aggregate functions were created in initialize() (op() available from

  std::vector<exec::Accumulator> accumulators;
  accumulators.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    accumulators.emplace_back(
        aggregate.function.get(), aggregate.intermediateType);
  }
  AggregatingStateDescriptor descriptor(
      "window-agg-accumulators",
      std::move(accumulators),
      [this](char** rows, size_t numRows) { initializeRows(rows, numRows); },
      [this](char* row, const std::vector<VectorPtr>& columns) {
        mergeRow(row, columns);
      },
      memoryPool());
  stateHandle_ =
      keyedBackend_->getOrCreateAggregatingState<SliceEndNs>(descriptor);
  state_ =
      dynamic_cast<HeapAggregatingState<RowContainerStateKey, SliceEndNs>*>(
          stateHandle_.get());
  VELOX_CHECK_NOT_NULL(
      state_,
      "the heap keyed state backend must produce a HeapAggregatingState");

  auto* valueRows = state_->valueRows();
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto& function = aggregates_[i].function;
    function->setAllocator(&valueRows->stringAllocator());
    const auto rowColumn = valueRows->columnAt(static_cast<int32_t>(i));
    function->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rowColumn.initializedByte(),
        rowColumn.initializedMask(),
        valueRows->rowSizeOffset());
    if (static_cast<int32_t>(i) == countStarIndex_) {
      countStarOffset_ = countStarColumnOffset(
          aggregates_[i].intermediateType->kind(), rowColumn.offset());
    }
  }

  StatefulOperator::initializeStateBackend(stateBackend);

  // Event-time window timers run through the framework's
  // watermark advances (Flink InternalTimerServiceImpl). The scheduler
  timerService_ =
      std::make_unique<InternalTimerService<WindowTimerKey, SliceEndNs>>(
          static_cast<Triggerable<WindowTimerKey, SliceEndNs>*>(this),
          std::make_unique<NoopProcessingTimeScheduler>());

  // Processing-time windows fire on the wall clock: the wake-up timer
  if (!node_->isEventTime()) {
    procTimeScheduler_ = std::make_unique<SystemProcessingTimeScheduler>();
  }
}

void WindowAggregator::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  checkInputRowKinds(*record);
  VELOX_CHECK(!input_, "Last input has not been processed");
  input_ = record->record();
}

void WindowAggregator::advance() {
  if (!input_) {
    if (keyedBackend_) {
      keyedBackend_->advance();
    }
    return;
  }

  const auto numRows = input_->size();
  if (numRows == 0) {
    input_.reset();
    keyedBackend_->advance();
    return;
  }

  // Group rows by sliceEnd using the batch-level SliceAssigner.
  auto sliceEndToData = sliceAssigner_->assignSliceEnd(input_);
  for (const auto& [sliceEnd, sliceData] : sliceEndToData) {
    // Rows accumulate into their own slice's accumulator (Flink
    SliceEndNs ns(sliceEnd);

    // Late check (event-time, two-phase per Flink
    if (node_->isEventTime() &&
        WindowTimezoneUtil::isWindowFired(sliceEnd, currentProgress_, tz_)) {
      int64_t lastWindowEnd = sliceAssigner_->getLastWindowEnd(sliceEnd);
      if (WindowTimezoneUtil::isWindowFired(
              lastWindowEnd, currentProgress_, tz_)) {
        lateDroppedRows_ += sliceData->size();
        continue; // All owning windows fired: drop.
      }
      // Partially fired: accumulate into the (unfired) own slice below
    }

    // Probe keys for this slice's data.
    keySelector_->probe(sliceData);
    const auto distinctKeys = keySelector_->distinctKeys();
    const auto sliceNumDistinct =
        static_cast<vector_size_t>(distinctKeys.size());
    const auto sliceNumRows = sliceData->size();

    // Batch state rows for the distinct keys under this namespace.
    stateRows_.resize(sliceNumDistinct);
    state_->rows(
        folly::Range<const RowContainerStateKey*>(
            distinctKeys.data(), static_cast<size_t>(sliceNumDistinct)),
        ns,
        stateRows_.data());
    // Mirror the live slice rows into the operator-owned cache (the
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      sliceRowCache_[{distinctKeys[d].row(), sliceEnd}] = stateRows_[d];
    }

    // Accumulate raw input (same pattern as GroupAggregator::accumulateInput).
    const SelectivityVector allRows(sliceNumRows);
    std::vector<std::optional<column_index_t>> maskChannels;
    maskChannels.reserve(aggregates_.size());
    for (const auto& aggregate : aggregates_) {
      maskChannels.push_back(aggregate.mask);
    }
    exec::AggregationMasks masks(std::move(maskChannels));
    masks.addInput(sliceData, allRows);

    // Map each row to its distinct key index, then to its state row.
    rowToDistinct_.clear();
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      rowToDistinct_[distinctKeys[d].row()] = d;
    }
    rowGroups_.resize(sliceNumRows);
    const auto perRowKeys = keySelector_->keys();
    for (vector_size_t row = 0; row < sliceNumRows; ++row) {
      rowGroups_[row] = stateRows_[rowToDistinct_[perRowKeys[row].row()]];
    }

    for (size_t i = 0; i < aggregates_.size(); ++i) {
      auto& aggregate = aggregates_[i];
      std::vector<VectorPtr> args;
      args.reserve(aggregate.inputs.size());
      for (size_t j = 0; j < aggregate.inputs.size(); ++j) {
        if (aggregate.constantInputs[j] != nullptr) {
          args.push_back(aggregate.constantInputs[j]);
        } else {
          args.push_back(sliceData->childAt(aggregate.inputs[j]));
        }
      }
      const auto* activeRows = masks.activeRows(i);
      aggregate.function->addRawInput(
          rowGroups_.data(),
          activeRows != nullptr ? *activeRows : allRows,
          args,
          false /* mayPushdown */);
    }

    // Register fire for the first unfired window owning this slice,
    // scanning forward from sliceEnd by the slice interval (Flink
    int64_t fireWindowEnd = sliceEnd;
    if (node_->isEventTime()) {
      const int64_t sliceSize = sliceAssigner_->getSliceEndInterval();
      while (WindowTimezoneUtil::isWindowFired(
          fireWindowEnd, currentProgress_, tz_)) {
        // Clamp before overflow: isWindowFired is false at INT64_MAX.
        if (fireWindowEnd > std::numeric_limits<int64_t>::max() - sliceSize) {
          fireWindowEnd = std::numeric_limits<int64_t>::max();
          break;
        }
        fireWindowEnd += sliceSize;
      }
    }
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      registerFire(
          distinctKeys[d],
          stateRows_[d],
          sliceAssigner_->getWindowStart(fireWindowEnd),
          fireWindowEnd);
    }
  }

  input_.reset();
  keyedBackend_->advance();
  // Processing-time windows fire opportunistically after each batch (the
  if (!node_->isEventTime() && procTimeScheduler_) {
    fireDue(WindowTimezoneUtil::getCurrentProcessingTime());
    armProcTimeWakeUp();
  }
}

void WindowAggregator::processWatermark(int64_t timestamp) {
  // Event-time windows advance on watermarks only. A processing-time
  // the window twice). Flink advances the processor on watermarks
  if (node_->isEventTime() && timestamp > currentProgress_) {
    // Dispatches every due event-time timer to onEventTime (fire
    // Flink's InternalTimerServiceImpl.advanceWatermark ordering).
    timerService_->advanceWatermark(timestamp);
    currentProgress_ = timestamp;
    flushPendingTimers(timestamp);
  }
  // Propagate watermark downstream (after fire, matching Flink order).
  StatefulOperator::processWatermark(timestamp);
}

void WindowAggregator::onEventTime(
    std::shared_ptr<TimerHeapInternalTimer<WindowTimerKey, SliceEndNs>> timer) {
  const auto& ns = timer->ns();
  const auto& timerKey = timer->key();
  // Rebuild the full state key from the snapshot (keyGroup is
  RowContainerStateKey key(
      timerKey.schema, timerKey.row, timerKey.hash, maxParallelism_);
  inEventTimeDispatch_ = true;
  fireWindow(key, windowStart(ns.sliceEnd()), ns.sliceEnd());
  inEventTimeDispatch_ = false;
  firedThisPass_.emplace(timerKey.row, ns.sliceEnd());
}

void WindowAggregator::onProcessingTime(int64_t timestamp) {
  if (timestamp > lastTriggeredProcessingTime_) {
    fireDue(timestamp);
    lastTriggeredProcessingTime_ = timestamp;
  }
  armProcTimeWakeUp();
}

void WindowAggregator::checkWatermarkStatus(int64_t now) {
  // Processing-time tick on the task thread: StatefulTask::next invokes
  if (!node_->isEventTime() && keyedBackend_) {
    fireDue(now);
    armProcTimeWakeUp();
  }
  StatefulOperator::checkWatermarkStatus(now);
}

void WindowAggregator::close() {
  shutdownProcTimeScheduler();
  if (timerService_) {
    // Flush trailing event-time windows (bounded input), then release.
    timerService_->advanceWatermark(std::numeric_limits<int64_t>::max());
    timerService_->close();
    timerService_.reset();
  }
  // Flush trailing proc-time windows (bounded input).
  fireDue(std::numeric_limits<int64_t>::max());
  StatefulOperator::close();
  input_.reset();
  state_ = nullptr;
  stateHandle_.reset();
  keyedBackend_.reset();
  keySelector_.reset();
  procFireIndex_.clear();
  registeredFires_.clear();
  fireRowCache_.clear();
  sliceRowCache_.clear();
  firedThisPass_.clear();
  pendingTimers_.clear();
  currentProgress_ = std::numeric_limits<int64_t>::min();
  lastTriggeredProcessingTime_ = 0;
  lateDroppedRows_ = 0;
}

void WindowAggregator::fireDue(int64_t progress) {
  // Processing-time fire index only: event-time windows dispatch
  while (!procFireIndex_.empty() && procFireIndex_.begin()->first <= progress) {
    auto it = procFireIndex_.begin();
    auto key = it->second.key;
    auto windowStart = it->second.windowStart;
    auto windowEnd = it->second.ns.sliceEnd();
    registeredFires_.erase({key.row(), windowEnd});
    procFireIndex_.erase(it);
    fireWindow(key, windowStart, windowEnd);
  }
  if (!node_->isEventTime() && procTimeScheduler_) {
    armProcTimeWakeUp();
  }
}

void WindowAggregator::fireWindow(
    const RowContainerStateKey& key,
    int64_t windowStart,
    int64_t windowEnd) {
  const auto fireWindowType = static_cast<WindowType>(node_->windowType());
  if (fireWindowType == WindowType::HOP) {
    fireHopWindow(key, windowEnd);
    return;
  }
  if (fireWindowType == WindowType::CUMULATIVE) {
    fireCumulateWindow(key, windowEnd);
    return;
  }
  // TUMBLE: the slice is the window. Read-only peek: a miss means the
  auto ns = SliceEndNs(windowEnd);
  // Operator-owned fire-row cache (see the header): a miss means the
  auto it = fireRowCache_.find({key.row(), windowEnd});
  if (it == fireRowCache_.end()) {
    return;
  }
  char* row = it->second;
  // Empty-window suppression (Flink WindowIsEmptySupplier): a window
  // whose count star reads 0 emits nothing. Cleanup still runs (Flink
  if (!countStarIsZero(row, countStarOffset_)) {
    emitWindowResult(key, windowStart, windowEnd, row);
  }
  state_->remove(key, ns);
  fireRowCache_.erase({key.row(), windowEnd});
}

void WindowAggregator::fireHopWindow(
    const RowContainerStateKey& key,
    int64_t windowEnd) {
  // Live-registration check: the fire-row cache erases the registration
  if (fireRowCache_.find({key.row(), windowEnd}) == fireRowCache_.end()) {
    return;
  }
  fireRowCache_.erase({key.row(), windowEnd});
  // Heap merge of the slices covered by [windowEnd - size, windowEnd):
  // firstSliceEnd .. windowEnd stepping by the slice interval (Flink
  const int64_t sliceSize = sliceAssigner_->getSliceEndInterval();
  const int64_t windowStartMs = windowStart(windowEnd);
  const int64_t firstSliceEnd = windowStartMs + sliceSize;

  // Read-only peeks through the operator-owned slice row cache: a
  // chain continuation (Flink empty-window semantics).
  char* firstRow = lookupSliceRow(key.row(), firstSliceEnd);
  bool hasData = firstRow != nullptr;
  for (int64_t e = firstSliceEnd + sliceSize; e <= windowEnd && !hasData;
       e += sliceSize) {
    hasData = lookupSliceRow(key.row(), e) != nullptr;
  }
  if (!hasData) {
    return;
  }

  // The merge target is the first slice; materialize its accumulator
  // when the window's data starts in a later slice (Flink mergeSlices
  if (firstRow == nullptr) {
    firstRow = state_->row(key, SliceEndNs(firstSliceEnd));
    sliceRowCache_[{key.row(), firstSliceEnd}] = firstRow;
  }

  // Merge the remaining covered slices into the first slice in ONE
  hopMergeScratchRows_.clear();
  for (int64_t e = firstSliceEnd + sliceSize; e <= windowEnd; e += sliceSize) {
    if (char* sourceRow = lookupSliceRow(key.row(), e)) {
      hopMergeScratchRows_.push_back(sourceRow);
    }
  }
  if (!hopMergeScratchRows_.empty()) {
    mergeAccumulatorRowsBatch(
        firstRow,
        hopMergeScratchRows_.data(),
        hopMergeScratchRows_.size(),
        aggregates_,
        hopMergeScratchColumns_,
        memoryPool());
  }

  // Empty-window suppression on the merged result (Flink checks the
  // the same way, so the observable stream matches Flink exactly (the
  // probe keeps the R1 key-liveness invariant; a count-gated chain
  if (!countStarIsZero(firstRow, countStarOffset_)) {
    emitWindowResult(key, windowStartMs, windowEnd, firstRow);
  }

  // ExpiredSlices: HOP cleans up only the first slice of the fired
  // window (Flink SliceAssigners#SharedSliceAssigner.expiredSlices does
  state_->remove(key, SliceEndNs(firstSliceEnd));
  sliceRowCache_.erase({key.row(), firstSliceEnd});

  // Chain continuation: the fired window is non-empty, so register the
  // next window at windowEnd + sliceSize (Flink nextTriggerWindow has
  const int64_t nextWindowEnd = windowEnd + sliceSize;
  const int64_t nextFirstSliceEnd = windowStart(nextWindowEnd) + sliceSize;
  bool nextHasState = false;
  for (int64_t e = nextFirstSliceEnd; e <= nextWindowEnd; e += sliceSize) {
    if (lookupSliceRow(key.row(), e) != nullptr) {
      nextHasState = true;
      break;
    }
  }
  if (nextHasState) {
    registerFire(key, nullptr, windowStart(nextWindowEnd), nextWindowEnd);
  }
}

void WindowAggregator::flushPendingTimers(int64_t watermark) {
  // Registrations deferred during a dispatch are applied outside the
  // deduplicate against it; skip it the way Flink's dispatch loop
  while (!pendingTimers_.empty()) {
    auto pending = std::move(pendingTimers_);
    pendingTimers_.clear();
    bool anyDue = false;
    for (auto& [timerKey, ns, due] : pending) {
      if (firedThisPass_.count({timerKey.row, ns.sliceEnd()}) > 0) {
        continue;
      }
      timerService_->registerEventTimeTimer(timerKey, ns, due);
      anyDue = anyDue || due <= watermark;
    }
    if (!anyDue) {
      break;
    }
    timerService_->advanceWatermark(watermark);
  }
  firedThisPass_.clear();
}

void WindowAggregator::fireCumulateWindow(
    const RowContainerStateKey& key,
    int64_t windowEnd) {
  // Each step fires the cumulative window [groupStart, windowEnd): the
  // and the result emits from there (Flink CumulativeSliceAssigner:
  const int64_t sliceSize = sliceAssigner_->getSliceEndInterval();
  const int64_t groupStart = sliceAssigner_->getWindowStart(windowEnd);
  const int64_t firstSliceEnd = groupStart + sliceSize;
  const int64_t lastSliceEnd = groupStart + node_->size();

  // The group accumulator lives in the first slice. A first-step miss
  char* firstRow = lookupSliceRow(key.row(), firstSliceEnd);
  if (firstRow == nullptr && windowEnd == firstSliceEnd) {
    return;
  }

  // MergeSlices: a non-first step folds its slice into the first slice
  // matching Flink where only slices with rows carry timers).
  if (windowEnd != firstSliceEnd) {
    if (char* sourceRow = lookupSliceRow(key.row(), windowEnd)) {
      if (firstRow == nullptr) {
        firstRow = state_->row(key, SliceEndNs(firstSliceEnd));
        sliceRowCache_[{key.row(), firstSliceEnd}] = firstRow;
      }
      mergeAccumulatorRows(firstRow, sourceRow);
    }
  }
  if (firstRow == nullptr) {
    return;
  }
  if (!countStarIsZero(firstRow, countStarOffset_)) {
    emitWindowResult(key, groupStart, windowEnd, firstRow);
  }

  // ExpiredSlices three-branch cleanup: the first step keeps the shared
  // clears both the step slice and the shared first slice (Flink
  if (windowEnd == lastSliceEnd) {
    state_->remove(key, SliceEndNs(windowEnd));
    sliceRowCache_.erase({key.row(), windowEnd});
    if (windowEnd != firstSliceEnd) {
      state_->remove(key, SliceEndNs(firstSliceEnd));
      sliceRowCache_.erase({key.row(), firstSliceEnd});
    }
  } else if (windowEnd != firstSliceEnd) {
    state_->remove(key, SliceEndNs(windowEnd));
  }

  // Chain to the next step unconditionally - empty steps do not stop
  // the chain (Flink nextTriggerWindow ignores isWindowEmpty for
  // R1 fire-entry liveness invariant; at the last step the chain stops
  const int64_t nextWindowEnd = windowEnd + sliceSize;
  if (nextWindowEnd <= lastSliceEnd) {
    registerFire(key, nullptr, groupStart, nextWindowEnd);
  }
}

void WindowAggregator::emitWindowResult(
    const RowContainerStateKey& key,
    int64_t windowStartMs,
    int64_t windowEndMs,
    char* row) {
  const auto& outputType = node_->outputType();
  const auto numOutputCols = static_cast<int32_t>(outputType->size());
  std::vector<VectorPtr> children(numOutputCols);

  auto* keyRows = keySelector_->keyRowContainer();

  // Key columns extracted from the key row (none for keyless - the
  const folly::Range<const vector_size_t*> oneRow(&kZeroIndex, 1);
  const char* keyRowPtr = key.row();
  for (int32_t k = 0; k < outputKeyCount_; ++k) {
    children[k] = BaseVector::create(outputType->childAt(k), 1, memoryPool());
    keyRows->extractColumn(&keyRowPtr, oneRow, k, 0, children[k]);
  }

  // Window columns as TIMESTAMP, at the projection positions the plan
  auto startVec =
      BaseVector::create(outputType->childAt(windowStartIdx_), 1, memoryPool());
  if (auto* tsVec = startVec->asFlatVector<Timestamp>()) {
    tsVec->set(0, Timestamp::fromMillis(windowStartMs));
  }
  children[windowStartIdx_] = startVec;

  auto endVec =
      BaseVector::create(outputType->childAt(windowEndIdx_), 1, memoryPool());
  if (auto* tsVec = endVec->asFlatVector<Timestamp>()) {
    tsVec->set(0, Timestamp::fromMillis(windowEndMs));
  }
  children[windowEndIdx_] = endVec;

  // Extract final aggregate values from the accumulator row.
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto result = BaseVector::create(
        outputType->childAt(numLeadingColumns_ + i), 1, memoryPool());
    aggregates_[i].function->extractValues(&row, 1, &result);
    children[numLeadingColumns_ + i] = result;
  }

  auto output = std::make_shared<RowVector>(
      memoryPool(), outputType, nullptr, 1, std::move(children));
  pushOutput(
      std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
}

int64_t WindowAggregator::windowStart(int64_t windowEnd) const {
  return windowEnd - node_->size();
}

void WindowAggregator::registerFire(
    const RowContainerStateKey& key,
    char* stateRow,
    int64_t windowStart,
    int64_t windowEnd) {
  // Due timestamp = window_end - 1 (matching Flink's maxTimestamp).
  // timestamp) - the queue deduplicates repeated registrations (Flink's
  fireRowCache_[{key.row(), windowEnd}] = stateRow;
  if (node_->isEventTime()) {
    // The operator-owned timer key: a snapshot of the fields the fire
    WindowTimerKey timerKey{
        key.schema(), key.row(), key.hash(), key.keyGroup()};
    SliceEndNs ns(windowEnd);
    const int64_t due =
        WindowTimezoneUtil::toEpochMillsForTimer(windowEnd - 1, tz_);
    if (inEventTimeDispatch_) {
      pendingTimers_.emplace_back(std::move(timerKey), ns, due);
      return;
    }
    timerService_->registerEventTimeTimer(timerKey, ns, due);
  }
  if (registeredFires_.insert({key.row(), windowEnd}).second) {
    procFireIndex_.emplace(
        WindowTimezoneUtil::toEpochMillsForTimer(windowEnd - 1, tz_),
        FireEntry{key, SliceEndNs(windowEnd), windowStart});
  }
}

void WindowAggregator::initializeRows(char** rows, size_t numRows) {
  std::vector<vector_size_t> indices(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    indices[i] = static_cast<vector_size_t>(i);
  }
  for (auto& aggregate : aggregates_) {
    aggregate.function->initializeNewGroups(
        rows,
        folly::Range<const vector_size_t*>(indices.data(), indices.size()));
  }
}

void WindowAggregator::mergeRow(
    char* row,
    const std::vector<VectorPtr>& columns) {
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    // Reused single-argument vector: the merge consumes the input
    singleIntermediateArg_[0] = columns[i];
    aggregates_[i].function->addSingleGroupIntermediateResults(
        row, SelectivityVector(1), singleIntermediateArg_, false);
  }
}

void WindowAggregator::mergeAccumulatorRows(char* targetRow, char* sourceRow) {
  // One-row intermediate extraction per aggregate, merged into the
  if (mergeScratchColumns_.size() != aggregates_.size()) {
    mergeScratchColumns_.assign(aggregates_.size(), nullptr);
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      mergeScratchColumns_[i] =
          BaseVector::create(aggregates_[i].intermediateType, 1, memoryPool());
    }
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i].function->extractAccumulators(
        &sourceRow, 1, &mergeScratchColumns_[i]);
  }
  mergeRow(targetRow, mergeScratchColumns_);
}

void WindowAggregator::checkInputRowKinds(const StreamRecord& record) const {
  if (record.appendOnly()) {
    return;
  }
  const auto& rowKind = *record.rowKind();
  // TODO: support retraction semantics (L2); every row must be INSERT until
  for (vector_size_t row = 0; row < rowKind.size(); ++row) {
    VELOX_CHECK_EQ(
        rowKind.valueAt(row),
        static_cast<int8_t>(RowKind::INSERT),
        "WindowAggregator received a non-INSERT row; retraction is not "
        "supported yet (L2)");
  }
}

void WindowAggregator::rebuildFireIndex() {
  // One timer per live (key, window) state entry; deduplicated by
  state_->forEachEntry([this](
                           const RowContainerStateKey& key,
                           const SliceEndNs& ns) {
    // Restored entries are live by definition: row() finds the
    char* row = state_->row(key, ns);
    sliceRowCache_[{key.row(), ns.sliceEnd()}] = row;
    registerFire(
        key, row, sliceAssigner_->getWindowStart(ns.sliceEnd()), ns.sliceEnd());
  });
  armProcTimeWakeUp();
}

void WindowAggregator::armProcTimeWakeUp() {
  if (!procTimeScheduler_ || procFireIndex_.empty()) {
    return;
  }
  // The wake-up timer is one-shot: SystemProcessingTimeScheduler::
  // until the next input batch, while Flink's processing-time timers
  if (procTimerName_.has_value() &&
      armedDueTs_ <= WindowTimezoneUtil::getCurrentProcessingTime()) {
    armedDueTs_ = std::numeric_limits<int64_t>::max();
    procTimerName_ = std::nullopt;
  }
  const int64_t nextDue = procFireIndex_.begin()->first;
  if (procTimerName_.has_value() && armedDueTs_ <= nextDue) {
    return; // Already armed at an earlier-or-equal due time.
  }
  if (procTimerName_.has_value()) {
    procTimeScheduler_->cancel(procTimerName_.value());
    procTimeScheduler_->unregister(procTimerName_.value());
    procTimerName_ = std::nullopt;
  }
  const auto registered = procTimeScheduler_->registerTimer(
      nextDue, ProcessingTimerTask(nextDue, [this](int64_t timestamp) {
        // Wake-up only: ask Java to drain the task (mailbox), which runs
        auto bridge = nativeCallbackBridge();
        if (bridge) {
          bridge->onProcessingTime(timestamp);
        }
      }));
  if (registered.has_value()) {
    procTimerName_ = registered;
    armedDueTs_ = nextDue;
  }
}

void WindowAggregator::shutdownProcTimeScheduler() {
  if (procTimeScheduler_) {
    if (procTimerName_.has_value()) {
      procTimeScheduler_->cancel(procTimerName_.value());
      procTimerName_ = std::nullopt;
    }
    procTimeScheduler_->close();
    procTimeScheduler_.reset();
  }
}

} // namespace facebook::velox::stateful
