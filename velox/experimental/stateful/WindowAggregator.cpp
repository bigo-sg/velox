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
#include "velox/experimental/stateful/WindowAggregatorHopMerge.h"

#include <cstdlib>

#include "velox/exec/AggregationMasks.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/agg/AggregateInfos.h"
#include "velox/exec/Aggregate.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

WindowAggregator::WindowAggregator(
    std::unique_ptr<exec::Operator> localAggregator,
    std::vector<StatefulOperatorPtr> targets,
    std::shared_ptr<const StreamWindowAggregationNode> node,
    exec::DriverCtx* driverCtx)
    : StatefulOperator(
          std::make_unique<EmptyOperator>(0, driverCtx, node),
          std::move(targets)),
      node_(std::move(node)),
      localAggregator_(std::move(localAggregator)) {}

void WindowAggregator::initialize() {
  if (localAggregator_) {
    localAggregator_->initialize();
  }
  VELOX_CHECK_NOT_NULL(
      node_->keyedStateSpec(),
      "StreamWindowAggregationNode must carry a KeyedStateSpec");
  const WindowType wt = static_cast<WindowType>(node_->windowType());
  // TODO(L5): CUMULATE is deferred (preserved on the
  // window-agg-cumulate-deferred branch); running it through the
  // aligned-window logic would silently produce wrong results.
  if (wt != WindowType::HOP && wt != WindowType::TUMBLE &&
      wt != WindowType::SESSION) {
    VELOX_UNSUPPORTED(
        "WindowAggregator does not support CUMULATE windows yet (L5)");
  }
  VELOX_CHECK_GT(
      node_->size(), 0, "window size must be positive, got {}", node_->size());
  VELOX_CHECK_LT(
      std::abs(node_->offset()),
      node_->size(),
      "window offset must be in (-size, size)");
  if (wt == WindowType::HOP) {
    VELOX_CHECK_GT(
        node_->step(),
        0,
        "HOP window slide must be positive, got {}",
        node_->step());
    VELOX_CHECK_EQ(
        node_->size() % node_->step(),
        0,
        "HOP window size must be a multiple of slide (size={}, slide={})",
        node_->size(),
        node_->step());
  }
  const auto* aggNode =
      std::dynamic_pointer_cast<const core::AggregationNode>(
          node_->aggregation())
          .get();
  VELOX_CHECK_NOT_NULL(
      aggNode, "StreamWindowAggregationNode must carry an AggregationNode");
  auto inputType = std::dynamic_pointer_cast<const RowType>(
      aggNode->sources()[0]->outputType());
  VELOX_CHECK_NOT_NULL(
      inputType, "AggregationNode input type must be a RowType");
  declaredInputType_ = inputType;
  // Keyless windows (empty grouping): gluten appends a constant BIGINT
  // "$keyless_key" column to the declared input type and keys on it (the
  // keyed state backend requires at least one key channel); runtime
  // batches are one column narrower and get the column wrapped on in
  // addInput.
  const auto& keyChannels = node_->keyedStateSpec()->keyChannels();
  // Position-agnostic: the $keyless_key column can sit at any channel
  // (appended last in the declared input by both the single-phase and
  // the two-stage shadows). Name + single key suffices.
  keyless_ = keyChannels.size() == 1 &&
      std::string(inputType->nameOf(keyChannels[0])) == "$keyless_key";
  numKeys_ = static_cast<int32_t>(keyChannels.size());
  // The keyless wrap adds the key column to the input type only - the
  // output projection carries no key column at all.
  outputKeyCount_ = keyless_ ? 0 : numKeys_;
  // Window columns carry windowStartIndex/windowEndIndex; -1 falls back
  // to the legacy layout right after the key columns.
  windowStartIdx_ =
      node_->windowStartIndex() >= 0 ? node_->windowStartIndex() : outputKeyCount_;
  windowEndIdx_ =
      node_->windowEndIndex() >= 0 ? node_->windowEndIndex() : outputKeyCount_ + 1;
  // The aggregate output columns sit where the plan's projection puts
  // them - either right after the keys (q12-style: key, aggs, window) or
  // after the window columns (wa-e1-style: key, window, aggs). The base
  // is the first output index that is neither a key column nor a window
  // column; the run must be contiguous and cover every remaining column.
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
  VELOX_CHECK_GE(
      numLeadingColumns_,
      0,
      "No output column left for aggregates in {}",
      outputType->toString());
  // Count star detection (Flink indexOfCountStar): the first
  // zero-argument UNFILTERED COUNT call (AggregateUtil requires
  // filterArg < 0 - a filtered COUNT(*) is not a count star, the
  // empty-window check stays disabled and zero-count windows emit).
  // Hopping windows require one - Flink checkArgument.
  countStarIndex_ = findCountStarIndex(
      aggNode->aggregates(), wt == WindowType::HOP);
  isPartialInput_ =
      aggNode->step() == core::AggregationNode::Step::kFinal;
  aggregates_ = makeAggregateInfos(
      aggNode->aggregates(),
      inputType,
      node_->outputType(),
      numLeadingColumns_,
      op()->operatorCtx()->driverCtx()->queryConfig(),
      memoryPool(),
      isPartialInput_ ? core::AggregationNode::Step::kFinal
                      : core::AggregationNode::Step::kSingle);
}

void WindowAggregator::initializeStateBackend(StateBackend* stateBackend) {
  tz_ = TimeWindowUtil::locateTimeZone(node_->shiftTimeZone());
  const auto& spec = *node_->keyedStateSpec();
  keyedBackend_ = createKeyedStateBackend<RowContainerStateKey>(
      *stateBackend, spec, memoryPool());
  keySelector_ = std::make_unique<KeySelector>(
      keyedBackend_->keyIndex(), spec.keyChannels());

  sliceAssigner_ = std::make_unique<SliceAssigner>(
      nullptr, /* keySelector unused for batch-level slice */
      node_->size(),
      node_->step(),
      node_->offset(),
      Window::getType(node_->windowType()),
      node_->rowtimeIndex(),
      tz_,
      /*attached=*/isPartialInput_);

  // Aggregate functions were created in initialize() (op() available from
  // the EmptyOperator set in the constructor).


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
  const bool isSession =
      static_cast<WindowType>(node_->windowType()) == WindowType::SESSION;
  if (isSession) {
    // SESSION: accumulators live under the pinned state-window
    // namespace; the persistent actual->state mapping is the
    // "session-window-mapping" MapState (Flink
    // MergingWindowProcessFunction#open, under a constant namespace -
    // the map is per key).
    state_.reset();
    sessionState_ =
        keyedBackend_->getOrCreateAggregatingState<TimeWindowNs>(descriptor);
    MapStateDescriptor<TimeWindowNs, TimeWindowNs> mappingDescriptor(
        "session-window-mapping",
        std::make_shared<TimeWindowNsSerializer>(),
        std::make_shared<TimeWindowNsSerializer>(),
        memoryPool());
    mappingState_ = keyedBackend_->getOrCreateMapState<SliceEndNs, TimeWindowNs, TimeWindowNs>(
        mappingDescriptor);
   } else {
    state_ =
        keyedBackend_->getOrCreateAggregatingState<SliceEndNs>(descriptor);
    sessionState_.reset();
    mappingState_.reset();
  }

  auto* valueRows = isSession ? sessionState_->valueRows() : state_->valueRows();
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

  // Processing-time windows fire on the wall clock: the wake-up timer
  // asks Java to drain the task at the next due time and the fire itself
  // runs on the task thread (checkWatermarkStatus / advance).
  if (!node_->isEventTime()) {
    procTimeScheduler_ = std::make_unique<SystemProcessingTimeScheduler>();
  }
}

void WindowAggregator::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  checkInputRowKinds(*record);
  VELOX_CHECK(!input_, "Last input has not been processed");
  input_ = keyless_
      ? wrapKeylessColumn(memoryPool(), declaredInputType_, record->record())
      : record->record();
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

  if (static_cast<WindowType>(node_->windowType()) == WindowType::SESSION) {
    // SESSION takes the unslice path: per-row windows merged on arrival
    // (Flink UnsliceWindowAggProcessor); the slice machinery below is
    // the aligned-window path only.
    advanceSession();
    input_.reset();
    keyedBackend_->advance();
    if (!node_->isEventTime() && procTimeScheduler_) {
      fireDue(TimeWindowUtil::getCurrentProcessingTime());
      armProcTimeWakeUp();
    }
    return;
  }

  if (isPartialInput_) {
    // The two-stage global side: the input is the local partials [keys,
    // intermediates, windowEnd]; the slice end comes from the
    // appended column (attached) and the intermediates merge into the
    // slice state. Kept in a separate function so the single-stage raw
    // accumulation path below keeps its codegen unchanged (the W1
    // lesson: code added to this TU perturbs the hot advance() loop).
    advancePartialInput();
    input_.reset();
    keyedBackend_->advance();
    if (!node_->isEventTime() && procTimeScheduler_) {
      fireDue(TimeWindowUtil::getCurrentProcessingTime());
      armProcTimeWakeUp();
    }
    return;
  }

  // Group rows by sliceEnd using the batch-level SliceAssigner.
  auto sliceEndToData = sliceAssigner_->assignSliceEnd(input_);
  for (const auto& [sliceEnd, sliceData] : sliceEndToData) {
    // Rows accumulate into their own slice's accumulator (Flink
    // AbstractSliceWindowAggProcessor#processElement buffers per
    // (key, sliceEnd)); a HOP window merges its covered slices on the
    // heap at fire time (fireHopWindow). Redirecting accumulation into
    // a "first slice" namespace is wrong for shared slices.
    SliceEndNs ns(sliceEnd);

    // Late check (event-time, two-phase per Flink
    // AbstractSliceWindowAggProcessor#processElement):
    // Phase 1: slice fired? Phase 2: all owning windows fired?
    // TUMBLE: lastWindowEnd == sliceEnd (single window) -> phase 1
    // implies drop. HOP: lastWindowEnd = sliceEnd - sliceSize + size.
    if (node_->isEventTime() &&
        TimeWindowUtil::isWindowFired(sliceEnd, currentProgress_, tz_)) {
      int64_t lastWindowEnd = sliceAssigner_->getLastWindowEnd(sliceEnd);
      if (TimeWindowUtil::isWindowFired(lastWindowEnd, currentProgress_, tz_)) {
        lateDroppedRows_ += sliceData->size();
        continue; // All owning windows fired: drop.
      }
      // Partially fired: accumulate into the (unfired) own slice below
      // and register the first unfired owning window for fire.
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
            distinctKeys.data(),
            static_cast<size_t>(sliceNumDistinct)),
        ns,
        stateRows_.data());

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
    // registers a timer at sliceEnd for the earliest owning window and
    // the late path scans to the first unfired one; for a fresh slice
    // the scan exits immediately).
    int64_t fireWindowEnd = sliceEnd;
    if (node_->isEventTime()) {
      const int64_t sliceSize = sliceAssigner_->getSliceEndInterval();
      while (TimeWindowUtil::isWindowFired(
          fireWindowEnd, currentProgress_, tz_)) {
        // Clamp before overflow: isWindowFired is false at INT64_MAX.
        if (fireWindowEnd >
            std::numeric_limits<int64_t>::max() - sliceSize) {
          fireWindowEnd = std::numeric_limits<int64_t>::max();
          break;
        }
        fireWindowEnd += sliceSize;
      }
    }
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      registerFire(
          distinctKeys[d],
          sliceAssigner_->getWindowStart(fireWindowEnd),
          fireWindowEnd);
    }
  }

  input_.reset();
  keyedBackend_->advance();
  // Processing-time windows fire opportunistically after each batch (the
  // wall clock only moves forward); idle periods are covered by the
  // wake-up timer + checkWatermarkStatus tick.
  if (!node_->isEventTime() && procTimeScheduler_) {
    fireDue(TimeWindowUtil::getCurrentProcessingTime());
    armProcTimeWakeUp();
  }
}

void WindowAggregator::processWatermark(int64_t timestamp) {
  if (timestamp > currentProgress_) {
    fireDue(timestamp);
    currentProgress_ = timestamp;
  }
  // Propagate watermark downstream (after fire, matching Flink order).
  StatefulOperator::processWatermark(timestamp);
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
  // checkWatermarkStatus(now) on idle drains. This is where proc-time
  // windows actually fire (the background wake-up timer only asks Java
  // to drain the task; the operator state is never touched off-thread).
  if (!node_->isEventTime() && keyedBackend_) {
    fireDue(now);
    armProcTimeWakeUp();
  }
  StatefulOperator::checkWatermarkStatus(now);
}

void WindowAggregator::close() {
  shutdownProcTimeScheduler();
  // Flush trailing windows (bounded input).
  fireDue(std::numeric_limits<int64_t>::max());
  StatefulOperator::close();
  if (localAggregator_) {
    localAggregator_->close();
  }
  input_.reset();
  state_.reset();
  sessionState_.reset();
  mappingState_.reset();
  keyedBackend_.reset();
  keySelector_.reset();
  fireIndex_.clear();
  registeredFires_.clear();
  currentProgress_ = std::numeric_limits<int64_t>::min();
  lastTriggeredProcessingTime_ = 0;
  lateDroppedRows_ = 0;
}

void WindowAggregator::fireDue(int64_t progress) {
  while (!fireIndex_.empty() && fireIndex_.begin()->first <= progress) {
    auto it = fireIndex_.begin();
    auto key = it->second.key;
    auto windowStart = it->second.windowStart;
    auto windowEnd = it->second.ns.sliceEnd();
    registeredFires_.erase({key.row(), windowEnd});
    fireIndex_.erase(it);
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
  const WindowType wt = static_cast<WindowType>(node_->windowType());
  if (wt == WindowType::HOP) {
    fireHopWindow(key, windowEnd);
    return;
  }
  if (wt == WindowType::SESSION) {
    fireSessionWindow(key, windowStart, windowEnd);
    return;
  }
  // TUMBLE: the slice is the window. Read-only peek: a miss means the
  // window never materialized or was already cleared; creating an empty
  // accumulator here would emit a bogus empty window row.
  auto ns = SliceEndNs(windowEnd);
  char* row = state_->get(key, ns);
  if (row == nullptr) {
    return;
  }
  // Empty-window suppression (Flink WindowIsEmptySupplier): a window
  // whose count star reads 0 emits nothing. Cleanup still runs (Flink
  // fireWindow suppresses collect, then clearWindow removes the state).
  if (!countStarIsZero(row, countStarOffset_)) {
    emitWindowResult(key, windowStart, windowEnd, row);
  }
  state_->remove(key, ns);
}

void WindowAggregator::fireHopWindow(
    const RowContainerStateKey& key,
    int64_t windowEnd) {
  // Heap merge of the slices covered by [windowEnd - size, windowEnd):
  // firstSliceEnd .. windowEnd stepping by the slice interval (Flink
  // SharedSliceAssigner#mergeSlices / HoppingSlicesIterable).
  const int64_t sliceSize = sliceAssigner_->getSliceEndInterval();
  const int64_t windowStartMs = windowStart(windowEnd);
  const int64_t firstSliceEnd = windowStartMs + sliceSize;

  // Read-only peeks: a window with no materialized slice is empty —
  // no emission, no chain continuation (Flink empty-window semantics).
  char* firstRow = state_->get(key, SliceEndNs(firstSliceEnd));
  bool hasData = firstRow != nullptr;
  for (int64_t e = firstSliceEnd + sliceSize;
       e <= windowEnd && !hasData;
       e += sliceSize) {
    hasData = state_->get(key, SliceEndNs(e)) != nullptr;
  }
  if (!hasData) {
    return;
  }

  // The merge target is the first slice; materialize its accumulator
  // when the window's data starts in a later slice (Flink mergeSlices
  // merges into the first slice of the window).
  if (firstRow == nullptr) {
    firstRow = state_->row(key, SliceEndNs(firstSliceEnd));
  }

  // Merge the remaining covered slices into the first slice in ONE
  // batched fold (ascending sliceEnd, the same order the row-by-row
  // merge consumed - see mergeAccumulatorRowsBatch). Source slices are
  // NOT removed: they are shared with later windows and are only
  // cleared when they become a later window's first slice.
  hopMergeScratchRows_.clear();
  for (int64_t e = firstSliceEnd + sliceSize; e <= windowEnd; e += sliceSize) {
    if (char* sourceRow = state_->get(key, SliceEndNs(e))) {
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
  // emptySupplier after mergeSlices, before collect). Emission only;
  // cleanup and the probe-based chain still run - the chain continues
  // only into windows whose state exists, whose fires are suppressed
  // the same way, so the observable stream matches Flink exactly (the
  // probe keeps the R1 key-liveness invariant; a count-gated chain
  // would register entries for keys with no state and dangle).
  if (!countStarIsZero(firstRow, countStarOffset_)) {
    emitWindowResult(key, windowStartMs, windowEnd, firstRow);
  }

  // ExpiredSlices: HOP cleans up only the first slice of the fired
  // window (Flink SliceAssigners#SharedSliceAssigner.expiredSlices does
  // not differentiate).
  state_->remove(key, SliceEndNs(firstSliceEnd));

  // Chain continuation: the fired window is non-empty, so register the
  // next window at windowEnd + sliceSize (Flink nextTriggerWindow has
  // no upper bound; an empty window stops the chain above). Deduplicated
  // with any registration the next window's own slices already made.
  // Register ONLY when the next window actually has state: an entry for
  // a key with no live state would dangle after the KeyTable reclaims
  // the key row at the next batch boundary (fire entries hold row
  // pointers that StateMap equality comparisons dereference).
  const int64_t nextWindowEnd = windowEnd + sliceSize;
  const int64_t nextFirstSliceEnd =
      windowStart(nextWindowEnd) + sliceSize;
  bool nextHasState = false;
  for (int64_t e = nextFirstSliceEnd; e <= nextWindowEnd; e += sliceSize) {
    if (state_->get(key, SliceEndNs(e)) != nullptr) {
      nextHasState = true;
      break;
    }
  }
  if (nextHasState) {
    registerFire(key, windowStart(nextWindowEnd), nextWindowEnd);
  }
}


void WindowAggregator::advancePartialInput() {
  // Mirrors the single-stage slicing loop: the only deltas are the
  // sliceEnd coming from the attached column (the assigner is in
  // attached mode, so assignSliceEnd takes the windowEnd column
  // values) and the accumulation merging intermediate columns via
  // addIntermediateResults instead of consuming raw input.
  auto sliceEndToData = sliceAssigner_->assignSliceEnd(input_);
  for (const auto& [sliceEnd, sliceData] : sliceEndToData) {
    SliceEndNs ns(sliceEnd);

    // Late check: same two-phase semantics as the single-stage path.
    if (node_->isEventTime() &&
        TimeWindowUtil::isWindowFired(sliceEnd, currentProgress_, tz_)) {
      int64_t lastWindowEnd = sliceAssigner_->getLastWindowEnd(sliceEnd);
      if (TimeWindowUtil::isWindowFired(lastWindowEnd, currentProgress_, tz_)) {
        lateDroppedRows_ += sliceData->size();
        continue;
      }
    }

    keySelector_->probe(sliceData);
    const auto distinctKeys = keySelector_->distinctKeys();
    const auto sliceNumDistinct =
        static_cast<vector_size_t>(distinctKeys.size());
    const auto sliceNumRows = sliceData->size();

    stateRows_.resize(sliceNumDistinct);
    state_->rows(
        folly::Range<const RowContainerStateKey*>(
            distinctKeys.data(),
            static_cast<size_t>(sliceNumDistinct)),
        ns,
        stateRows_.data());

    rowToDistinct_.clear();
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      rowToDistinct_[distinctKeys[d].row()] = d;
    }
    rowGroups_.resize(sliceNumRows);
    const auto perRowKeys = keySelector_->keys();
    for (vector_size_t row = 0; row < sliceNumRows; ++row) {
      rowGroups_[row] = stateRows_[rowToDistinct_[perRowKeys[row].row()]];
    }

    const SelectivityVector allRows(sliceNumRows);
    std::vector<std::optional<column_index_t>> maskChannels;
    maskChannels.reserve(aggregates_.size());
    for (const auto& aggregate : aggregates_) {
      maskChannels.push_back(aggregate.mask);
    }
    exec::AggregationMasks masks(std::move(maskChannels));
    masks.addInput(sliceData, allRows);
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      auto& aggregate = aggregates_[i];
      const auto* activeRows = masks.activeRows(i);
      aggregate.function->addIntermediateResults(
          rowGroups_.data(),
          activeRows != nullptr ? *activeRows : allRows,
          std::vector<VectorPtr>{sliceData->childAt(aggregate.inputs[0])},
          false /* mayPushdown */);
    }

    int64_t fireWindowEnd = sliceEnd;
    if (node_->isEventTime()) {
      const int64_t sliceSize = sliceAssigner_->getSliceEndInterval();
      while (TimeWindowUtil::isWindowFired(
          fireWindowEnd, currentProgress_, tz_)) {
        if (fireWindowEnd >
            std::numeric_limits<int64_t>::max() - sliceSize) {
          fireWindowEnd = std::numeric_limits<int64_t>::max();
          break;
        }
        fireWindowEnd += sliceSize;
      }
    }
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      registerFire(
          distinctKeys[d],
          sliceAssigner_->getWindowStart(fireWindowEnd),
          fireWindowEnd);
    }
  }
}

namespace {

// Flink TimeWindow#intersects / cover (SessionWindowAssigner#mergeWindow).
// NOTE: Flink's intersects is inclusive on both bounds - sessions split
// only when the row interval EXCEEDS the gap (an interval exactly equal
// to the gap still merges; native-verified on the session smoke).
bool sessionWindowsIntersect(const TimeWindowNs& a, const TimeWindowNs& b) {
  return a.start() <= b.end() && a.end() >= b.start();
}

TimeWindowNs sessionWindowCover(const TimeWindowNs& a, const TimeWindowNs& b) {
  return TimeWindowNs(
      std::min(a.start(), b.start()), std::max(a.end(), b.end()));
}

// The session-window-mapping lives under a constant namespace: the map
// is per key (Flink registers it under VoidNamespace).
const SliceEndNs kMappingNs{0};

} // namespace

void WindowAggregator::advanceSession() {
  // Flink UnsliceWindowAggProcessor#processElement over the
  // MergingWindowSet: each row opens [ts, ts+gap); intersecting
  // in-flight windows merge into the cover, the accumulator stays
  // pinned to the surviving state window ("avoid costly state
  // juggling", MergingWindowSet.java:64-68) with the others folded in
  // and dropped; the row accumulates into that state window and the
  // fire registration re-hangs to the merged window end.
  //
  // Batch-level amortizations, all semantics preserving (each argued):
  // - The in-flight set is keyed by start with a mutable (end, state
  //   window) value: the disjointness invariant (prev.end < tail.start)
  //   means an in-order row can only intersect the TAIL window, and a
  //   tail extension merely rewrites the end value - no tree surgery.
  // - Mapping-state and fire-index mutations are deferred to the end of
  //   each key's rows and applied as the snapshot-vs-final DIFF (the
  //   diff is the net change by construction): nothing reads the
  //   mapping or fires between advanceSession's entry and exit (fires
  //   run in fireDue from processWatermark/checkWatermarkStatus, and
  //   the mapping is only read at the fire, at restore, or when the
  //   next batch builds the in-flight set from it - all outside this
  //   call), so per-row remove/put and unregister/register collapse to
  //   the per-key net effect.
  // - The state row for the current window is cached per key: a row
  //   only ever accumulates into a live survivor state window, and the
  //   cache is dropped whenever a merge removes a state window.
  // - The late check is monotone within a batch: the window end only
  //   grows, the timer conversion is non-decreasing (the DST gap floors
  //   to the hour and the ambiguity resolves to a fixed offset), and
  //   the progress is fixed within the batch - so once a window end
  //   passes the check, an unchanged end passes again.
  const int64_t gap = node_->size();
  const auto numRows = input_->size();

  // Per-row UTC timestamps; a proc-time batch shares the batch-arrival
  // instant (the aligned-window convention).
  std::vector<int64_t> rowTs(numRows);
  if (node_->isEventTime()) {
    const auto& rowtimeVector = input_->childAt(node_->rowtimeIndex());
    const auto* tsSimple = rowtimeVector->as<SimpleVector<Timestamp>>();
    const auto* tsSimpleInt = rowtimeVector->as<SimpleVector<int64_t>>();
    VELOX_CHECK(
        tsSimple != nullptr || tsSimpleInt != nullptr,
        "rowtime column must be TIMESTAMP/Int64 simple vector");
    for (vector_size_t row = 0; row < numRows; ++row) {
      VELOX_CHECK(
          !(tsSimple ? tsSimple->isNullAt(row) : tsSimpleInt->isNullAt(row)),
          "RowTime field should not be null");
      const int64_t ms = tsSimple ? tsSimple->valueAt(row).toMillis()
                                  : tsSimpleInt->valueAt(row);
      rowTs[row] = TimeWindowUtil::toUtcTimestampMills(ms, tz_);
    }
  } else {
    const int64_t now = TimeWindowUtil::toUtcTimestampMills(
        TimeWindowUtil::getCurrentProcessingTime(), tz_);
    std::fill(rowTs.begin(), rowTs.end(), now);
  }

  // One batch-level key probe, then per-key in-order window merging.
  keySelector_->probe(input_);
  const auto distinctKeys = keySelector_->distinctKeys();
  const auto perRowKeys = keySelector_->keys();
  rowToDistinct_.clear();
  for (vector_size_t d = 0; d < static_cast<vector_size_t>(distinctKeys.size());
       ++d) {
    rowToDistinct_[distinctKeys[d].row()] = d;
  }
  std::vector<std::vector<vector_size_t>> rowsOfKey(distinctKeys.size());
  for (vector_size_t row = 0; row < numRows; ++row) {
    rowsOfKey[rowToDistinct_[perRowKeys[row].row()]].push_back(row);
  }

  rowGroups_.resize(numRows);
  SelectivityVector keptRows(numRows, true);
  bool anyDropped = false;

  for (vector_size_t d = 0; d < static_cast<vector_size_t>(distinctKeys.size());
       ++d) {
    const auto& key = distinctKeys[d];
    // The in-flight window set for this key, keyed by start (unique
    // under the disjointness invariant); the value carries the mutable
    // end and the pinned state window. Rebuilt from the persistent
    // mapping once per key per batch (the truth source; Flink keeps an
    // LRU cache per record - same content, different cache
    // granularity).
    // The batch-start snapshot of the persistent mapping (actual ->
    // state window) doubles as the in-flight set: the row loop below
    // mutates only this in-memory view, and the per-key NET mapping and
    // fire mutations applied afterwards are the snapshot-vs-final
    // diff - the diff IS the net change by construction.
    std::map<int64_t, std::pair<int64_t, TimeWindowNs>> inFlight;
    std::set<TimeWindowNs> mappingSnapshot;
    for (const auto& entry : mappingState_->entries(key, kMappingNs)) {
      inFlight.emplace(
          entry.first.start(), std::make_pair(entry.first.end(), entry.second));
      mappingSnapshot.insert(entry.first);
    }

    // Batch-local state row cache: a row only accumulates into a live
    // survivor state window; the cache drops whenever a merge removes
    // a state window.
    bool rowCached = false;
    TimeWindowNs cachedStateWin{};
    char* cachedRow = nullptr;

    for (vector_size_t row : rowsOfKey[d]) {
      const TimeWindowNs newWindow(rowTs[row], rowTs[row] + gap);

      // Tail fast path: an in-order row touches only the last window
      // (disjointness: every earlier window ends strictly before the
      // tail starts). Swallowed rows change nothing at all (Flink's
      // addWindow "same as a pre-existing window" skip); extensions
      // rewrite the mutable end and defer the mapping/fire churn.
      if (!inFlight.empty()) {
        auto tailIt = std::prev(inFlight.end());
        const int64_t tailStart = tailIt->first;
        const int64_t tailEnd = tailIt->second.first;
        const TimeWindowNs tailStateWin = tailIt->second.second;
        if (tailStart <= newWindow.start() && newWindow.start() <= tailEnd) {
          // No late check is needed on this path: the merged end is
          // max(tailEnd, newWindow.end) >= tailEnd, the timer conversion
          // is non-decreasing in the end (DST gap floors to the hour,
          // ambiguity resolves to a fixed offset), and the tail is
          // unfired (in-flight windows are, by induction: batch start
          // brings only unfired windows - fired ones are retired from
          // the mapping at fire time - and every extension keeps the
          // end >= an unfired end). So the merged window cannot be
          // late - the same argument covers the general merge below.
          const int64_t extendedEnd = std::max(tailEnd, newWindow.end());
          if (extendedEnd != tailEnd) {
            tailIt->second.first = extendedEnd;
          }
          if (!rowCached || !(cachedStateWin == tailStateWin)) {
            cachedRow = sessionState_->row(key, tailStateWin);
            cachedStateWin = tailStateWin;
            rowCached = true;
          }
          rowGroups_[row] = cachedRow;
          continue;
        }
      }

      // General path: out-of-order rows and fresh windows, probed by
      // ceiling and floor like SessionWindowAssigner#mergeWindows (the
      // floor check runs against the running cover).
      auto ceil = inFlight.lower_bound(newWindow.start());
      auto floor =
          (ceil == inFlight.begin()) ? inFlight.end() : std::prev(ceil);
      TimeWindowNs result = newWindow;
      std::vector<std::pair<int64_t, std::pair<int64_t, TimeWindowNs>>> merged;
      if (ceil != inFlight.end()) {
        const TimeWindowNs ceilWindow(ceil->first, ceil->second.first);
        if (sessionWindowsIntersect(ceilWindow, result)) {
          merged.push_back(*ceil);
          result = sessionWindowCover(result, ceilWindow);
        }
      }
      if (floor != inFlight.end()) {
        const TimeWindowNs floorWindow(floor->first, floor->second.first);
        if (sessionWindowsIntersect(floorWindow, result)) {
          merged.push_back(*floor);
          result = sessionWindowCover(result, floorWindow);
        }
      }

      if (merged.empty()) {
        // A self-contained window that is already late retires without
        // touching the mapping or the fire index (Flink retires from
        // the cache after addWindow; the row drops before any state or
        // timer write).
        if (node_->isEventTime() && TimeWindowUtil::isWindowFired(
                                         newWindow.end(), currentProgress_, tz_)) {
          lateDroppedRows_++;
          keptRows.setValid(row, false);
          anyDropped = true;
          continue;
        }
        inFlight.emplace(
            newWindow.start(),
            std::make_pair(newWindow.end(), newWindow));
        cachedRow = sessionState_->row(key, newWindow);
        cachedStateWin = newWindow;
        rowCached = true;
        rowGroups_[row] = cachedRow;
        continue;
      }

      // MergingWindowSet#addWindow: the surviving state namespace is
      // any merged window's state window (Flink picks the first of a
      // hash-set iteration; the choice is immaterial).
      const TimeWindowNs stateWin = merged[0].second.second;
      for (const auto& m : merged) {
        auto mIt = inFlight.find(m.first);
        const TimeWindowNs mState = mIt->second.second;
        inFlight.erase(mIt);
        if (!(mState == stateWin)) {
          // Fold the merged-away state window's accumulator into the
          // survivor and drop its entry (Flink
          // DefaultAccMergingConsumer).
          char* survivor = sessionState_->row(key, stateWin);
          if (char* source = sessionState_->get(key, mState)) {
            mergeAccumulatorRows(survivor, source);
          }
          sessionState_->remove(key, mState);
          rowCached = false;
        }
      }
      inFlight.insert_or_assign(
          result.start(), std::make_pair(result.end(), stateWin));

      // No late check here either: the cover end dominates every
      // merged (in-flight, unfired) end, so it is unfired - see the
      // tail fast path comment for the monotonicity argument.
      if (!rowCached || !(cachedStateWin == stateWin)) {
        cachedRow = sessionState_->row(key, stateWin);
        cachedStateWin = stateWin;
        rowCached = true;
      }
      rowGroups_[row] = cachedRow;
    }

    // Apply the per-key NET change as the snapshot-vs-final diff: the
    // persistent mapping and the fire index are only consumed outside
    // advanceSession (fireDue from processWatermark/checkWatermarkStatus,
    // restore, and the next batch's snapshot build), so intermediate
    // states are unobservable. Every batch-start actual carries exactly
    // one fire registration at its end (an invariant maintained by this
    // same diff), and in-flight window ends are unique under
    // disjointness, so the fire diff is well defined by end.
    std::map<TimeWindowNs, TimeWindowNs> finalMapping;
    for (const auto& [start, value] : inFlight) {
      finalMapping[TimeWindowNs(start, value.first)] = value.second;
    }
    for (const auto& actual : mappingSnapshot) {
      if (finalMapping.count(actual) == 0) {
        mappingState_->remove(key, kMappingNs, actual);
        unregisterFire(key, actual.end());
      }
    }
    for (const auto& [actual, stateWin] : finalMapping) {
      if (mappingSnapshot.count(actual) == 0) {
        mappingState_->put(key, kMappingNs, actual, stateWin);
        registerFire(key, actual.start(), actual.end());
      }
    }
  }

  // One vectorized accumulation pass over the kept rows (late rows
  // never reached a state window).
  const SelectivityVector allRows(numRows);
  std::vector<std::optional<column_index_t>> maskChannels;
  maskChannels.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    maskChannels.push_back(aggregate.mask);
  }
  exec::AggregationMasks masks(std::move(maskChannels));
  masks.addInput(input_, allRows);
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto& aggregate = aggregates_[i];
    std::vector<VectorPtr> args;
    args.reserve(aggregate.inputs.size());
    for (size_t j = 0; j < aggregate.inputs.size(); ++j) {
      if (aggregate.constantInputs[j] != nullptr) {
        args.push_back(aggregate.constantInputs[j]);
      } else {
        args.push_back(input_->childAt(aggregate.inputs[j]));
      }
    }
    const auto* activeRows = masks.activeRows(i);
    if (!anyDropped) {
      aggregate.function->addRawInput(
          rowGroups_.data(),
          activeRows != nullptr ? *activeRows : allRows,
          args,
          false /* mayPushdown */);
    } else {
      SelectivityVector combined(numRows, false);
      for (vector_size_t row = 0; row < numRows; ++row) {
        combined.setValid(
            row,
            keptRows.isValid(row) &&
                (activeRows == nullptr || activeRows->isValid(row)));
      }
      aggregate.function->addRawInput(
          rowGroups_.data(), combined, args, false /* mayPushdown */);
    }
  }
}

void WindowAggregator::fireSessionWindow(
    const RowContainerStateKey& key,
    int64_t windowStart,
    int64_t windowEnd) {
  // Flink prepareAggregateAccumulatorForEmit: resolve the actual
  // window's state window through the mapping. A miss is a stale
  // registration (a merged-away or retired window) - fire nothing and
  // clean nothing.
  const TimeWindowNs actual(windowStart, windowEnd);
  const auto stateWin = mappingState_->get(key, kMappingNs, actual);
  if (!stateWin.has_value()) {
    return;
  }
  char* row = sessionState_->get(key, *stateWin);
  if (row == nullptr) {
    return;
  }
  if (!countStarIsZero(row, countStarOffset_)) {
    emitWindowResult(key, windowStart, windowEnd, row);
  }
  // cleanWindowIfNeeded: clear the state window's accumulator and
  // retire the mapping entry - the window becomes invisible to later
  // rows (late rows open new windows or drop).
  sessionState_->remove(key, *stateWin);
  mappingState_->remove(key, kMappingNs, actual);
}

void WindowAggregator::mergeAccumulatorRows(char* targetRow, char* sourceRow) {
  // One-row intermediate extraction per aggregate, merged into the
  // target accumulator - the same path restore uses (mergeRow). The
  // intermediate columns live in a member scratch and are reused
  // across calls: the HOP fire heap-merges (size/slide - 1) slice
  // accumulators per window and the SESSION merge folds merged-away
  // state windows, and a ladder measurement attributes ~half of the
  // HOP wall time to these merges - dominated by the per-merge vector
  // allocation. Reuse is observation-free: extractAccumulators
  // overwrites row 0, the merge consumes the columns synchronously
  // before the next call, the vector slot is passed by reference (a
  // representation swap updates the scratch in place), and the
  // operator runs on a single task thread.
  if (mergeScratchColumns_.size() != aggregates_.size()) {
    mergeScratchColumns_.assign(aggregates_.size(), nullptr);
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      mergeScratchColumns_[i] = BaseVector::create(
          aggregates_[i].intermediateType, 1, memoryPool());
    }
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i].function->extractAccumulators(
        &sourceRow, 1, &mergeScratchColumns_[i]);
  }
  mergeRow(targetRow, mergeScratchColumns_);
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
  // output projection carries no key column).
  const folly::Range<const vector_size_t*> oneRow(
      &kZeroIndex, 1);
  const char* keyRowPtr = key.row();
  for (int32_t k = 0; k < outputKeyCount_; ++k) {
    children[k] = BaseVector::create(outputType->childAt(k), 1, memoryPool());
    keyRows->extractColumn(
        &keyRowPtr, oneRow, k, 0, children[k]);
  }

  // Window columns as TIMESTAMP, at the projection positions the plan
  // carries (windowStartIdx_/windowEndIdx_, resolved at initialize from
  // windowStartIndex/windowEndIndex).
  auto startVec =
      BaseVector::create(outputType->childAt(windowStartIdx_), 1, memoryPool());
  if (auto* tsVec = startVec->asFlatVector<Timestamp>()) {
    tsVec->set(0, Timestamp::fromMillis(windowStartMs));
  }
  children[windowStartIdx_] = startVec;

  auto endVec = BaseVector::create(outputType->childAt(windowEndIdx_), 1, memoryPool());
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
  pushOutput(std::make_shared<StreamRecord>(
      getPlanNodeId(), std::move(output)));
}

int64_t WindowAggregator::windowStart(int64_t windowEnd) const {
  return windowEnd - node_->size();
}

void WindowAggregator::registerFire(
    const RowContainerStateKey& key,
    int64_t windowStart,
    int64_t windowEnd) {
  // Due timestamp = window_end - 1 (matching Flink's maxTimestamp).
  // Deduplicated per (key row, windowEnd): Flink's timer service
  // registers one timer per (key, namespace), and a duplicate fire
  // entry would re-emit an already-cleared window (the second fire
  // would read a freshly created empty accumulator).
  if (registeredFires_.insert({key.row(), windowEnd}).second) {
    fireIndex_.emplace(
        TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, tz_),
        FireEntry{key, SliceEndNs(windowEnd), windowStart});
  }
}

void WindowAggregator::unregisterFire(
    const RowContainerStateKey& key,
    int64_t windowEnd) {
  // The SESSION merge re-hang: Flink clears the merged-away windows'
  // event-time triggers in the merge callback
  // (MergingWindowProcessFunction.java:178-182) so an extending merge
  // never fires at an interior end.
  if (registeredFires_.erase({key.row(), windowEnd}) > 0) {
    const int64_t dueTs =
        TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, tz_);
    auto range = fireIndex_.equal_range(dueTs);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second.key.row() == key.row() &&
          it->second.ns.sliceEnd() == windowEnd) {
        fireIndex_.erase(it);
        break;
      }
    }
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
    // synchronously, and a fresh {columns[i]} per aggregate per merge
    // is a hidden allocation on the HOP fire path.
    singleIntermediateArg_[0] = columns[i];
    aggregates_[i].function->addSingleGroupIntermediateResults(
        row, SelectivityVector(1), singleIntermediateArg_, false);
  }
}

void WindowAggregator::checkInputRowKinds(const StreamRecord& record) const {
  if (record.appendOnly()) {
    return;
  }
  const auto& rowKind = *record.rowKind();
  // TODO: support retraction semantics (L2); every row must be INSERT until then.
  for (vector_size_t row = 0; row < rowKind.size(); ++row) {
    VELOX_CHECK_EQ(
        rowKind.valueAt(row),
        static_cast<int8_t>(RowKind::INSERT),
        "WindowAggregator received a non-INSERT row; retraction is not "
        "supported yet (L2)");
  }
}

void WindowAggregator::rebuildFireIndex() {
  // One fire entry per live (key, window) state entry; deduplicated by
  // registerFire against anything the current session already armed.
  // currentProgress_ restarts at Long.MIN (progress is not persisted,
  // L3): the first watermark drives everything whose due time it passes,
  // which is the "fire immediately what is due" behavior.
  if (sessionState_) {
    // SESSION: fire entries carry the ACTUAL windows (the mapping's
    // keys), not the state windows; rebuild per key from the mapping.
    std::map<const char*, RowContainerStateKey> keys;
    sessionState_->forEachEntry(
        [&keys](const RowContainerStateKey& key, const TimeWindowNs& ns) {
          keys.emplace(key.row(), key);
        });
    for (const auto& [rowPtr, key] : keys) {
      for (const auto& entry : mappingState_->entries(key, kMappingNs)) {
        registerFire(key, entry.first.start(), entry.first.end());
      }
    }
  } else {
    state_->forEachEntry([this](
                             const RowContainerStateKey& key,
                             const SliceEndNs& ns) {
      registerFire(
          key, sliceAssigner_->getWindowStart(ns.sliceEnd()), ns.sliceEnd());
    });
  }
  armProcTimeWakeUp();
}

void WindowAggregator::armProcTimeWakeUp() {
  if (!procTimeScheduler_ || fireIndex_.empty()) {
    return;
  }
  const int64_t nextDue = fireIndex_.begin()->first;
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
        // task.advance() -> checkWatermarkStatus on the task thread where
        // the actual fire happens. The operator state is never touched
        // from this background thread (WatermarkAssigner idle-timer
        // pattern).
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
