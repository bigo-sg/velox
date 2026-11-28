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

// The session-window-mapping lives under a constant namespace: the map
// is per key (Flink registers it under VoidNamespace).
const SliceEndNs kMappingNs{0};

// Two session windows intersect when their intervals overlap, touching
// endpoints included - Flink merges on overlap OR exact adjacency;
bool sessionWindowsIntersect(const TimeWindowNs& a, const TimeWindowNs& b) {
  return a.start() <= b.end() && a.end() >= b.start();
}

TimeWindowNs sessionWindowCover(const TimeWindowNs& a, const TimeWindowNs& b) {
  return TimeWindowNs(
      std::min(a.start(), b.start()), std::max(a.end(), b.end()));
}

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
  // HOP / CUMULATE and the two-stage local-global split are
  if (wt != WindowType::TUMBLE && wt != WindowType::SESSION) {
    VELOX_UNSUPPORTED(
        "WindowAggregator supports TUMBLE / SESSION windows in this PR; "
        "HOP / CUMULATE and the two-stage split are planned "
        "follow-ups");
  }
  if (wt == WindowType::SESSION) {
    VELOX_CHECK_GT(
        node_->size(),
        0,
        "SESSION window gap must be positive, got {}",
        node_->size());
  }
  VELOX_CHECK_NOT_NULL(
      node_->keyedStateSpec(),
      "StreamWindowAggregationNode must carry a KeyedStateSpec");
  VELOX_CHECK_GT(
      node_->size(), 0, "window size must be positive, got {}", node_->size());
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
  countStarIndex_ = findCountStarIndex(aggNode->aggregates(), false);
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
  const auto windowType = static_cast<WindowType>(node_->windowType());
  const bool isSession = windowType == WindowType::SESSION;
  if (isSession) {
    sessionStateHandle_ =
        keyedBackend_->getOrCreateAggregatingState<TimeWindowNs>(descriptor);
    sessionState_ =
        dynamic_cast<HeapAggregatingState<RowContainerStateKey, TimeWindowNs>*>(
            sessionStateHandle_.get());
    VELOX_CHECK_NOT_NULL(
        sessionState_,
        "the heap keyed state backend must produce a HeapAggregatingState");
    MapStateDescriptor<TimeWindowNs, TimeWindowNs> mappingDescriptor(
        "session-window-mapping",
        std::make_shared<TimeWindowNsSerializer>(),
        std::make_shared<TimeWindowNsSerializer>(),
        memoryPool());
    mappingState_ =
        keyedBackend_
            ->getOrCreateMapState<SliceEndNs, TimeWindowNs, TimeWindowNs>(
                mappingDescriptor);
  } else {
    stateHandle_ =
        keyedBackend_->getOrCreateAggregatingState<SliceEndNs>(descriptor);
    state_ =
        dynamic_cast<HeapAggregatingState<RowContainerStateKey, SliceEndNs>*>(
            stateHandle_.get());
    VELOX_CHECK_NOT_NULL(
        state_,
        "the heap keyed state backend must produce a HeapAggregatingState");
  }

  auto* valueRows =
      isSession ? sessionState_->valueRows() : state_->valueRows();
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
  if (isSession) {
    sessionTimerService_ =
        std::make_unique<InternalTimerService<WindowTimerKey, TimeWindowNs>>(
            static_cast<Triggerable<WindowTimerKey, TimeWindowNs>*>(this),
            std::make_unique<NoopProcessingTimeScheduler>());
  }

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

  if (static_cast<WindowType>(node_->windowType()) == WindowType::SESSION) {
    advanceSession();
    input_.reset();
    keyedBackend_->advance();
    if (!node_->isEventTime() && procTimeScheduler_) {
      fireDue(WindowTimezoneUtil::getCurrentProcessingTime());
      armProcTimeWakeUp();
    }
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
    if (sessionTimerService_) {
      sessionTimerService_->advanceWatermark(timestamp);
    } else {
      timerService_->advanceWatermark(timestamp);
    }
    currentProgress_ = timestamp;
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
  fireWindow(key, windowStart(ns.sliceEnd()), ns.sliceEnd());
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
  if (sessionTimerService_) {
    sessionTimerService_->advanceWatermark(std::numeric_limits<int64_t>::max());
    sessionTimerService_->close();
    sessionTimerService_.reset();
  }
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
  sessionState_ = nullptr;
  sessionStateHandle_.reset();
  mappingState_.reset();
  sessionRowCache_.clear();
  keyedBackend_.reset();
  keySelector_.reset();
  procFireIndex_.clear();
  registeredFires_.clear();
  fireRowCache_.clear();
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
  if (static_cast<WindowType>(node_->windowType()) == WindowType::SESSION) {
    fireSessionWindow(key, windowStart, windowEnd);
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

void WindowAggregator::advanceSession() {
  // Flink UnsliceWindowAggProcessor#processElement over the
  const int64_t gap = node_->size();
  const auto numRows = input_->size();

  // Per-row UTC timestamps; a proc-time batch shares the batch-arrival
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
      rowTs[row] = WindowTimezoneUtil::toUtcTimestampMills(ms, tz_);
    }
  } else {
    const int64_t now = WindowTimezoneUtil::toUtcTimestampMills(
        WindowTimezoneUtil::getCurrentProcessingTime(), tz_);
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
    // mapping once per key per batch (the truth source; Flink keeps an
    std::map<int64_t, std::pair<int64_t, TimeWindowNs>> inFlight;
    std::set<TimeWindowNs> mappingSnapshot;
    for (const auto& entry : mappingState_->entries(key, kMappingNs)) {
      inFlight.emplace(
          entry.first.start(), std::make_pair(entry.first.end(), entry.second));
      mappingSnapshot.insert(entry.first);
    }

    // Batch-local state row cache: a row only accumulates into a live
    bool rowCached = false;
    TimeWindowNs cachedStateWin{};
    char* cachedRow = nullptr;

    for (vector_size_t row : rowsOfKey[d]) {
      const TimeWindowNs newWindow(rowTs[row], rowTs[row] + gap);

      // Tail fast path: an in-order row touches only the last window
      // tail starts). Swallowed rows change nothing at all (Flink's
      if (!inFlight.empty()) {
        auto tailIt = std::prev(inFlight.end());
        const int64_t tailStart = tailIt->first;
        const int64_t tailEnd = tailIt->second.first;
        const TimeWindowNs tailStateWin = tailIt->second.second;
        if (tailStart <= newWindow.start() && newWindow.start() <= tailEnd) {
          // No late check is needed on this path: the merged end is
          const int64_t extendedEnd = std::max(tailEnd, newWindow.end());
          if (extendedEnd != tailEnd) {
            tailIt->second.first = extendedEnd;
          }
          if (!rowCached || !(cachedStateWin == tailStateWin)) {
            cachedRow = sessionState_->row(key, tailStateWin);
            sessionRowCache_[{
                key.row(), tailStateWin.start(), tailStateWin.end()}] =
                cachedRow;
            cachedStateWin = tailStateWin;
            rowCached = true;
          }
          rowGroups_[row] = cachedRow;
          continue;
        }
      }

      // General path: out-of-order rows and fresh windows, probed by
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
        if (node_->isEventTime() &&
            WindowTimezoneUtil::isWindowFired(
                newWindow.end(), currentProgress_, tz_)) {
          lateDroppedRows_++;
          keptRows.setValid(row, false);
          anyDropped = true;
          continue;
        }
        inFlight.emplace(
            newWindow.start(), std::make_pair(newWindow.end(), newWindow));
        cachedRow = sessionState_->row(key, newWindow);
        sessionRowCache_[{key.row(), newWindow.start(), newWindow.end()}] =
            cachedRow;
        cachedStateWin = newWindow;
        rowCached = true;
        rowGroups_[row] = cachedRow;
        continue;
      }

      // MergingWindowSet#addWindow: the surviving state namespace is
      // any merged window's state window (Flink picks the first of a
      const TimeWindowNs stateWin = merged[0].second.second;
      for (const auto& m : merged) {
        auto mIt = inFlight.find(m.first);
        const TimeWindowNs mState = mIt->second.second;
        inFlight.erase(mIt);
        if (!(mState == stateWin)) {
          // Fold the merged-away state window's accumulator into the
          // survivor and drop its entry (Flink
          char* survivor = sessionState_->row(key, stateWin);
          sessionRowCache_[{key.row(), stateWin.start(), stateWin.end()}] =
              survivor;
          auto srcIt =
              sessionRowCache_.find({key.row(), mState.start(), mState.end()});
          char* source =
              srcIt == sessionRowCache_.end() ? nullptr : srcIt->second;
          if (source) {
            mergeAccumulatorRows(survivor, source);
          }
          sessionState_->remove(key, mState);
          sessionRowCache_.erase({key.row(), mState.start(), mState.end()});
          rowCached = false;
        }
      }
      inFlight.insert_or_assign(
          result.start(), std::make_pair(result.end(), stateWin));

      // No late check here either: the cover end dominates every
      if (!rowCached || !(cachedStateWin == stateWin)) {
        cachedRow = sessionState_->row(key, stateWin);
        sessionRowCache_[{key.row(), stateWin.start(), stateWin.end()}] =
            cachedRow;
        cachedStateWin = stateWin;
        rowCached = true;
      }
      rowGroups_[row] = cachedRow;
    }

    // Apply the per-key NET change as the snapshot-vs-final diff: the
    std::map<TimeWindowNs, TimeWindowNs> finalMapping;
    for (const auto& [start, value] : inFlight) {
      finalMapping[TimeWindowNs(start, value.first)] = value.second;
    }
    for (const auto& actual : mappingSnapshot) {
      if (finalMapping.count(actual) == 0) {
        mappingState_->remove(key, kMappingNs, actual);
        unregisterSessionFire(key, actual.start(), actual.end());
      }
    }
    for (const auto& [actual, stateWin] : finalMapping) {
      if (mappingSnapshot.count(actual) == 0) {
        mappingState_->put(key, kMappingNs, actual, stateWin);
        registerFire(key, nullptr, actual.start(), actual.end());
      }
    }
  }

  // One vectorized accumulation pass over the kept rows (late rows
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
  const TimeWindowNs actual(windowStart, windowEnd);
  const auto stateWin = mappingState_->get(key, kMappingNs, actual);
  if (!stateWin.has_value()) {
    return;
  }
  auto cacheIt =
      sessionRowCache_.find({key.row(), stateWin->start(), stateWin->end()});
  char* row = cacheIt == sessionRowCache_.end() ? nullptr : cacheIt->second;
  if (row == nullptr) {
    return;
  }
  if (!countStarIsZero(row, countStarOffset_)) {
    emitWindowResult(key, windowStart, windowEnd, row);
  }
  // cleanWindowIfNeeded: clear the state window's accumulator and
  sessionState_->remove(key, *stateWin);
  sessionRowCache_.erase({key.row(), stateWin->start(), stateWin->end()});
  mappingState_->remove(key, kMappingNs, actual);
}

void WindowAggregator::unregisterSessionFire(
    const RowContainerStateKey& key,
    int64_t windowStart,
    int64_t windowEnd) {
  // The SESSION merge re-hang: Flink clears the merged-away windows'
  sessionTimerService_->deleteEventTimeTimer(
      WindowTimerKey{key.schema(), key.row(), key.hash(), key.keyGroup()},
      TimeWindowNs(windowStart, windowEnd),
      WindowTimezoneUtil::toEpochMillsForTimer(windowEnd - 1, tz_));
}

void WindowAggregator::onEventTime(
    std::shared_ptr<TimerHeapInternalTimer<WindowTimerKey, TimeWindowNs>>
        timer) {
  const auto& ns = timer->ns();
  const auto& timerKey = timer->key();
  RowContainerStateKey key(
      timerKey.schema, timerKey.row, timerKey.hash, maxParallelism_);
  fireSessionWindow(key, ns.start(), ns.end());
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
    WindowTimerKey timerKey{
        key.schema(), key.row(), key.hash(), key.keyGroup()};
    const int64_t due =
        WindowTimezoneUtil::toEpochMillsForTimer(windowEnd - 1, tz_);
    if (sessionTimerService_) {
      sessionTimerService_->registerEventTimeTimer(
          timerKey, TimeWindowNs(windowStart, windowEnd), due);
      return;
    }
    timerService_->registerEventTimeTimer(timerKey, SliceEndNs(windowEnd), due);
    return;
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
  if (sessionState_) {
    // SESSION: timers carry the ACTUAL windows (the mapping's keys),
    std::map<const char*, RowContainerStateKey> keys;
    sessionState_->forEachEntry(
        [&keys](const RowContainerStateKey& key, const TimeWindowNs& ns) {
          keys.emplace(key.row(), key);
        });
    for (const auto& [rowPtr, key] : keys) {
      // Prime the session row cache for restored state windows (the
      for (const auto& entry : mappingState_->entries(key, kMappingNs)) {
        sessionRowCache_[{
            key.row(), entry.second.start(), entry.second.end()}] =
            sessionState_->row(key, entry.second);
        registerFire(key, nullptr, entry.first.start(), entry.first.end());
      }
    }
  } else {
    state_->forEachEntry(
        [this](const RowContainerStateKey& key, const SliceEndNs& ns) {
          char* row = state_->row(key, ns);
          registerFire(
              key,
              row,
              sliceAssigner_->getWindowStart(ns.sliceEnd()),
              ns.sliceEnd());
        });
  }
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
