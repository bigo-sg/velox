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

#include "velox/experimental/stateful/GroupWindowAggregator.h"

#include <utility>

#include "velox/exec/Aggregate.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::stateful {

namespace {

// Fills one single-row window property column with raw epoch millis: the
// TIMESTAMP(3) form (the window boundary and time attribute columns) or
// the BIGINT form.
void fillWindowPropertyMillis(const VectorPtr& column, int64_t millis) {
  switch (column->type()->kind()) {
    case TypeKind::TIMESTAMP: {
      auto* flat = column->asFlatVector<Timestamp>();
      flat->set(0, Timestamp::fromMillis(millis));
      break;
    }
    case TypeKind::BIGINT: {
      auto* flat = column->asFlatVector<int64_t>();
      flat->set(0, millis);
      break;
    }
    default:
      VELOX_UNSUPPORTED(
          "group window aggregation cannot emit a window property column of type {}",
          column->type()->toString());
  }
}

} // namespace

GroupWindowAggregator::GroupWindowAggregator(
    std::unique_ptr<GroupWindowAggsHandler> windowAggsHandler,
    std::vector<StatefulOperatorPtr> targets,
    const std::shared_ptr<const GroupWindowAggregationNode>& node)
    : StatefulOperator(std::move(windowAggsHandler), std::move(targets)),
      node_(node) {
  windowAggsHandler_ = dynamic_cast<GroupWindowAggsHandler*>(op().get());
  VELOX_CHECK_NOT_NULL(
      windowAggsHandler_,
      "GroupWindowAggregator must wrap a GroupWindowAggsHandler");
  windowAggsHandler_->setOwner(this);
}

void GroupWindowAggregator::initialize() {
  // Aggregate details first: the backend wiring below consumes them.
  windowAggsHandler_->initializeWindowAggregates(
      node_->inputType(),
      static_cast<uint32_t>(node_->keyedStateSpec()->keyChannels().size()),
      op()->operatorCtx()->driverCtx()->queryConfig());

  // Window facilities from the plan's window parameters: the assigner keyed
  // by window type (SESSION merges, TUMBLE/HOP do not), the AfterEndOfWindow
  // trigger, and the operator-owned event-time timer service (the old
  // stateHandler entry is NYI on the typed backends).
  switch (static_cast<WindowType>(node_->windowType())) {
    case WindowType::SESSION:
      windowAssigner_ = std::make_shared<SessionWindowAssigner>(
          node_->gap(), node_->isEventTime());
      break;
    case WindowType::TUMBLE:
      windowAssigner_ = std::make_shared<TumblingWindowAssigner>(
          node_->size(), node_->isEventTime());
      break;
    case WindowType::HOP:
      windowAssigner_ = std::make_shared<SlidingWindowAssigner>(
          node_->size(), node_->slide(), node_->isEventTime());
      break;
    default:
      VELOX_UNSUPPORTED(
          "group window aggregation does not support windowType {}",
          node_->windowType());
  }
  trigger_ = std::make_shared<AfterEndOfWindow>();
  windowTimerService_ =
      std::make_shared<InternalTimerService<TimerKey, TimeWindow>>(this);
  triggerContext_ = std::make_shared<WindowTriggerContext>(this);
  windowContext_ = std::make_shared<WindowContext>(this);
  triggerContext_->open();

  StatefulOperator::initialize();
}

void GroupWindowAggregator::initializeStateBackend(StateBackend* stateBackend) {
  // Five steps per the stateful-operator contract.
  const auto& spec = *node_->keyedStateSpec();
  keyedBackend_ = createKeyedStateBackend<RowContainerStateKey>(
      *stateBackend, spec, memoryPool()); // 1. backend (RocksDB fails loud).
  keySelector_ = std::make_unique<KeySelector>(
      keyedBackend_->keyIndex(), spec.keyChannels()); // 2. bind the key table.

  std::vector<exec::Accumulator> accumulators;
  accumulators.reserve(windowAggsHandler_->aggregates().size());
  for (const auto& aggregate : windowAggsHandler_->aggregates()) {
    accumulators.emplace_back(
        aggregate.function.get(), aggregate.intermediateType);
  }
  AggregatingStateDescriptor descriptor(
      "window-aggs",
      std::move(accumulators),
      [this](char** rows, size_t numRows) {
        windowAggsHandler_->initializeRows(rows, numRows);
      },
      [this](char* row, const std::vector<VectorPtr>& columns) {
        windowAggsHandler_->mergeRow(row, columns);
      },
      memoryPool());
  // 3a. main handle: one accumulator row per (key, state window).
  state_ = keyedBackend_->getOrCreateAggregatingState<TimeWindow>(descriptor);

  // The window process function keyed by window type. SESSION: the merging
  // family over the in-flight mapping (3b below builds the handle and the
  // function consumes it). TUMBLE / HOP: the general non-merging path — the
  // window IS the state namespace, no mapping handle, no per-key cache.
  if (static_cast<WindowType>(node_->windowType()) == WindowType::SESSION) {
    // 3b. session in-flight set: actual window -> state window per key, the
    // fixed VoidNamespace (the map is the per-key window table).
    MapStateDescriptor<TimeWindow, TimeWindow> mappingDescriptor(
        "session-window-mapping",
        std::make_shared<TimeWindowSerializer>(),
        std::make_shared<TimeWindowSerializer>(),
        memoryPool());
    mappingState_ =
        keyedBackend_
            ->getOrCreateMapState<VoidNamespace, TimeWindow, TimeWindow>(
                mappingDescriptor);

    // The session process function consumes the typed mapping handle; the
    // context routes its state access to this operator's handles.
    windowFunction_ = std::make_unique<MergingWindowProcessFunction>(
        std::dynamic_pointer_cast<MergingWindowAssigner>(windowAssigner_),
        windowAggsHandler_,
        node_->allowedLateness(),
        mappingState_);
  } else {
    windowFunction_ = std::make_unique<GeneralWindowProcessFunction>(
        windowAssigner_, windowAggsHandler_, node_->allowedLateness());
  }
  windowFunction_->open(windowContext_);

  // 4. Wire setAllocator / setOffsets before the first rows() call (and
  // before restore, which also materializes rows).
  auto* valueRows = state_->valueRows();
  for (size_t i = 0; i < windowAggsHandler_->aggregates().size(); ++i) {
    auto& function = windowAggsHandler_->aggregates()[i].function;
    function->setAllocator(&valueRows->stringAllocator());
    const auto rowColumn = valueRows->columnAt(static_cast<int32_t>(i));
    function->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rowColumn.initializedByte(),
        rowColumn.initializedMask(),
        valueRows->rowSizeOffset());
  }

  // 5. Cascade downstream.
  StatefulOperator::initializeStateBackend(stateBackend);
}

void GroupWindowAggregator::checkInputRowKinds(
    const StreamRecord& record) const {
  const auto& kinds = record.rowKind();
  if (kinds == nullptr) {
    return; // appendOnly: all INSERT.
  }
  for (auto i = 0; i < kinds->size(); ++i) {
    if (kinds->valueAt(i) != static_cast<int8_t>(RowKind::INSERT)) {
      // TODO(L6): retract input arrives with the P1 retract-to-window chain.
      VELOX_UNSUPPORTED(
          "group window aggregation in insert-only mode received a non-INSERT row "
          "kind {} at row {}",
          static_cast<int32_t>(kinds->valueAt(i)),
          i);
    }
  }
}

void GroupWindowAggregator::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  checkInputRowKinds(*record); // 1. cast + rowKind validation.
  const auto& inputVector = record->record();
  const auto numRows = inputVector->size();
  if (numRows == 0) {
    return; // this operator's own empty-batch defense.
  }

  keySelector_->probe(inputVector); // 2. per-row keys.

  // The rowtime column drives the window assignment. TIMESTAMP(3) passes
  // through as a velox TIMESTAMP (session zone UTC, shift 0: the raw millis
  // value is the window algebra's input, SPEC alignment #1); a BIGINT
  // rowtime column carries raw millis directly.
  auto rowtimeVector = inputVector->childAt(node_->rowtimeIndex());
  const auto& rowtimeColumn = *rowtimeVector;
  std::vector<int64_t> rowtimes;
  rowtimes.reserve(numRows);
  switch (rowtimeColumn.typeKind()) {
    case TypeKind::TIMESTAMP: {
      const auto* timestamps = rowtimeVector->as<SimpleVector<Timestamp>>();
      VELOX_CHECK_NOT_NULL(
          timestamps, "the rowtime TIMESTAMP vector is not a simple vector");
      for (vector_size_t row = 0; row < numRows; ++row) {
        VELOX_CHECK(
            !timestamps->isNullAt(row),
            "group window aggregation received a null rowtime at row {}",
            row);
        rowtimes.push_back(timestamps->valueAt(row).toMillis());
      }
      break;
    }
    case TypeKind::BIGINT: {
      const auto* millis = rowtimeVector->as<SimpleVector<int64_t>>();
      VELOX_CHECK_NOT_NULL(
          millis, "the rowtime BIGINT vector is not a simple vector");
      for (vector_size_t row = 0; row < numRows; ++row) {
        VELOX_CHECK(
            !millis->isNullAt(row),
            "group window aggregation received a null rowtime at row {}",
            row);
        rowtimes.push_back(millis->valueAt(row));
      }
      break;
    }
    default:
      VELOX_UNSUPPORTED(
          "group window aggregation requires a TIMESTAMP or BIGINT rowtime column, got {}",
          rowtimeColumn.type()->toString());
  }

  // One-row selections and per-aggregate arguments, reused across the rows
  // of this batch (batch scratch).
  SelectivityVector rowSelection(numRows, false);
  std::vector<std::vector<VectorPtr>> aggregateArgs;
  aggregateArgs.reserve(windowAggsHandler_->aggregates().size());
  for (const auto& aggregate : windowAggsHandler_->aggregates()) {
    aggregateArgs.push_back(
        windowAggsHandler_->aggregateArgs(aggregate, inputVector));
  }

  for (vector_size_t row = 0; row < numRows; ++row) {
    const int64_t timestamp = rowtimes[row];
    // 3. assign the state namespace (session merging, lateness filter):
    // the state windows return in the same order as the actual windows.
    const auto& key = keySelector_->keys()[row];
    // The key view copies, it never assigns (RowContainerStateKey deletes
    // assignment): optional::emplace constructs in place.
    currentKey_.emplace(key);
    std::vector<TimeWindow> stateWindows =
        windowFunction_->assignStateNamespace(key, timestamp);
    std::vector<TimeWindow> actualWindows =
        windowFunction_->assignActualWindows();
    if (actualWindows.empty()) {
      // Every assigned window is late: drop the row and count it (Flink's
      // lateRecordsDroppedRate equivalent).
      ++numLateRecordsDropped_;
      continue;
    }

    rowSelection.setValid(row, true);
    rowSelection.updateBounds();
    for (size_t w = 0; w < actualWindows.size(); ++w) {
      // 4. resolve the state row of this row's window and accumulate the
      // row into it (misses materialize through the descriptor init
      // callback; the per-row resolution matches Flink's element loop - a
      // later merge inside this batch may absorb earlier rows' windows,
      // folding their accumulators into the surviving state window).
      char* stateRow = state_->row(key, TimeWindow(stateWindows[w]));
      const auto& aggregates = windowAggsHandler_->aggregates();
      for (size_t a = 0; a < aggregates.size(); ++a) {
        aggregates[a].function->addSingleGroupRawInput(
            stateRow, rowSelection, aggregateArgs[a], false);
      }

      // 5/6. trigger: register the fire timer (AfterEndOfWindow) and the
      // cleanup timer; an immediate fire (window already past the
      // watermark) emits right away - unreachable at allowedLateness = 0
      // where such a window is late and dropped in step 3.
      if (triggerContext_->onElement(
              key, nullptr, timestamp, actualWindows[w])) {
        emitWindowResult(key, actualWindows[w]);
      }
      registerCleanupTimer(key, actualWindows[w]);
    }
    rowSelection.setValid(row, false);
    rowSelection.updateBounds();
  }

  // Batch boundary: reclaim key rows no state entry references; drop the
  // in-flight window cache (its keys are row views that must not outlive
  // the batch - the mapping state reloads on the next access).
  windowFunction_->clearCache(); // 7. batch boundary.
  keyedBackend_->advance();
}

void GroupWindowAggregator::processWatermark(int64_t timestamp) {
  // C5: the operator-held last watermark drives isWindowLate and the
  // trigger decisions; advance it before the timers pop.
  lastWatermark_ = timestamp;
  if (windowTimerService_) {
    windowTimerService_->advanceWatermark(timestamp);
  }
  StatefulOperator::processWatermark(timestamp);
}

void GroupWindowAggregator::onEventTime(
    std::shared_ptr<TimerHeapInternalTimer<TimerKey, TimeWindow>> timer) {
  // The key view copies, it never assigns (see addInput): optional::emplace
  // constructs in place. TimerKey dereferences to the shared row view.
  currentKey_.emplace(*timer->key());
  const TimeWindow window = timer->ns();
  const int64_t time = timer->timestamp();
  // The callback runs under the timer service's queue lock; the cleanup
  // chain below re-enters the queue only through a delete of the popped
  // registration itself - a no-op the fired-timer match turns into a plain
  // skip, while any other re-entrant queue mutation fails loud instead of
  // deadlocking the plain mutex (see the member comment).
  inTimerCallback_ = true;
  firedTimer_ = std::move(timer);
  if (triggerContext_->onEventTime(window, time)) {
    // Fire the window and emit its result (empty windows suppressed).
    emitWindowResult(*firedTimer_->key(), window);
  }
  if (windowAssigner_->isEventTime()) {
    // Clean exactly at the cleanup time (the fire and cleanup timers are
    // the same (key, window, time) registration at allowedLateness = 0 and
    // dedup into one pop).
    windowFunction_->cleanWindowIfNeeded(window, time);
  }
  firedTimer_ = nullptr;
  inTimerCallback_ = false;
}

void GroupWindowAggregator::registerCleanupTimer(
    const RowContainerStateKey& key,
    TimeWindow window) {
  int64_t cleanupTime = TimeWindowUtil::toEpochMillsForTimer(
      TimeWindowUtil::cleanupTime(
          window.maxTimestamp(),
          node_->allowedLateness(),
          node_->isEventTime()),
      0);
  if (cleanupTime == INT64_MAX) {
    // Do not set a GC timer for "end of time".
    return;
  }

  if (windowAssigner_->isEventTime()) {
    triggerContext_->registerEventTimeTimer(key, window, cleanupTime);
  } else {
    triggerContext_->registerProcessingTimeTimer(key, window, cleanupTime);
  }
}

void GroupWindowAggregator::emitWindowResult(
    const RowContainerStateKey& key,
    TimeWindow window) {
  // Layout guard: the row assembly below assumes the conventional output
  // layout - keys at [0, numKeys), aggregates right after, then the window
  // property columns at the tail. The planner's windowStartIndex /
  // windowEndIndex helpers always produce it (the upstream window
  // properties are named start-before-end); a hand-built node with an
  // interleaved column order would otherwise assemble mismatched names
  // and types silently.
  const auto& outputType = node_->outputType();
  const int32_t numKeys =
      static_cast<int32_t>(node_->keyedStateSpec()->keyChannels().size());
  const int32_t numAggs =
      static_cast<int32_t>(windowAggsHandler_->aggregates().size());
  const int32_t windowStartIndex = node_->windowStartIndex();
  const int32_t windowEndIndex = node_->windowEndIndex();
  VELOX_CHECK(
      (windowStartIndex == -1 || windowStartIndex == numKeys + numAggs) &&
          (windowEndIndex == -1 || windowEndIndex == numKeys + numAggs ||
           windowEndIndex == numKeys + numAggs + 1),
      "group window aggregation expects the window property columns at the "
      "output tail (keys, aggregates, then window start / end and the time "
      "attributes), got "
      "windowStartIndex={}, windowEndIndex={}, numKeys={}, numAggs={}",
      windowStartIndex,
      windowEndIndex,
      numKeys,
      numAggs);

  // D1 scheme A: the existence read - an empty window (no state entry)
  // never materializes state and emits nothing.
  char* stateRow =
      windowFunction_->prepareAggregateAccumulatorForEmit(key, window);
  if (stateRow == nullptr) {
    return;
  }

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> children;
  names.reserve(outputType->size());
  types.reserve(outputType->size());
  children.reserve(outputType->size());

  // Key columns from the key row container.
  if (numKeys > 0) {
    char* keyRow = const_cast<char*>(key.row());
    for (int32_t k = 0; k < numKeys; ++k) {
      names.push_back(outputType->nameOf(k));
      types.push_back(outputType->childAt(k));
      VectorPtr keyColumn =
          BaseVector::create(outputType->childAt(k), 1, memoryPool());
      keySelector_->keyRowContainer()->extractColumn(&keyRow, 1, k, keyColumn);
      children.push_back(std::move(keyColumn));
    }
  }

  // Aggregate values from the state row.
  std::vector<char*> stateRows{stateRow};
  std::vector<VectorPtr> values;
  windowAggsHandler_->extractValues(stateRows, values);
  for (size_t a = 0; a < windowAggsHandler_->aggregates().size(); ++a) {
    const auto outputIndex = numKeys + static_cast<int32_t>(a);
    names.push_back(outputType->nameOf(outputIndex));
    types.push_back(outputType->childAt(outputIndex));
    children.push_back(std::move(values[a]));
  }

  // Window property columns at the tail, every declared one: start / end
  // from the window, the time attributes per the planner's fixed property
  // order. The planner pushes all four properties of a rowtime window and
  // orders them [start, end, rowtime, proctime] (WindowPropertiesRule), so
  // the tail beyond start / end is the time attributes in that order.
  // Flink's generated handler fills rowtime with window.maxTimestamp()
  // (the end - 1) and leaves the proctime attribute null at the position
  // ("it will be null at the position later" - AggsHandlerCodeGenerator.
  // getWindowExpressions, the TimeWindow branch, emitted through
  // AggregateWindowOperator.emitWindowResult -> getValue). The emission
  // width must match the declared outputType width: the Java bridge slices
  // rowType.size() children off the emitted row. TIMESTAMP(3), session
  // zone UTC: the raw millis pass through, SPEC alignment #1.
  int32_t timeAttributeOrdinal = 0;
  for (int32_t c = numKeys + numAggs; c < outputType->size(); ++c) {
    names.push_back(outputType->nameOf(c));
    types.push_back(outputType->childAt(c));
    VectorPtr column =
        BaseVector::create(outputType->childAt(c), 1, memoryPool());
    if (c == windowStartIndex || c == windowEndIndex) {
      fillWindowPropertyMillis(
          column, c == windowStartIndex ? window.start() : window.end());
    } else if (timeAttributeOrdinal == 0) {
      // The rowtime attribute: the window's maxTimestamp (end - 1).
      fillWindowPropertyMillis(column, window.maxTimestamp());
      ++timeAttributeOrdinal;
    } else if (timeAttributeOrdinal == 1) {
      // The proctime attribute: null at the position (Flink semantics).
      column->setNull(0, true);
      ++timeAttributeOrdinal;
    } else {
      VELOX_UNSUPPORTED(
          "group window aggregation cannot emit more time attribute columns "
          "than [rowtime, proctime] at column {}",
          c);
    }
    children.push_back(std::move(column));
  }

  auto output = std::make_shared<RowVector>(
      memoryPool(),
      ROW(std::move(names), std::move(types)),
      BufferPtr(nullptr),
      1,
      std::move(children));
  // P0 default EMIT: every emission is an INSERT.
  pushOutput(
      std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
}

std::vector<std::string> GroupWindowAggregator::snapshotKeyedState() {
  VELOX_CHECK_NOT_NULL(
      keyedBackend_, "the keyed state backend is not initialized");
  return {keyedBackend_->snapshot()};
}

void GroupWindowAggregator::restoreKeyedState(
    const std::vector<std::string>& records) {
  VELOX_CHECK_NOT_NULL(
      keyedBackend_, "the keyed state backend is not initialized");
  for (const auto& record : records) {
    keyedBackend_->restore(record);
  }
  // A2: the restore dispatch is complete and both handles are in place -
  // rebuild the timers before the first input or watermark arrives.
  rebuildTimersFromState();
}

void GroupWindowAggregator::rebuildTimersFromState() {
  // The timer queue rides outside the typed snapshot plane (C3); it is
  // derived back from the restored state before the first input or
  // watermark arrives. Fire = the window's end - 1 (the AfterEndOfWindow
  // trigger time), cleanup at the same point at allowedLateness = 0 - one
  // deduped queue entry per live window. The last watermark starts from
  // its initial value, so rebuilt timers due past it fire as the watermark
  // advances, with or without new input.
  if (static_cast<WindowType>(node_->windowType()) == WindowType::SESSION) {
    // Session: the key set comes from the aggregating state (a key with a
    // live session always carries an accumulator entry, because addInput,
    // the merge consumer and cleanWindowIfNeeded maintain the mapping and
    // the accumulators together); the windows come from the session
    // mapping (the actual -> state window pairs), the same handle the
    // runtime reads - every live actual window gets its fire / cleanup
    // pair back.
    state_->visitKeys([this](const RowContainerStateKey& key) {
      const auto mapping =
          mappingState_->entries(key, VoidNamespace::instance());
      for (const auto& entry : mapping) {
        TimeWindow actualWindow = entry.first;
        const int64_t fireTime = TimeWindowUtil::toEpochMillsForTimer(
            actualWindow.maxTimestamp(), triggerContext_->getShiftTimeZone());
        triggerContext_->registerEventTimeTimer(key, actualWindow, fireTime);
        registerCleanupTimer(key, actualWindow);
      }
    });
  } else {
    // TUMBLE / HOP: the window IS the state namespace (the general
    // non-merging path has no mapping state) - every live (key, window)
    // entry of the aggregating state gets its fire / cleanup pair back.
    state_->visitEntries(
        [this](const RowContainerStateKey& key, const TimeWindow& window) {
          // Window::maxTimestamp is non-const; the namespace view is copied
          // (16 bytes) for the timer math.
          TimeWindow w = window;
          const int64_t fireTime = TimeWindowUtil::toEpochMillsForTimer(
              w.maxTimestamp(), triggerContext_->getShiftTimeZone());
          triggerContext_->registerEventTimeTimer(key, w, fireTime);
          registerCleanupTimer(key, w);
        });
  }
}

void GroupWindowAggregator::close() {
  // Reverse construction order: the state handles capture the handler and
  // this operator through their descriptor closures, so they go first, then
  // the timer service, then the backend - the base reset of op() (the
  // handler) comes last.
  windowFunction_.reset();
  state_.reset();
  mappingState_.reset();
  if (windowTimerService_) {
    windowTimerService_->close();
    windowTimerService_.reset();
  }
  triggerContext_.reset();
  windowContext_.reset();
  trigger_.reset();
  windowAssigner_.reset();
  keyedBackend_.reset();
  keySelector_.reset();
  StatefulOperator::close();
}

// --- WindowTriggerContext ---

void GroupWindowAggregator::WindowTriggerContext::open() {
  owner_->trigger_->open(shared_from_this());
}

bool GroupWindowAggregator::WindowTriggerContext::onElement(
    const RowContainerStateKey& key,
    RowVectorPtr row,
    int64_t timestamp,
    TimeWindow window) {
  return owner_->trigger_->onElement(key, std::move(row), timestamp, window);
}

bool GroupWindowAggregator::WindowTriggerContext::onProcessingTime(
    TimeWindow window,
    int64_t time) {
  return owner_->trigger_->onProcessingTime(window, time);
}

bool GroupWindowAggregator::WindowTriggerContext::onEventTime(
    TimeWindow window,
    int64_t time) {
  return owner_->trigger_->onEventTime(window, time);
}

void GroupWindowAggregator::WindowTriggerContext::onMerge(
    const RowContainerStateKey& key,
    TimeWindow window) {
  owner_->trigger_->onMerge(key, window, nullptr);
}

int64_t
GroupWindowAggregator::WindowTriggerContext::getCurrentProcessingTime() {
  return TimeWindowUtil::getCurrentProcessingTime();
}

int64_t GroupWindowAggregator::WindowTriggerContext::getCurrentWatermark() {
  // C5: the operator-held last watermark (the timer service's
  // currentWatermark is the earliest pending timer, unusable here).
  return owner_->lastWatermark_;
}

void GroupWindowAggregator::WindowTriggerContext::registerProcessingTimeTimer(
    const RowContainerStateKey& key,
    TimeWindow window,
    int64_t time) {
  // TODO(L4): processing-time timers arrive with the proctime windows (P1).
  VELOX_UNSUPPORTED(
      "group window aggregation does not support processing-time timers");
}

void GroupWindowAggregator::WindowTriggerContext::registerEventTimeTimer(
    const RowContainerStateKey& key,
    TimeWindow window,
    int64_t time) {
  // Re-entrant registration during the onEventTime callback would deadlock
  // the queue lock the callback runs under (see the member comment).
  VELOX_CHECK(
      !owner_->inTimerCallback_,
      "registering a timer inside onEventTime would deadlock the timer queue");
  owner_->windowTimerService_->registerEventTimeTimer(TimerKey(key), window, time);
}

void GroupWindowAggregator::WindowTriggerContext::deleteProcessingTimeTimer(
    const RowContainerStateKey& key,
    TimeWindow window,
    int64_t time) {
  VELOX_UNSUPPORTED(
      "group window aggregation does not support processing-time timers");
}

void GroupWindowAggregator::WindowTriggerContext::deleteEventTimeTimer(
    const RowContainerStateKey& key,
    TimeWindow window,
    int64_t time) {
  if (owner_->inTimerCallback_) {
    // The callback runs under the timer service's queue lock, so a queue
    // call here would deadlock the plain mutex. The fired window's own
    // cleanup chain (clearTrigger -> deleteEventTimeTimer) targets exactly
    // the registration the pop already removed from the deduplicated
    // queue - the match is that no-op skip; any other re-entrant delete
    // is a bug and fails loud instead of silently dropping a live timer.
    VELOX_CHECK_NOT_NULL(
        owner_->firedTimer_,
        "the fired timer is not tracked inside onEventTime");
    const auto& fired = *owner_->firedTimer_;
    VELOX_CHECK(
        fired.key().key->equals(key) && fired.ns() == window &&
            fired.timestamp() == time,
        "deleting a timer other than the fired one inside onEventTime would "
        "deadlock the timer queue: ({}, {})",
        window.toString(),
        time);
    return;
  }
  owner_->windowTimerService_->deleteEventTimeTimer(TimerKey(key), window, time);
}

int GroupWindowAggregator::WindowTriggerContext::getShiftTimeZone() {
  return 0; // TODO(L5): TIMESTAMP_LTZ shift arrives with P1.
}

void GroupWindowAggregator::WindowTriggerContext::clear(
    const RowContainerStateKey& key,
    TimeWindow window) {
  owner_->trigger_->clear(key, window);
}

StatePtr GroupWindowAggregator::WindowTriggerContext::getPartitionedState(
    StateDescriptor& /*stateDescriptor*/) {
  // The pre-generic stateHandler plane is gone; trigger-side state arrives
  // with the EMIT strategies (P1).
  VELOX_NYI("trigger partitioned state is not supported");
}

void GroupWindowAggregator::WindowTriggerContext::mergePartitionedState(
    StateDescriptor& /*stateDescriptor*/) {
  VELOX_NYI("trigger partitioned state is not supported");
}

// --- WindowContext ---

const RowContainerStateKey& GroupWindowAggregator::WindowContext::currentKey() {
  VELOX_CHECK(
      owner_->currentKey_.has_value(),
      "the window context is used before a key is set");
  return *owner_->currentKey_;
}

int64_t GroupWindowAggregator::WindowContext::currentProcessingTime() {
  return TimeWindowUtil::getCurrentProcessingTime();
}

int64_t GroupWindowAggregator::WindowContext::currentWatermark() {
  return owner_->lastWatermark_;
}

int GroupWindowAggregator::WindowContext::getShiftTimeZone() {
  return 0; // TODO(L5): TIMESTAMP_LTZ shift arrives with P1.
}

char* GroupWindowAggregator::WindowContext::getWindowAccumulators(
    TimeWindow window) {
  return owner_->state_->get(currentKey(), TimeWindow(window));
}

char* GroupWindowAggregator::WindowContext::getOrCreateWindowAccumulators(
    TimeWindow window) {
  return owner_->state_->row(currentKey(), TimeWindow(window));
}

void GroupWindowAggregator::WindowContext::clearWindowState(TimeWindow window) {
  owner_->state_->remove(currentKey(), TimeWindow(window));
}

void GroupWindowAggregator::WindowContext::clearPreviousState(
    TimeWindow /*window*/) {
  // Previous state is disabled in session windows.
}

void GroupWindowAggregator::WindowContext::clearTrigger(TimeWindow window) {
  owner_->triggerContext_->clear(currentKey(), window);
}

void GroupWindowAggregator::WindowContext::onMerge(
    TimeWindow newWindow,
    std::vector<TimeWindow>& /*mergedWindows*/) {
  owner_->triggerContext_->onMerge(currentKey(), newWindow);
}

void GroupWindowAggregator::WindowContext::deleteCleanupTimer(
    TimeWindow window) {
  int64_t cleanupTime = TimeWindowUtil::toEpochMillsForTimer(
      TimeWindowUtil::cleanupTime(
          window.maxTimestamp(),
          owner_->node_->allowedLateness(),
          owner_->windowAssigner_->isEventTime()),
      getShiftTimeZone());
  if (cleanupTime == INT64_MAX) {
    // no need to clean up because we didn't set one
    return;
  }
  if (owner_->windowAssigner_->isEventTime()) {
    owner_->triggerContext_->deleteEventTimeTimer(
        currentKey(), window, cleanupTime);
  } else {
    owner_->triggerContext_->deleteProcessingTimeTimer(
        currentKey(), window, cleanupTime);
  }
}

} // namespace facebook::velox::stateful
