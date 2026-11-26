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
#include "velox/experimental/stateful/agg/GroupAggregator.h"

#include <cstdint>

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregationMasks.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/agg/AggregateInfos.h"
#include "velox/experimental/stateful/agg/RetractableAggregate.h"
#include "velox/experimental/stateful/state/KeyedStateBackendFactory.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::stateful {

// The record-count channel of a retracting plan, mirroring Flink's
// RecordCounter (RecordCounter.java): the record count of a key is the
// count(*) accumulator column GroupAggregator reused or appended (see
// initialize), and a key is dead exactly when that count is zero - never
// when negative (RetractionRecordCounter semantics; an over-retracted key
// keeps its state).
class RecordCounter {
 public:
  RecordCounter(
      const std::vector<exec::AggregateInfo>* aggregates,
      size_t indexOfCountStar)
      : aggregates_(aggregates), indexOfCountStar_(indexOfCountStar) {}

  // Extracts the record counts of 'numRows' accumulator rows through the
  // count function's own final-value extraction.
  VectorPtr extractCounts(
      char** rows,
      vector_size_t numRows,
      memory::MemoryPool* pool) const {
    auto counts = BaseVector::create(BIGINT(), numRows, pool);
    (*aggregates_)[indexOfCountStar_].function->extractValues(
        rows, numRows, &counts);
    return counts;
  }

  // A key is dead exactly at a zero count.
  bool countIsZero(const VectorPtr& counts, vector_size_t row) const {
    return counts->as<SimpleVector<int64_t>>()->valueAt(row) == 0;
  }

 private:
  // The operator's aggregate list (stable after initialize) and the position
  // of the count column in it.
  const std::vector<exec::AggregateInfo>* aggregates_;
  const size_t indexOfCountStar_;
};

GroupAggregator::GroupAggregator(
    std::unique_ptr<exec::Operator> op,
    std::vector<StatefulOperatorPtr> targets,
    std::shared_ptr<const GroupAggregationNode> node)
    : StatefulOperator(std::move(op), std::move(targets)),
      node_(std::move(node)),
      numKeys_(node_->keyedStateSpec()->keyChannels().size()),
      generateUpdateBefore_(node_->aggregation()->generateUpdateBefore()),
      needRetraction_(node_->aggregation()->needRetraction()) {
  VELOX_CHECK_EQ(
      node_->aggregation()->outputType()->size(),
      numKeys_ + node_->aggregation()->aggregates().size(),
      "GroupAggsHandler output must be the key columns followed by one "
      "column per aggregate");
}

GroupAggregator::~GroupAggregator() = default;

void GroupAggregator::initialize() {
  StatefulOperator::initialize();
  const auto& handler = *node_->aggregation();
  aggregates_ = makeAggregateInfos(
      handler.aggregates(),
      handler.inputType(),
      handler.outputType(),
      numKeys_,
      op()->operatorCtx()->driverCtx()->queryConfig(),
      memoryPool());

  if (!needRetraction_) {
    return;
  }
  // Retraction consumes the aggregates through RetractableAggregate (see
  // applyInput); probe every family up front so an unsupported one fails at
  // initialization rather than on the first retraction row deep inside the
  // run.
  for (auto i = 0; i < aggregates_.size(); ++i) {
    // TODO: cover the remaining aggregate families with retract variants
    // (first batch: count/sum/avg/min/max); every family outside the first
    // batch fails loud here until its retract math lands.
    VELOX_CHECK_NOT_NULL(
        dynamic_cast<RetractableAggregate*>(aggregates_[i].function.get()),
        "aggregate {} does not support retraction",
        handler.aggregates()[i].call->name());
  }
  // The record-count channel decides -D (Flink: AggregateUtil
  // .insertCountStarAggCall + RecordCounter): reuse a qualifying count(*) -
  // zero-argument and unmasked, so its accumulator is exactly the key's row
  // count - or append a hidden count after the AggregateInfos construction.
  // The 1:1 plan/output contract was checked on the visible aggregates
  // above; the hidden column lives only in the accumulator row (the value
  // container and the input paths below), never in the output.
  int32_t indexOfCountStar = -1;
  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (aggregates_[i].inputs.empty() && !aggregates_[i].mask.has_value() &&
        handler.aggregates()[i].call->name() == "count") {
      indexOfCountStar = i;
      break;
    }
  }
  if (indexOfCountStar < 0) {
    exec::AggregateInfo countStar;
    countStar.intermediateType = BIGINT();
    countStar.function = exec::Aggregate::create(
        "count",
        core::AggregationNode::Step::kSingle,
        {},
        BIGINT(),
        op()->operatorCtx()->driverCtx()->queryConfig());
    aggregates_.push_back(std::move(countStar));
    indexOfCountStar = static_cast<int32_t>(aggregates_.size()) - 1;
  }
  recordCounter_ =
      std::make_unique<RecordCounter>(&aggregates_, indexOfCountStar);
}

void GroupAggregator::initializeRows(char** rows, size_t numRows) {
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

void GroupAggregator::mergeRow(
    char* row,
    const std::vector<VectorPtr>& columns) {
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i].function->addSingleGroupIntermediateResults(
        row, SelectivityVector(1), {columns[i]}, false);
  }
}

void GroupAggregator::initializeStateBackend(StateBackend* stateBackend) {
  const auto& spec = *node_->keyedStateSpec();
  auto keyedBackend = createKeyedStateBackend<RowContainerStateKey>(
      *stateBackend, spec, memoryPool());
  keySelector_ = std::make_unique<KeySelector>(
      keyedBackend->keyIndex(), spec.keyChannels());

  // The descriptor carries the aggregate functions next to their
  // accumulators: the state wires each function to its accumulator column
  // when it builds the value rows, before the first rows() call and before
  // restore, which also materializes rows.
  std::vector<exec::Aggregate*> functions;
  std::vector<exec::Accumulator> accumulators;
  functions.reserve(aggregates_.size());
  accumulators.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    functions.push_back(aggregate.function.get());
    accumulators.emplace_back(
        aggregate.function.get(), aggregate.intermediateType);
  }
  AggregatingStateDescriptor descriptor(
      "group-agg-accumulators",
      std::move(functions),
      std::move(accumulators),
      [this](char** rows, size_t numRows) { initializeRows(rows, numRows); },
      [this](char* row, const std::vector<VectorPtr>& columns) {
        mergeRow(row, columns);
      },
      memoryPool());
  state_ = keyedBackend->getOrCreateAggregatingState(
      descriptor, NamespaceSerializerTraits<VoidNamespace>::create());
  keyedBackend_ = std::move(keyedBackend);

  StatefulOperator::initializeStateBackend(stateBackend);
}

// Splits the batch rows by rowKind into the accumulate selection (+I/+U) and
// the retract selection (-U/-D), the vectorized form of Flink's
// isAccumulateMsg split; StreamRecord construction guarantees every kind is
// one of the four ordinals. An append-only record accumulates every row. A
// plan without retraction is promised insert-only input by the planner's
// contract; a retracting row there is a contract violation and fails loud,
// since its aggregates have no retract path to route the row to.
void GroupAggregator::splitByRowKind(
    const StreamRecord& record,
    SelectivityVector& accRows,
    SelectivityVector& retractRows) const {
  if (record.appendOnly()) {
    accRows.setAll();
    return;
  }
  const auto& rowKind = *record.rowKind();
  for (vector_size_t row = 0; row < rowKind.size(); ++row) {
    switch (static_cast<RowKind>(rowKind.valueAt(row))) {
      case RowKind::INSERT:
      case RowKind::UPDATE_AFTER:
        accRows.setValid(row, true);
        break;
      case RowKind::UPDATE_BEFORE:
      case RowKind::DELETE:
        VELOX_CHECK(
            needRetraction_,
            "GroupAggregation received a non-INSERT row ({}) but the plan does not ask for retraction",
            static_cast<int8_t>(rowKind.valueAt(row)));
        retractRows.setValid(row, true);
        break;
    }
  }
  accRows.updateBounds();
  retractRows.updateBounds();
}

std::vector<VectorPtr> GroupAggregator::extractFinalValues(
    char** rows,
    vector_size_t numRows) {
  const auto& outputType = *node_->aggregation()->outputType();
  // The hidden record-count column of a retracting plan has no output
  // channel; extraction covers the plan-visible aggregates only.
  const auto numVisible = node_->aggregation()->aggregates().size();
  std::vector<VectorPtr> values(numVisible);
  for (size_t i = 0; i < numVisible; ++i) {
    // extractValues requires a pre-allocated result vector, like
    // HashAggregation's output path.
    values[i] = BaseVector::create(
        outputType.childAt(numKeys_ + i), numRows, memoryPool());
    aggregates_[i].function->extractValues(rows, numRows, &values[i]);
  }
  return values;
}

vector_size_t GroupAggregator::resolveKeys(const RowVectorPtr& inputVector) {
  keySelector_->probe(inputVector);
  const auto distinctKeys = keySelector_->distinctKeys();
  const auto numDistinct = static_cast<vector_size_t>(distinctKeys.size());
  const auto perRowKeys = keySelector_->keys();

  // Key row identity -> distinct index, then the new-key marks (a key is
  // new when any of its rows entered the key table with this probe).
  rowToDistinct_.clear();
  for (vector_size_t d = 0; d < numDistinct; ++d) {
    rowToDistinct_[distinctKeys[d].row()] = d;
  }
  isNewKey_.assign(numDistinct, false);
  for (auto row : keySelector_->newGroups()) {
    isNewKey_[rowToDistinct_[perRowKeys[row].row()]] = true;
  }

  stateRows_.resize(numDistinct);
  state_->rows(
      folly::Range<const RowContainerStateKey*>(
          distinctKeys.data(), static_cast<size_t>(numDistinct)),
      VoidNamespace::instance(),
      stateRows_.data());
  return numDistinct;
}

void GroupAggregator::accumulateInput(
    const RowVectorPtr& inputVector,
    const SelectivityVector& rows) {
  applyInput(inputVector, rows, /*retract=*/false);
}

void GroupAggregator::retractInput(
    const RowVectorPtr& inputVector,
    const SelectivityVector& rows) {
  applyInput(inputVector, rows, /*retract=*/true);
}

// Shared driver of the accumulate/retract input paths: resolves every input
// row's state row into the groups array, filters 'rows' through the
// aggregation masks (the same mask construction both directions reuse), and
// hands each aggregate its selected rows - addRawInput, or retractRawInput
// through the RetractableAggregate mix-in (the initialize() probe guarantees
// the cast on a retracting plan).
void GroupAggregator::applyInput(
    const RowVectorPtr& inputVector,
    const SelectivityVector& rows,
    bool retract) {
  const auto numRows = static_cast<vector_size_t>(inputVector->size());
  rowGroups_.resize(numRows);
  const auto& perRowKeys = keySelector_->keys();
  for (vector_size_t row = 0; row < numRows; ++row) {
    rowGroups_[row] = stateRows_[rowToDistinct_[perRowKeys[row].row()]];
  }
  std::vector<std::optional<column_index_t>> maskChannels;
  maskChannels.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    maskChannels.push_back(aggregate.mask);
  }
  exec::AggregationMasks masks(std::move(maskChannels));
  masks.addInput(inputVector, rows);
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto& aggregate = aggregates_[i];
    std::vector<VectorPtr> args;
    args.reserve(aggregate.inputs.size());
    for (size_t j = 0; j < aggregate.inputs.size(); ++j) {
      if (aggregate.constantInputs[j] != nullptr) {
        args.push_back(aggregate.constantInputs[j]);
      } else {
        args.push_back(inputVector->childAt(aggregate.inputs[j]));
      }
    }
    const auto* activeRows = masks.activeRows(i);
    const auto& selected = activeRows != nullptr ? *activeRows : rows;
    if (retract) {
      dynamic_cast<RetractableAggregate*>(aggregate.function.get())
          ->retractRawInput(rowGroups_.data(), selected, args, false);
    } else {
      aggregate.function->addRawInput(rowGroups_.data(), selected, args, false);
    }
  }
}

bool GroupAggregator::valuesChanged(
    const std::vector<VectorPtr>& prevValues,
    const std::vector<VectorPtr>& newValues,
    vector_size_t d) {
  for (size_t i = 0; i < prevValues.size(); ++i) {
    if (!prevValues[i]->equalValueAt(newValues[i].get(), d, d)) {
      return true;
    }
  }
  return false;
}

// Fills the emission plan from the before/after images: the insert-only
// path keys off physical key newness, the retracting path off the record
// count (see the header comment for the combination table).
void GroupAggregator::planEmission(
    const std::vector<VectorPtr>& prevValues,
    const std::vector<VectorPtr>& newValues,
    const VectorPtr& countBefore,
    const VectorPtr& countAfter) {
  emitRowKeys_.clear();
  emitRowKinds_.clear();
  keysToClear_.clear();
  const auto emitUpdate = [&](vector_size_t d) {
    if (generateUpdateBefore_) {
      emitRowKeys_.push_back(d);
      emitRowKinds_.push_back(RowKind::UPDATE_BEFORE);
    }
    emitRowKeys_.push_back(d);
    emitRowKinds_.push_back(RowKind::UPDATE_AFTER);
  };
  const auto numDistinct = static_cast<vector_size_t>(isNewKey_.size());
  for (vector_size_t d = 0; d < numDistinct; ++d) {
    if (recordCounter_ == nullptr) {
      // Insert-only plan: a physically new key emits +I, a changed existing
      // key its update pair, unchanged keys nothing.
      if (isNewKey_[d]) {
        emitRowKeys_.push_back(d);
        emitRowKinds_.push_back(RowKind::INSERT);
      } else if (valuesChanged(prevValues, newValues, d)) {
        emitUpdate(d);
      }
      continue;
    }
    const auto after = countAfter->as<SimpleVector<int64_t>>()->valueAt(d);
    if (recordCounter_->countIsZero(countBefore, d)) {
      // A fresh accumulator (a physically new key, or one reborn after -D):
      // net-positive is a semantic insert, anything else is dropped state -
      // Flink ignores a first message that retracts, so no emission, no
      // residue, no negative count.
      if (after > 0) {
        emitRowKeys_.push_back(d);
        emitRowKinds_.push_back(RowKind::INSERT);
      } else {
        keysToClear_.push_back(d);
      }
      continue;
    }
    if (recordCounter_->countIsZero(countAfter, d)) {
      // The key died with this batch: -D carries the previous values (the
      // batch-before image), the state is dropped below.
      emitRowKeys_.push_back(d);
      emitRowKinds_.push_back(RowKind::DELETE);
      keysToClear_.push_back(d);
      continue;
    }
    // Live before and after: negative counts stay live (zero is the only
    // death), so an over-retracted key takes the plain update row.
    if (valuesChanged(prevValues, newValues, d)) {
      emitUpdate(d);
    }
  }
}

StreamRecordPtr GroupAggregator::buildEmitRecord(
    const std::vector<VectorPtr>& prevValues,
    const std::vector<VectorPtr>& newValues) {
  const auto& outputType = node_->aggregation()->outputType();
  const auto emitCount = static_cast<vector_size_t>(emitRowKeys_.size());

  // Key columns, read back from the key table through the selector. The
  // emission plan doubles as the extraction indices: one entry per emitted
  // row, naming that row's distinct key.
  const folly::Range<const vector_size_t*> emitIndices(
      emitRowKeys_.data(), static_cast<size_t>(emitCount));
  std::vector<VectorPtr> children(outputType->size());
  for (uint32_t k = 0; k < numKeys_; ++k) {
    children[k] =
        BaseVector::create(outputType->childAt(k), emitCount, memoryPool());
    keySelector_->extractKeyColumn(
        keySelector_->distinctKeys(), emitIndices, k, 0, children[k]);
  }

  // Aggregate columns: each row copies its image from the matching side of
  // the before/after extraction - previous values on -U/-D rows, new values
  // on +I/+U rows. prevValues carries the plan-visible aggregates only (the
  // hidden record-count column has no output).
  for (size_t i = 0; i < prevValues.size(); ++i) {
    auto column = BaseVector::create(
        outputType->childAt(numKeys_ + i), emitCount, memoryPool());
    for (vector_size_t p = 0; p < emitCount; ++p) {
      const auto& source = emitRowKinds_[p] == RowKind::UPDATE_BEFORE ||
              emitRowKinds_[p] == RowKind::DELETE
          ? prevValues[i]
          : newValues[i];
      column->copy(source.get(), p, emitRowKeys_[p], 1);
    }
    children[numKeys_ + i] = std::move(column);
  }

  return std::make_shared<StreamRecord>(
      getPlanNodeId(),
      std::make_shared<RowVector>(
          memoryPool(),
          outputType,
          BufferPtr(nullptr),
          emitCount,
          std::move(children)),
      makeRowKindVector(
          memoryPool(),
          folly::Range<const RowKind*>(
              emitRowKinds_.data(), static_cast<size_t>(emitCount))));
}

void GroupAggregator::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  const auto& inputVector = record->record();
  const auto numRows = inputVector->size();
  if (numRows == 0) {
    return;
  }

  const auto numDistinct = resolveKeys(inputVector);

  // The batch's before images: final values and, on a retracting plan, the
  // record counts planEmission's -D/new-key decisions read.
  const auto countBefore = recordCounter_ != nullptr
      ? recordCounter_->extractCounts(
            stateRows_.data(), numDistinct, memoryPool())
      : nullptr;
  const auto prevValues = extractFinalValues(stateRows_.data(), numDistinct);

  // The rowKind split: +I/+U rows accumulate, -U/-D rows retract (the batch
  // order does not matter - the retract-capable families' accumulator
  // algebra is commutative).
  SelectivityVector accRows(numRows, false);
  SelectivityVector retractRows(numRows, false);
  splitByRowKind(*record, accRows, retractRows);
  if (accRows.hasSelections()) {
    accumulateInput(inputVector, accRows);
  }
  if (retractRows.hasSelections()) {
    retractInput(inputVector, retractRows);
  }

  const auto newValues = extractFinalValues(stateRows_.data(), numDistinct);
  const auto countAfter = recordCounter_ != nullptr
      ? recordCounter_->extractCounts(
            stateRows_.data(), numDistinct, memoryPool())
      : nullptr;
  planEmission(prevValues, newValues, countBefore, countAfter);

  if (!emitRowKeys_.empty()) {
    pushOutput(buildEmitRecord(prevValues, newValues));
  }

  // Drop the state of the keys the emission plan retired (-D) or silently
  // cleaned (a fresh key with no net contribution): their accumulator rows -
  // the hidden count column included - return to the value container.
  for (auto d : keysToClear_) {
    state_->remove(keySelector_->distinctKeys()[d], VoidNamespace::instance());
  }

  // Batch boundary: hand the batch's state changes to the backend's storage
  // (a write-back backend stages them until here), then let it reclaim key
  // rows no state entry references any more - the retired keys' rows among
  // them. Row handles of this batch are not used past this point.
  keyedBackend_->commit();
  keyedBackend_->reclaim();
}

void GroupAggregator::close() {
  state_.reset();
  keyedBackend_.reset();
  keySelector_.reset();
  StatefulOperator::close();
}

} // namespace facebook::velox::stateful
