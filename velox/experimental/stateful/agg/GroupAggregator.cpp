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

#include "velox/core/Expressions.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregationMasks.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::stateful {

namespace {

// Resolves the signature-bound final result type of an aggregate call the
// same way Aggregate::intermediateType resolves the intermediate type: the
// first registered signature whose variables bind to argTypes decides.
TypePtr resolveFinalResultType(
    const std::string& name,
    const std::vector<TypePtr>& argTypes) {
  const auto signatures = exec::getAggregateFunctionSignatures(name);
  VELOX_USER_CHECK(
      signatures.has_value(), "Aggregate function not registered: {}", name);
  for (const auto& signature : signatures.value()) {
    exec::SignatureBinder binder(*signature, argTypes);
    if (binder.tryBind()) {
      auto type = binder.tryResolveReturnType();
      VELOX_USER_CHECK(
          type,
          "Cannot resolve the final result type of aggregate function {}",
          exec::toString(name, argTypes));
      return type;
    }
  }
  VELOX_USER_FAIL(
      "Aggregate function signature is not supported: {}",
      exec::toString(name, argTypes));
}

} // namespace

GroupAggregator::GroupAggregator(
    std::unique_ptr<exec::Operator> op,
    std::vector<StatefulOperatorPtr> targets,
    std::shared_ptr<const GroupAggregationNode> node)
    : StatefulOperator(std::move(op), std::move(targets)),
      node_(std::move(node)),
      numKeys_(node_->keyedStateSpec()->keyChannels().size()),
      generateUpdateBefore_(node_->aggregation()->generateUpdateBefore()) {
  VELOX_CHECK(
      !node_->aggregation()->needRetraction(),
      "GroupAggregation with retraction (needRetraction) is not supported "
      "yet");
  VELOX_CHECK_EQ(
      node_->aggregation()->outputType()->size(),
      numKeys_ + node_->aggregation()->aggregates().size(),
      "GroupAggsHandler output must be the key columns followed by one "
      "column per aggregate");
}

void GroupAggregator::initialize() {
  StatefulOperator::initialize();
  aggregates_ = makeAggregates();
}

std::vector<exec::AggregateInfo> GroupAggregator::makeAggregates() {
  const auto& handler = *node_->aggregation();
  const auto& inputType = *handler.inputType();
  const auto& outputType = *handler.outputType();
  const auto& queryConfig = op()->operatorCtx()->driverCtx()->queryConfig();

  std::vector<exec::AggregateInfo> aggregates;
  aggregates.reserve(handler.aggregates().size());
  for (auto i = 0; i < handler.aggregates().size(); ++i) {
    const auto& aggregate = handler.aggregates()[i];
    exec::AggregateInfo info;
    for (const auto& arg : aggregate.call->inputs()) {
      if (auto field =
              dynamic_cast<const core::FieldAccessTypedExpr*>(arg.get())) {
        info.inputs.push_back(inputType.getChildIdx(field->name()));
        info.constantInputs.push_back(nullptr);
      } else if (
          auto constant =
              dynamic_cast<const core::ConstantTypedExpr*>(arg.get())) {
        info.inputs.push_back(kConstantChannel);
        info.constantInputs.push_back(constant->toConstantVector(memoryPool()));
      } else {
        VELOX_NYI("Unsupported aggregate argument: {}", arg->toString());
      }
    }
    if (aggregate.mask) {
      auto maskField =
          dynamic_cast<const core::FieldAccessTypedExpr*>(aggregate.mask.get());
      VELOX_USER_CHECK(
          maskField != nullptr,
          "Only column-reference aggregation masks are supported, got: {}",
          aggregate.mask->toString());
      info.mask = inputType.getChildIdx(maskField->name());
      VELOX_USER_CHECK_EQ(
          inputType.childAt(*info.mask)->kind(),
          TypeKind::BOOLEAN,
          "Aggregation mask column '{}' must be BOOLEAN",
          maskField->name());
    }
    VELOX_CHECK(
        aggregate.sortingKeys.empty(),
        "Sorted aggregates are not supported yet");
    VELOX_CHECK(
        !aggregate.distinct, "Distinct aggregates are not supported yet");
    info.distinct = false;
    info.intermediateType = exec::Aggregate::intermediateType(
        aggregate.call->name(), aggregate.rawInputTypes);
    // The handler's output column must carry exactly the type the
    // function's signature produces; a mismatch would otherwise surface
    // later at value extraction without naming the function.
    const auto& declaredResult = outputType.childAt(numKeys_ + i);
    const auto signatureResult =
        resolveFinalResultType(aggregate.call->name(), aggregate.rawInputTypes);
    VELOX_USER_CHECK(
        *signatureResult == *declaredResult,
        "Aggregate function {} produces {}, but the handler output column "
        "declares {}",
        exec::toString(aggregate.call->name(), aggregate.rawInputTypes),
        signatureResult->toString(),
        declaredResult->toString());
    info.function = exec::Aggregate::create(
        aggregate.call->name(),
        core::AggregationNode::Step::kSingle,
        aggregate.rawInputTypes,
        outputType.childAt(numKeys_ + i),
        queryConfig);
    info.output = numKeys_ + i;
    aggregates.emplace_back(std::move(info));
  }
  return aggregates;
}

void GroupAggregator::initializeRow(char* row) {
  char* groups[1] = {row};
  const std::vector<vector_size_t> indices{0};
  for (auto& aggregate : aggregates_) {
    aggregate.function->initializeNewGroups(
        groups,
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
  const auto& parameters = stateBackend->parameters();
  const auto& spec = *node_->keyedStateSpec();
  keyedBackend_ = std::make_unique<HeapKeyedStateBackend<RowContainerStateKey>>(
      spec.keyTypes(),
      parameters->getMaxParallelism(),
      parameters->getStartKeyGroup(),
      parameters->getNumKeyGroups(),
      memoryPool());
  keySelector_ = std::make_unique<KeySelector>(
      keyedBackend_->keyIndex(), spec.keyChannels());

  std::vector<exec::Accumulator> accumulators;
  accumulators.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    accumulators.emplace_back(
        aggregate.function.get(), aggregate.intermediateType);
  }
  AggregatingStateDescriptor descriptor(
      "group-agg-accumulators",
      std::move(accumulators),
      [this](char* row) { initializeRow(row); },
      [this](char* row, const std::vector<VectorPtr>& columns) {
        mergeRow(row, columns);
      },
      memoryPool());
  state_ =
      keyedBackend_->getOrCreateAggregatingState<VoidNamespace>(descriptor);

  // Wire before the first rows() call (and before restore, which also
  // materializes rows): the aggregate functions must write the offsets the
  // value container actually laid out.
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
  }

  StatefulOperator::initializeStateBackend(stateBackend);
}

void GroupAggregator::checkInputRowKinds(const StreamRecord& record) const {
  if (record.appendOnly()) {
    return;
  }
  const auto& rowKind = *record.rowKind();
  for (vector_size_t row = 0; row < rowKind.size(); ++row) {
    VELOX_CHECK_EQ(
        rowKind.valueAt(row),
        static_cast<int8_t>(RowKind::INSERT),
        "GroupAggregator received a non-INSERT row; retraction is not "
        "supported yet");
  }
}

std::vector<VectorPtr> GroupAggregator::extractFinalValues(
    char** rows,
    vector_size_t numRows) {
  const auto& outputType = *node_->aggregation()->outputType();
  std::vector<VectorPtr> values(aggregates_.size());
  for (size_t i = 0; i < aggregates_.size(); ++i) {
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
  distinctKeyRows_.resize(numDistinct);
  for (vector_size_t d = 0; d < numDistinct; ++d) {
    rowToDistinct_[distinctKeys[d].row()] = d;
    distinctKeyRows_[d] = distinctKeys[d].row();
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
    vector_size_t numRows) {
  rowGroups_.resize(numRows);
  const auto& perRowKeys = keySelector_->keys();
  for (vector_size_t row = 0; row < numRows; ++row) {
    rowGroups_[row] = stateRows_[rowToDistinct_[perRowKeys[row].row()]];
  }
  const SelectivityVector allRows(numRows);
  std::vector<std::optional<column_index_t>> maskChannels;
  maskChannels.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    maskChannels.push_back(aggregate.mask);
  }
  exec::AggregationMasks masks(std::move(maskChannels));
  masks.addInput(inputVector, allRows);
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
    aggregate.function->addRawInput(
        rowGroups_.data(),
        activeRows != nullptr ? *activeRows : allRows,
        args,
        false);
  }
}

void GroupAggregator::planEmission(
    const std::vector<VectorPtr>& prevValues,
    const std::vector<VectorPtr>& newValues) {
  emitRowKeys_.clear();
  emitRowKinds_.clear();
  const auto numDistinct = static_cast<vector_size_t>(isNewKey_.size());
  for (vector_size_t d = 0; d < numDistinct; ++d) {
    if (isNewKey_[d]) {
      emitRowKeys_.push_back(d);
      emitRowKinds_.push_back(RowKind::INSERT);
      continue;
    }
    bool changed = false;
    for (size_t i = 0; i < aggregates_.size() && !changed; ++i) {
      changed = !prevValues[i]->equalValueAt(newValues[i].get(), d, d);
    }
    if (!changed) {
      continue;
    }
    if (generateUpdateBefore_) {
      emitRowKeys_.push_back(d);
      emitRowKinds_.push_back(RowKind::UPDATE_BEFORE);
    }
    emitRowKeys_.push_back(d);
    emitRowKinds_.push_back(RowKind::UPDATE_AFTER);
  }
}

StreamRecordPtr GroupAggregator::buildEmitRecord(
    const std::vector<VectorPtr>& prevValues,
    const std::vector<VectorPtr>& newValues) {
  const auto& outputType = node_->aggregation()->outputType();
  auto* keyRows = keySelector_->keyRowContainer();
  const auto emitCount = static_cast<vector_size_t>(emitRowKeys_.size());

  // Key columns, extracted from the key rows of the emitting keys.
  auto keyIndices =
      AlignedBuffer::allocate<vector_size_t>(emitCount, memoryPool());
  auto* rawKeyIndices = keyIndices->asMutable<vector_size_t>();
  for (vector_size_t p = 0; p < emitCount; ++p) {
    rawKeyIndices[p] = emitRowKeys_[p];
  }
  std::vector<VectorPtr> children(outputType->size());
  for (uint32_t k = 0; k < numKeys_; ++k) {
    children[k] =
        BaseVector::create(outputType->childAt(k), emitCount, memoryPool());
    keyRows->extractColumn(
        distinctKeyRows_.data(),
        folly::Range<const vector_size_t*>(
            rawKeyIndices, static_cast<size_t>(emitCount)),
        static_cast<int32_t>(k),
        0,
        children[k]);
  }

  // Aggregate columns: each row copies its image from the matching side of
  // the before/after extraction - previous values on -U rows, new values on
  // +I/+U rows.
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto column = BaseVector::create(
        outputType->childAt(numKeys_ + i), emitCount, memoryPool());
    for (vector_size_t p = 0; p < emitCount; ++p) {
      const auto& source = emitRowKinds_[p] == RowKind::UPDATE_BEFORE
          ? prevValues[i]
          : newValues[i];
      column->copy(source.get(), p, emitRowKeys_[p], 1);
    }
    children[numKeys_ + i] = std::move(column);
  }

  std::vector<int8_t> rowKinds;
  rowKinds.reserve(emitCount);
  for (const auto kind : emitRowKinds_) {
    rowKinds.push_back(static_cast<int8_t>(kind));
  }
  return std::make_shared<StreamRecord>(
      getPlanNodeId(),
      std::make_shared<RowVector>(
          memoryPool(),
          outputType,
          BufferPtr(nullptr),
          emitCount,
          std::move(children)),
      makeRowKindVector(memoryPool(), rowKinds));
}

void GroupAggregator::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  checkInputRowKinds(*record);
  const auto& inputVector = record->record();
  const auto numRows = inputVector->size();
  if (numRows == 0) {
    return;
  }

  const auto numDistinct = resolveKeys(inputVector);

  // Previous final values, before this batch accumulates.
  const auto prevValues = extractFinalValues(stateRows_.data(), numDistinct);

  accumulateInput(inputVector, numRows);

  const auto newValues = extractFinalValues(stateRows_.data(), numDistinct);
  planEmission(prevValues, newValues);

  if (!emitRowKeys_.empty()) {
    pushOutput(buildEmitRecord(prevValues, newValues));
  }

  // Batch boundary: reclaim key rows no state entry references any more.
  keyedBackend_->advance();
}

void GroupAggregator::close() {
  state_.reset();
  keyedBackend_.reset();
  keySelector_.reset();
  StatefulOperator::close();
}

} // namespace facebook::velox::stateful
