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

#include "velox/experimental/stateful/LocalWindowAggregator.h"

#include <cstdint>
#include <vector>

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregationMasks.h"
#include "velox/experimental/stateful/EmptyOperator.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/WindowAggregator.h"
#include "velox/experimental/stateful/agg/AggregateInfos.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

LocalWindowAggregator::LocalWindowAggregator(
    std::vector<StatefulOperatorPtr> targets,
    std::unique_ptr<WindowSliceAssigner> sliceAssigner,
    std::shared_ptr<const StreamWindowAggregationNode> node,
    exec::DriverCtx* driverCtx)
    : StatefulOperator(
          std::make_unique<EmptyOperator>(0, driverCtx, node),
          std::move(targets)),
      node_(std::move(node)),
      sliceAssigner_(std::move(sliceAssigner)) {}

void LocalWindowAggregator::initialize() {
  VELOX_CHECK(
      node_->isEventTime(),
      "two-phase local window aggregation is event-time only (the "
      "planner rule excludes processing-time windows)");
  VELOX_CHECK_NOT_NULL(
      node_->keyedStateSpec(),
      "the local window aggregation node must carry a KeyedStateSpec "
      "(the key structure of both stages; no state backend is used "
      "locally)");
  tz_ = WindowTimezoneUtil::locateTimeZone(node_->shiftTimeZone());

  const auto* aggNode = std::dynamic_pointer_cast<const core::AggregationNode>(
                            node_->aggregation())
                            .get();
  VELOX_CHECK_NOT_NULL(
      aggNode, "StreamWindowAggregationNode must carry an AggregationNode");
  VELOX_CHECK(
      aggNode->step() == core::AggregationNode::Step::kPartial,
      "the local stage aggregation must be a PARTIAL step");
  auto inputType = std::dynamic_pointer_cast<const RowType>(
      aggNode->sources()[0]->outputType());
  VELOX_CHECK_NOT_NULL(
      inputType, "AggregationNode input type must be a RowType");
  declaredInputType_ = inputType;

  const auto& spec = *node_->keyedStateSpec();
  // Keyless windows: gluten appends a constant BIGINT "$keyless_key"
  // output carries no key column (Flink's local output is
  keyless_ = spec.keyChannels().size() == 1 &&
      std::string(inputType->nameOf(spec.keyChannels()[0])) == "$keyless_key";
  if (keyless_) {
    VELOX_UNSUPPORTED(
        "LocalWindowAggregator does not support keyless windows (empty "
        "grouping); they are rejected at plan translation");
  }
  keyTable_ =
      std::make_unique<KeyTable>(spec.keyTypes(), INT_MAX, memoryPool());
  keySelector_ = std::make_unique<KeySelector>(*keyTable_, spec.keyChannels());

  // The local node's output = [key columns, intermediate aggregate
  const int32_t numLeading = static_cast<int32_t>(spec.keyChannels().size());
  aggregates_ = makeAggregateInfos(
      aggNode->aggregates(),
      inputType,
      node_->outputType(),
      numLeading,
      op()->operatorCtx()->driverCtx()->queryConfig(),
      memoryPool(),
      core::AggregationNode::Step::kPartial);

  // Accumulator rows: aggregates only - the same layout as the global
  std::vector<exec::Accumulator> accumulators;
  accumulators.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    accumulators.emplace_back(
        aggregate.function.get(), aggregate.intermediateType);
  }
  accRows_ = std::make_unique<exec::RowContainer>(
      std::vector<TypePtr>{},
      true,
      std::move(accumulators),
      std::vector<TypePtr>{},
      false,
      false,
      false,
      false,
      memoryPool());
  auto* rows = accRows_.get();
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto& function = aggregates_[i].function;
    function->setAllocator(&rows->stringAllocator());
    const auto rowColumn = rows->columnAt(static_cast<int32_t>(i));
    function->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rowColumn.initializedByte(),
        rowColumn.initializedMask(),
        rows->rowSizeOffset());
  }
}

void LocalWindowAggregator::addInput(StreamElementPtr input) {
  VELOX_CHECK(!input_, "Last input has not been processed");
  auto record = std::static_pointer_cast<StreamRecord>(input);
  input_ = record->record();
}

char* LocalWindowAggregator::accumulatorRow(
    const char* keyRow,
    int64_t sliceEnd) {
  auto& sliceEntries = entries_[sliceEnd];
  auto it = sliceEntries.find(keyRow);
  if (it != sliceEntries.end()) {
    return it->second;
  }
  char* row = accRows_->newRow();
  std::vector<vector_size_t> indices{0};
  for (auto& aggregate : aggregates_) {
    aggregate.function->initializeNewGroups(
        &row, folly::Range<const vector_size_t*>(indices.data(), 1));
  }
  sliceEntries.emplace(keyRow, row);
  return row;
}

void LocalWindowAggregator::advance() {
  if (!input_) {
    return;
  }
  const auto numRows = input_->size();
  if (numRows == 0) {
    input_.reset();
    return;
  }

  auto sliceEndToData = sliceAssigner_->assignSliceEnd(input_);
  for (const auto& [sliceEnd, data] : sliceEndToData) {
    const auto sliceNumRows = data->size();
    keySelector_->probe(data);
    const auto distinctKeys = keySelector_->distinctKeys();
    const auto perRowKeys = keySelector_->keys();
    rowToDistinct_.clear();
    for (vector_size_t d = 0;
         d < static_cast<vector_size_t>(distinctKeys.size());
         ++d) {
      rowToDistinct_[distinctKeys[d].row()] = d;
    }

    // Resolve the accumulator row per input row (creating on first
    rowGroups_.resize(sliceNumRows);
    for (vector_size_t row = 0; row < sliceNumRows; ++row) {
      rowGroups_[row] = accumulatorRow(perRowKeys[row].row(), sliceEnd);
    }

    const SelectivityVector allRows(sliceNumRows);
    std::vector<std::optional<column_index_t>> maskChannels;
    maskChannels.reserve(aggregates_.size());
    for (const auto& aggregate : aggregates_) {
      maskChannels.push_back(aggregate.mask);
    }
    exec::AggregationMasks masks(std::move(maskChannels));
    masks.addInput(data, allRows);
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      auto& aggregate = aggregates_[i];
      std::vector<VectorPtr> args;
      args.reserve(aggregate.inputs.size());
      for (size_t j = 0; j < aggregate.inputs.size(); ++j) {
        if (aggregate.constantInputs[j] != nullptr) {
          args.push_back(aggregate.constantInputs[j]);
        } else {
          args.push_back(data->childAt(aggregate.inputs[j]));
        }
      }
      const auto* activeRows = masks.activeRows(i);
      aggregate.function->addRawInput(
          rowGroups_.data(),
          activeRows != nullptr ? *activeRows : allRows,
          args,
          false /* mayPushdown */);
    }
  }
  input_.reset();
}

void LocalWindowAggregator::flushFiredSlices(int64_t progress) {
  if (entries_.empty()) {
    return;
  }
  const int64_t minSliceEnd = entries_.begin()->first;
  if (WindowTimezoneUtil::isWindowFired(minSliceEnd, progress, tz_)) {
    flushAll();
  }
}

void LocalWindowAggregator::flushAll() {
  if (entries_.empty()) {
    return;
  }
  const auto& outputType = node_->outputType();
  const int32_t numKeyChannels =
      static_cast<int32_t>(node_->keyedStateSpec()->keyChannels().size());
  const int32_t outputKeyColumns = numKeyChannels;
  const int32_t numOutputCols = static_cast<int32_t>(outputType->size());
  VELOX_CHECK_EQ(
      numOutputCols,
      outputKeyColumns + static_cast<int32_t>(aggregates_.size()) + 1,
      "local window aggregation output must be [keys, partials, windowEnd]");

  // Gather every buffered (key, sliceEnd) partial in one multi-row
  std::vector<const char*> keyRows;
  std::vector<char*> accRows;
  std::vector<int64_t> sliceEnds;
  size_t total = 0;
  for (const auto& [sliceEnd, sliceEntries] : entries_) {
    total += sliceEntries.size();
  }
  keyRows.reserve(total);
  accRows.reserve(total);
  sliceEnds.reserve(total);
  std::vector<vector_size_t> which;
  which.reserve(total);
  for (const auto& [sliceEnd, sliceEntries] : entries_) {
    for (const auto& [keyRow, row] : sliceEntries) {
      keyRows.push_back(keyRow);
      accRows.push_back(row);
      sliceEnds.push_back(sliceEnd);
      which.push_back(static_cast<vector_size_t>(which.size()));
    }
  }
  const auto numRows = static_cast<vector_size_t>(total);
  const folly::Range<const vector_size_t*> allRows(which.data(), which.size());

  std::vector<VectorPtr> children(numOutputCols);
  auto* keyContainer = keySelector_->keyRowContainer();
  for (int32_t k = 0; k < outputKeyColumns; ++k) {
    children[k] =
        BaseVector::create(outputType->childAt(k), numRows, memoryPool());
    keyContainer->extractColumn(keyRows.data(), allRows, k, 0, children[k]);
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const int32_t column = outputKeyColumns + static_cast<int32_t>(i);
    children[column] =
        BaseVector::create(outputType->childAt(column), numRows, memoryPool());
    aggregates_[i].function->extractAccumulators(
        accRows.data(), numRows, &children[column]);
  }
  auto windowEnds = BaseVector::create(BIGINT(), numRows, memoryPool());
  if (auto* flat = windowEnds->asFlatVector<int64_t>()) {
    for (vector_size_t row = 0; row < numRows; ++row) {
      flat->set(row, sliceEnds[row]);
    }
  }
  children[numOutputCols - 1] = windowEnds;

  auto output = std::make_shared<RowVector>(
      memoryPool(), outputType, nullptr, numRows, std::move(children));
  pushOutput(std::make_shared<StreamRecord>(getPlanNodeId(), output));

  accRows_->eraseRows(folly::Range<char**>(accRows.data(), accRows.size()));
  entries_.clear();
}

void LocalWindowAggregator::close() {
  // Bounded-input tail flush (Flink's operator closes with a MAX
  flushAll();
  StatefulOperator::close();
  input_.reset();
  aggregates_.clear();
  keySelector_.reset();
  keyTable_.reset();
  accRows_.reset();
  entries_.clear();
}

} // namespace facebook::velox::stateful
