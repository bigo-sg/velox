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
#include "velox/experimental/stateful/StreamPartition.h"
#include <cstdint>
#include "velox/experimental/stateful/StatefulTask.h"
#include "velox/vector/VectorEncoding.h"

namespace facebook::velox::stateful {

StreamPartition::StreamPartition(
    std::unique_ptr<exec::Operator> op,
    const core::PartitionFunctionSpec& partitionFunctionSpec,
    int numPartitions)
    : StatefulOperator(std::move(op), {}),
      partitionFunction_(std::move(partitionFunctionSpec.create(
          numPartitions,
          /*localExchange=*/false))),
      numPartitions_(numPartitions) {
  indexBuffers_.resize(numPartitions_);
  rawIndices_.resize(numPartitions_);
}

bool StreamPartition::isFinished() {
  return false;
}

void StreamPartition::addInput(StreamElementPtr input) {
  VELOX_CHECK_NULL(inputRowVector_);
  VELOX_CHECK_NULL(inputRowKind_);
  auto record = std::static_pointer_cast<StreamRecord>(input);
  inputRowVector_ = record->record();
  inputRowKind_ = record->rowKind();
}

void StreamPartition::advance() {
  prepareForInput(inputRowVector_);

  if (numPartitions_ == 1) {
    pushToTask(std::make_shared<StreamRecord>(
        getPlanNodeId(), 0, inputRowVector_, inputRowKind_));
    inputRowVector_.reset();
    inputRowKind_.reset();
    return;
  }

  // TODO: The partition function doesn't use max parallelism.
  partitionFunction_->partition(*inputRowVector_, partitions_);
  const auto numInput = inputRowVector_->size();
  std::vector<vector_size_t> maxIndex(numPartitions_, 0);
  for (auto i = 0; i < numInput; ++i) {
    ++maxIndex[partitions_[i]];
  }
  allocateIndexBuffers(maxIndex);

  std::fill(maxIndex.begin(), maxIndex.end(), 0);
  for (auto i = 0; i < numInput; ++i) {
    auto partition = partitions_[i];
    rawIndices_[partition][maxIndex[partition]] = i;
    ++maxIndex[partition];
  }

  for (auto i = 0; i < numPartitions_; i++) {
    auto partitionSize = maxIndex[i];
    if (partitionSize == 0) {
      // Do not enqueue empty partitions.
      continue;
    }
    auto [value, rowKind] = wrapForPartition(
        inputRowVector_, inputRowKind_, partitionSize, indexBuffers_[i]);
    pushToTask(std::make_shared<StreamRecord>(
        getPlanNodeId(), i, std::move(value), std::move(rowKind)));
  }
  inputRowVector_.reset();
  inputRowKind_.reset();
}

void StreamPartition::pushToTask(StreamElementPtr output) {
  auto task = std::static_pointer_cast<StatefulTask>(
      op()->operatorCtx()->driverCtx()->task);
  task->addOutput(std::move(output));
}

// prepareForInput and allocateIndexBuffers are adapted from LocalPartition.cpp.
void StreamPartition::prepareForInput(RowVectorPtr& input) {
  // Lazy vectors must be loaded or processed to ensure the late materialized in
  // order.
  for (auto& child : input->children()) {
    child->loadedVector();
  }
}

void StreamPartition::allocateIndexBuffers(
    const std::vector<vector_size_t>& sizes) {
  VELOX_CHECK_EQ(indexBuffers_.size(), sizes.size());
  VELOX_CHECK_EQ(rawIndices_.size(), sizes.size());

  for (auto i = 0; i < sizes.size(); ++i) {
    const auto indicesBufferBytes = sizes[i] * sizeof(vector_size_t);
    if ((indexBuffers_[i] == nullptr) ||
        (indexBuffers_[i]->capacity() < indicesBufferBytes) ||
        !indexBuffers_[i]->unique()) {
      indexBuffers_[i] = allocateIndices(sizes[i], op()->pool());
    } else {
      const auto indicesBufferBytes = sizes[i] * sizeof(vector_size_t);
      indexBuffers_[i]->setSize(indicesBufferBytes);
    }
    rawIndices_[i] = indexBuffers_[i]->asMutable<vector_size_t>();
  }
}

std::pair<RowVectorPtr, SimpleVectorPtr<int8_t>>
StreamPartition::wrapForPartition(
    const RowVectorPtr& value,
    const SimpleVectorPtr<int8_t>& rowKind,
    vector_size_t size,
    const BufferPtr& indices) {
  RowVectorPtr wrappedValue = std::make_shared<RowVector>(
      op()->pool(),
      value->type(),
      nullptr,
      size,
      std::vector<VectorPtr>(value->childrenSize()));
  for (auto i = 0; i < value->childrenSize(); ++i) {
    wrappedValue->childAt(i) =
        BaseVector::wrapInDictionary(nullptr, indices, size, value->childAt(i));
  }
  wrappedValue->updateContainsLazyNotLoaded();

  if (rowKind == nullptr) {
    return {std::move(wrappedValue), nullptr};
  }

  // Re-apply the same indices to rowKind so each partition carries the original
  // row kind for its rows. wrapInDictionary may return a DictionaryVector or,
  // for constant input, a ConstantVector; both inherit from SimpleVector.
  auto wrapped = BaseVector::wrapInDictionary(nullptr, indices, size, rowKind);
  auto wrappedRowKind =
      std::dynamic_pointer_cast<SimpleVector<int8_t>>(wrapped);
  VELOX_CHECK_NOT_NULL(
      wrappedRowKind,
      "wrapInDictionary unexpectedly returned a non-SimpleVector encoding for the rowKind column: {}",
      wrapped == nullptr
          ? std::string{"null"}
          : VectorEncoding::mapSimpleToName(wrapped->encoding()));
  return {std::move(wrappedValue), std::move(wrappedRowKind)};
}

} // namespace facebook::velox::stateful
