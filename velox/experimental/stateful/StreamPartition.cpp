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
#include "velox/experimental/stateful/StatefulTask.h"

namespace facebook::velox::stateful {

StreamPartition::StreamPartition(
    std::unique_ptr<exec::Operator> op,
    const core::PartitionFunctionSpec& partitionFunctionSpec,
    int numPartitions)
    : StatefulOperator(std::move(op), {}),
      partitionFunction_(std::move(partitionFunctionSpec.create(
          numPartitions_,
          /*localExchange=*/false))),
      numPartitions_(numPartitions) {
  indexBuffers_.resize(numPartitions_);
  rawIndices_.resize(numPartitions_);
}

bool StreamPartition::isFinished() {
  return false;
}

void StreamPartition::addInput(RowVectorPtr input) {
  VELOX_CHECK_NULL(input_);
  input_ = std::move(input);
}

void StreamPartition::getOutput() {
  prepareForInput(input_);

  if (numPartitions_ == 1) {
    pushToTask(std::make_shared<StreamRecord>(op()->planNodeId(), 0, input_));
    input_.reset();
    return;
  }

  // TODO: The partition function doesn't use max parallism.
  partitionFunction_->partition(*input_, partitions_);
  const auto numInput = input_->size();
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

  const int64_t totalSize = input_->retainedSize();
  for (auto i = 0; i < numPartitions_; i++) {
    auto partitionSize = maxIndex[i];
    if (partitionSize == 0) {
      // Do not enqueue empty partitions.
      continue;
    }
    auto partitionData = wrapChildren(input_, partitionSize, indexBuffers_[i]);
    pushToTask(
        std::make_shared<StreamRecord>(op()->planNodeId(), i, partitionData));
  }
  input_.reset();
}

void StreamPartition::pushToTask(StreamElementPtr output) {
  auto task = std::static_pointer_cast<StatefulTask>(
      op()->operatorCtx()->driverCtx()->task);
  task->addOutput(std::move(output));
}

// These methods are copied from LocalPartition.cpp, maybe we can refactor
// them to reuse the code in LocalPartition.cpp.
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

RowVectorPtr StreamPartition::wrapChildren(
    const RowVectorPtr& input,
    vector_size_t size,
    const BufferPtr& indices) {
  RowVectorPtr result = std::make_shared<RowVector>(
      op()->pool(),
      input->type(),
      nullptr,
      size,
      std::vector<VectorPtr>(input->childrenSize()));

  for (auto i = 0; i < input->childrenSize(); ++i) {
    auto& child = result->childAt(i);
    if (child && child->encoding() == VectorEncoding::Simple::DICTIONARY &&
        child.use_count() == 1) {
      child->BaseVector::resize(size);
      child->setWrapInfo(indices);
      child->setValueVector(input->childAt(i));
    } else {
      child = BaseVector::wrapInDictionary(
          nullptr, indices, size, input->childAt(i));
    }
  }

  result->updateContainsLazyNotLoaded();
  return result;
}

} // namespace facebook::velox::stateful
