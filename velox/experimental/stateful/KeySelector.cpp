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
#include "velox/experimental/stateful/KeySelector.h"

namespace facebook::velox::stateful {

KeySelector::KeySelector(
    std::unique_ptr<core::PartitionFunction> partitionFunction,
    memory::MemoryPool* pool,
    int numPartitions)
    : partitionFunction_(std::move(partitionFunction)),
      pool_(pool),
      numPartitions_(numPartitions) {}

std::map<int64_t, RowVectorPtr> KeySelector::partition(const RowVectorPtr& input) {
  if (numPartitions_ == 1) {
    return std::map<int64_t, RowVectorPtr>{{0, input}};
  }
  prepareForInput(input);

  // TODO: The partition function doesn't use max parallelism.
  std::vector<uint32_t> partitions(input->size());
  auto part = partitionFunction_->partition(*input, partitions);
  if (part) {
    // TODO: this is a optimization, as the RowVector may have be partitioned in
    // local aggregation, so need not to partition again in global agg, but need
    // to verify whether the judge condition is enough.
    return std::map<int64_t, RowVectorPtr>{{*part, input}};
  }
  const auto numInput = input->size();
  std::map<uint32_t, vector_size_t> numOfKeys;
  for (auto i = 0; i < numInput; ++i) {
    if (numOfKeys.count(partitions[i]) == 0) {
      numOfKeys[partitions[i]] = 1;
    } else {
      numOfKeys[partitions[i]] = numOfKeys[partitions[i]] + 1;
    }
  }

  std::map<uint32_t, BufferPtr> keyToIndexBuffers;
  std::map<uint32_t, vector_size_t*> keyToRawIndices;
  allocateIndexBuffers(numOfKeys, keyToIndexBuffers, keyToRawIndices);

  numOfKeys.clear();
  for (auto i = 0; i < numInput; ++i) {
    auto partition = partitions[i];
    int index = 0;
    if (numOfKeys.count(partition)) {
      index = numOfKeys[partition];
    }
    keyToRawIndices[partition][index] = i;
    numOfKeys[partition] = index + 1;
  }

  std::map<int64_t, RowVectorPtr> results;
  for (auto & [key, partitionSize] : numOfKeys) {
    auto partitionData = wrapChildren(input, partitionSize, keyToIndexBuffers[key]);
    results[key] = partitionData;
  }
  return results;
}

// These methods are copied from LocalPartition.cpp, maybe we can refactor
// them to reuse the code in LocalPartition.cpp.
void KeySelector::prepareForInput(const RowVectorPtr& input) {
  // Lazy vectors must be loaded or processed to ensure the late materialized in
  // order.
  for (auto& child : input->children()) {
    child->loadedVector();
  }
}

void KeySelector::allocateIndexBuffers(
    const std::map<uint32_t, vector_size_t>& numOfKeys,
    std::map<uint32_t, BufferPtr>& keyToIndexBuffers,
    std::map<uint32_t, vector_size_t*>& keyToRawIndices) {
  for (auto& [key, num] : numOfKeys) {
    keyToIndexBuffers[key] = allocateIndices(num, pool_);
    keyToRawIndices[key] = keyToIndexBuffers[key]->asMutable<vector_size_t>();
  }
}

RowVectorPtr KeySelector::wrapChildren(
    const RowVectorPtr& input,
    vector_size_t size,
    const BufferPtr& indices) {
  RowVectorPtr result = std::make_shared<RowVector>(
      pool_,
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
