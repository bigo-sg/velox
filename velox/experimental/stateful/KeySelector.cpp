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
#include "velox/experimental/stateful/window/WindowPartitionFunction.h"
#include <folly/container/F14Map.h>
#include <algorithm>
#include <memory>
#include <optional>

namespace facebook::velox::stateful {

namespace {

void allocateIndexBuffers(
    const folly::F14FastMap<int64_t, vector_size_t>& partitionCounts,
    folly::F14FastMap<int64_t, BufferPtr>& keyToIndexBuffers,
    folly::F14FastMap<int64_t, vector_size_t*>& keyToRawIndices,
    memory::MemoryPool* pool) {
  keyToIndexBuffers.reserve(partitionCounts.size());
  keyToRawIndices.reserve(partitionCounts.size());
  for (const auto& [key, num] : partitionCounts) {
    keyToIndexBuffers[key] = allocateIndices(num, pool);
    keyToRawIndices[key] = keyToIndexBuffers[key]->asMutable<vector_size_t>();
  }
}

} // namespace

KeySelector::KeySelector(
    std::unique_ptr<core::PartitionFunction> partitionFunction,
    memory::MemoryPool* pool,
    int numPartitions)
    : partitionFunction_(std::move(partitionFunction)),
      pool_(pool),
      numPartitions_(numPartitions) {
  windowPartitionFunction_ =
      dynamic_cast<WindowPartitionFunction*>(partitionFunction_.get());
}

std::map<int64_t, RowVectorPtr> KeySelector::partition(const RowVectorPtr& input) {
  prepareForInput(input);
  const auto numInput = input->size();
  std::vector<int64_t> partitions(numInput);
  std::optional<int64_t> res;
  if (windowPartitionFunction_) {
    res = windowPartitionFunction_->partition(*input, partitions);
  } else {
    std::vector<uint32_t> tmpPartitions(numInput);
    std::optional<uint32_t> tmpRes =
        partitionFunction_->partition(*input, tmpPartitions);
    if (tmpRes) {
      res = static_cast<int64_t>(*tmpRes);
    }
    for (vector_size_t i = 0; i < numInput; ++i) {
      partitions[i] =static_cast<int64_t>(tmpPartitions[i]);
    }
  }
  if (res) {
    return std::map<int64_t, RowVectorPtr>{{*res, input}};
  }

  folly::F14FastMap<int64_t, vector_size_t> partitionCounts;
  partitionCounts.reserve(std::min<vector_size_t>(numInput, 4096));

  for (vector_size_t i = 0; i < numInput; ++i) {
    ++partitionCounts[partitions[i]];
  }
  folly::F14FastMap<int64_t, BufferPtr> keyToIndexBuffers;
  folly::F14FastMap<int64_t, vector_size_t*> keyToRawIndices;
  allocateIndexBuffers(partitionCounts, keyToIndexBuffers, keyToRawIndices, pool_);

  folly::F14FastMap<int64_t, vector_size_t> nextRowIndex;
  nextRowIndex.reserve(partitionCounts.size());

  for (vector_size_t i = 0; i < numInput; ++i) {
    const auto partition = partitions[i];
    const auto index = nextRowIndex[partition];
    keyToRawIndices[partition][index] = i;
    nextRowIndex[partition] = index + 1;
  }

  std::map<int64_t, RowVectorPtr> results;
  for (const auto& [partitionKey, partitionSize] : partitionCounts) {
    results.emplace(
      partitionKey, wrapChildren(input, partitionSize, keyToIndexBuffers[partitionKey]));
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
