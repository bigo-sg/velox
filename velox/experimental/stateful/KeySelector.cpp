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

#include <optional>

#include "velox/exec/HashTable.h"
#include "velox/experimental/stateful/window/WindowPartitionFunction.h"

namespace facebook::velox::stateful {

KeySelector::~KeySelector() = default;

KeySelector::KeySelector(
    KeyIndex& keyIndex,
    std::vector<column_index_t> keyChannels)
    : keyIndex_(&keyIndex), keyChannels_(std::move(keyChannels)) {
  VELOX_CHECK(!keyChannels_.empty(), "KeySelector requires a key channel");
  VELOX_CHECK_EQ(
      keyChannels_.size(),
      keyIndex.keySchema()->keyTypes().size(),
      "One key channel per key column of the bound key table is required");
  // This selector's own probe scratch; it references the table's hashers,
  // so it is created by the table and dies with this selector.
  lookup_ = keyIndex.newLookup();
}

void KeySelector::probe(const RowVectorPtr& input) {
  const auto numInput = input->size();
  lastNumInput_ = numInput;
  distinctValid_ = false;

  if (numInput == 0) {
    stableHashes_.resize(0);
    keys_.clear();
    probed_ = true;
    return;
  }

  // Key-only view of the input: the bound table's hashers read sequential
  // channels, so every client feeds the same shape.
  std::vector<VectorPtr> keyChildren;
  keyChildren.reserve(keyChannels_.size());
  for (auto channel : keyChannels_) {
    // Loaded through the shared owner: the view's children must be
    // shared_ptr, and a lazy child must be loaded before the probe.
    keyChildren.push_back(
        BaseVector::loadedVectorShared(input->childAt(channel)));
  }
  keyIndex_->probe(std::move(keyChildren), *lookup_, stableHashes_);

  keys_.clear();
  keys_.reserve(numInput);
  for (vector_size_t row = 0; row < numInput; ++row) {
    keys_.emplace_back(
        keyIndex_->keySchema(),
        lookup_->hits[row],
        stableHashes_[row],
        keyIndex_->maxParallelism());
  }
  probed_ = true;
}

std::shared_ptr<RowContainerStateKeySerializer> KeySelector::keySerializer() {
  return keyIndex_->keySerializer();
}

folly::Range<const RowContainerStateKey*> KeySelector::keys() const {
  VELOX_CHECK(probed_, "probe() must be called before keys()");
  return folly::Range<const RowContainerStateKey*>(keys_.data(), keys_.size());
}

folly::Range<const RowContainerStateKey*> KeySelector::distinctKeys() const {
  ensureDistinct();
  return folly::Range<const RowContainerStateKey*>(
      distinctKeys_.data(), distinctKeys_.size());
}

const std::vector<SelectivityVector>& KeySelector::groupRows() const {
  ensureDistinct();
  return groupRows_;
}

folly::Range<const vector_size_t*> KeySelector::newGroups() const {
  VELOX_CHECK(probed_, "probe() must be called before newGroups()");
  return folly::Range<const vector_size_t*>(
      lookup_->newGroups.data(), lookup_->newGroups.size());
}

exec::RowContainer* KeySelector::keyRowContainer() const {
  return keyIndex_->keyRows();
}

const RowContainerKeySchema* KeySelector::schema() const {
  return keyIndex_->keySchema();
}

void KeySelector::ensureDistinct() const {
  VELOX_CHECK(probed_, "probe() must be called before distinctKeys()");
  if (distinctValid_) {
    return;
  }
  distinctKeys_.clear();
  groupRows_.clear();
  rowToDistinct_.clear();
  for (vector_size_t row = 0; row < lastNumInput_; ++row) {
    // Rows with equal user keys share one group row in the key RowContainer
    // (allowDuplicates is false and row pointers are stable), so the row
    // pointer is a faithful identity for deduplication.
    char* groupRow = lookup_->hits[row];
    auto [iter, inserted] = rowToDistinct_.emplace(
        groupRow, static_cast<vector_size_t>(distinctKeys_.size()));
    if (inserted) {
      distinctKeys_.emplace_back(
          keyIndex_->keySchema(),
          groupRow,
          stableHashes_[row],
          keyIndex_->maxParallelism());
      groupRows_.emplace_back(lastNumInput_, false);
    }
    groupRows_[iter->second].setValid(row, true);
  }
  for (auto& rows : groupRows_) {
    rows.updateBounds();
  }
  distinctValid_ = true;
}

// -----------------------------------------------------------------------------
// Deprecated partition(): hash-derived partition id as key identity. Removed
// once the stateful operators migrate to probe().
// -----------------------------------------------------------------------------

KeySelector::KeySelector(
    std::unique_ptr<core::PartitionFunction> partitionFunction,
    memory::MemoryPool* pool,
    int numPartitions)
    : partitionFunction_(std::move(partitionFunction)),
      pool_(pool),
      numPartitions_(numPartitions) {}

std::map<int64_t, RowVectorPtr> KeySelector::partition(
    const RowVectorPtr& input) {
  if (numPartitions_ == 1) {
    return std::map<int64_t, RowVectorPtr>{{0, input}};
  }
  prepareForInput(input);

  // TODO: The partition function doesn't use max parallelism.
  std::vector<int64_t> partitions(input->size());
  std::optional<int64_t> res;
  auto windowPartitionFunction =
      dynamic_cast<WindowPartitionFunction*>(partitionFunction_.get());
  if (windowPartitionFunction) {
    res = windowPartitionFunction->partition(*input, partitions);
  } else {
    std::vector<uint32_t> tmpPartitions(input->size());
    std::optional<uint32_t> tmpRes =
        partitionFunction_->partition(*input, tmpPartitions);
    if (tmpRes) {
      res = static_cast<int64_t>(*tmpRes);
    }
    for (vector_size_t i = 0; i < tmpPartitions.size(); ++i) {
      partitions[i] = static_cast<int64_t>(tmpPartitions[i]);
    }
  }
  if (res) {
    // TODO: this is a optimization, as the RowVector may have be partitioned in
    // local aggregation, so need not to partition again in global agg, but need
    // to verify whether the judge condition is enough.
    return std::map<int64_t, RowVectorPtr>{{*res, input}};
  }
  const auto numInput = input->size();
  std::map<int64_t, vector_size_t> numOfKeys;
  for (auto i = 0; i < numInput; ++i) {
    if (numOfKeys.count(partitions[i]) == 0) {
      numOfKeys[partitions[i]] = 1;
    } else {
      numOfKeys[partitions[i]] = numOfKeys[partitions[i]] + 1;
    }
  }

  std::map<int64_t, BufferPtr> keyToIndexBuffers;
  std::map<int64_t, vector_size_t*> keyToRawIndices;
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
  for (auto& [key, partitionSize] : numOfKeys) {
    auto partitionData =
        wrapChildren(input, partitionSize, keyToIndexBuffers[key]);
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
    const std::map<int64_t, vector_size_t>& numOfKeys,
    std::map<int64_t, BufferPtr>& keyToIndexBuffers,
    std::map<int64_t, vector_size_t*>& keyToRawIndices) {
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
