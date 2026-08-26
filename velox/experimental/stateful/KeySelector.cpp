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
#include "velox/exec/VectorHasher.h"
#include "velox/experimental/stateful/window/WindowPartitionFunction.h"

namespace facebook::velox::stateful {

KeySelector::~KeySelector() = default;

KeySelector::KeySelector(
    std::vector<column_index_t> keyChannels,
    uint32_t maxParallelism,
    memory::MemoryPool* pool)
    : pool_(pool),
      keyChannels_(std::move(keyChannels)),
      maxParallelism_(maxParallelism),
      stableHashes_(pool) {
  VELOX_CHECK(!keyChannels_.empty(), "KeySelector requires a key channel");
  VELOX_CHECK_GT(maxParallelism_, 0, "maxParallelism must be positive");
}

void KeySelector::createInternal(const RowVectorPtr& input) {
  auto rowType = asRowType(input->type());
  VELOX_CHECK_NOT_NULL(
      rowType.get(), "KeySelector probe input must be a row vector");
  keyTypes_.reserve(keyChannels_.size());
  for (auto channel : keyChannels_) {
    VELOX_CHECK_LT(
        channel, rowType->size(), "Key channel {} out of range", channel);
    keyTypes_.push_back(rowType->childAt(channel));
  }

  // The probe hashers are handed to the table and follow its internal hash
  // mode evolution (kArray / kNormalizedKey / kHash).
  std::vector<std::unique_ptr<exec::VectorHasher>> probeHashers;
  probeHashers.reserve(keyChannels_.size());
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    probeHashers.push_back(
        exec::VectorHasher::create(keyTypes_[i], keyChannels_[i]));
  }
  hashTable_ = exec::HashTable<false>::createForAggregation(
      std::move(probeHashers), {}, pool_);
  // The lookup references the table's hashers (the GroupingSet wiring), so
  // 'hashTable_' must outlive 'lookup_'.
  lookup_ = std::make_unique<exec::HashLookup>(hashTable_->hashers(), pool_);

  for (auto i = 0; i < keyChannels_.size(); ++i) {
    stableHashers_.push_back(
        exec::VectorHasher::create(keyTypes_[i], keyChannels_[i]));
  }
  schema_ = std::make_unique<RowContainerKeySchema>(hashTable_->rows(), keyTypes_);
}

void KeySelector::probe(const RowVectorPtr& input) {
  if (FOLLY_UNLIKELY(!hashTable_)) {
    createInternal(input);
  }
  const auto numInput = input->size();
  lastNumInput_ = numInput;
  distinctValid_ = false;

  if (numInput == 0) {
    stableHashes_.resize(0);
    keys_.clear();
    probed_ = true;
    return;
  }
  activeRows_ = SelectivityVector(numInput);
  stableHashes_.resize(numInput);

  // Stable hash chain, independent of the table's hash mode: identical to
  // the chain recomputed on restore (RowContainerStateKeySerializer), so
  // hash and keyGroup agree bit for bit between probe time and restore
  // time. This is deliberately a second hasher set; the table's hash
  // values cannot be reused because their meaning changes with the table's
  // hash mode.
  for (auto i = 0; i < stableHashers_.size(); ++i) {
    auto& hasher = *stableHashers_[i];
    auto key = input->childAt(hasher.channel())->loadedVector();
    hasher.decode(*key, activeRows_);
    hasher.hash(activeRows_, i > 0, stableHashes_);
  }

  hashTable_->prepareForGroupProbe(
      *lookup_,
      input,
      activeRows_,
      exec::BaseHashTable::kNoSpillInputStartPartitionBit);
  // Null keys probe like any other key and form their own group
  // (HashTable<false>), so lookup_->rows is never empty here for non-empty
  // input.
  hashTable_->groupProbe(
      *lookup_, exec::BaseHashTable::kNoSpillInputStartPartitionBit);

  keys_.clear();
  keys_.reserve(numInput);
  for (vector_size_t row = 0; row < numInput; ++row) {
    keys_.emplace_back(
        schema_.get(), lookup_->hits[row], stableHashes_[row], maxParallelism_);
  }
  probed_ = true;
}

folly::Range<const RowContainerStateKey*> KeySelector::keys() const {
  VELOX_CHECK(probed_, "probe() must be called before keys()");
  return folly::Range<const RowContainerStateKey*>(
      keys_.data(), keys_.size());
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
  VELOX_CHECK_NOT_NULL(
      hashTable_, "probe() must be called before keyRowContainer()");
  return hashTable_->rows();
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
          schema_.get(), groupRow, stableHashes_[row], maxParallelism_);
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
      numPartitions_(numPartitions),
      stableHashes_(pool) {}

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
