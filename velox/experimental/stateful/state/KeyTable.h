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
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <folly/Range.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/memory/RawVector.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/VectorHasher.h"
#include "velox/experimental/stateful/state/KeyIndex.h"
#include "velox/experimental/stateful/state/KeySerializer.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::stateful {

/// The heap backend's key table: one native hash table groupProbe path
/// over the user key columns, the stable hash chain that defines key
/// identity (hash / key group), the key RowContainer, and the key
/// serializer over it. The hash is used only for bucketing; after a hash
/// hit the exact key is compared (RowContainer equals), so distinct keys
/// never merge, not even under hash collisions - the same grouping
/// semantics as batch HashAggregation.
///
/// Owned by the state backend for its lifetime. KeySelectors bind to it
/// through KeyIndex and hold only per-client probe scratch; restore probes
/// through probeKey() on the table's private lookup, so it never disturbs
/// any client's results.
class KeyTable : public KeyIndex {
 public:
  /// 'keyTypes' are the key column types as declared by the plan; the
  /// container, the hash table, the hashers and the schema are built here,
  /// at construction, so keys can be restored (through keySerializer())
  /// before the first input arrives. 'maxParallelism' feeds
  /// StateKey::keyGroup() = hash % maxParallelism and must be the same in
  /// every place that derives key groups (shuffle, probe, snapshot,
  /// restore).
  KeyTable(
      std::vector<TypePtr> keyTypes,
      uint32_t maxParallelism,
      memory::MemoryPool* pool)
      : keyTypes_(std::move(keyTypes)),
        maxParallelism_(maxParallelism),
        pool_(pool),
        keyRowType_(makeKeyRowType(keyTypes_)) {
    VELOX_CHECK(!keyTypes_.empty(), "A key table requires a key column");
    VELOX_CHECK_GT(maxParallelism_, 0, "maxParallelism must be positive");
    // Unsupported key column types fail here, at construction, rather than
    // midway through a probe, a comparison or a serialization.
    checkStateKeyTypes(keyTypes_);

    // The probe hashers are handed to the table and follow its internal
    // hash mode evolution (kArray / kNormalizedKey / kHash). Both hasher
    // sets read sequential channels: every probe call, runtime or restore,
    // feeds the key-only view of the input.
    std::vector<std::unique_ptr<exec::VectorHasher>> probeHashers;
    probeHashers.reserve(keyTypes_.size());
    for (auto i = 0; i < keyTypes_.size(); ++i) {
      probeHashers.push_back(exec::VectorHasher::create(keyTypes_[i], i));
    }
    hashTable_ = exec::HashTable<false>::createForAggregation(
        std::move(probeHashers), {}, pool_);
    for (auto i = 0; i < keyTypes_.size(); ++i) {
      stableHashers_.push_back(exec::VectorHasher::create(keyTypes_[i], i));
    }
    schema_ =
        std::make_unique<RowContainerKeySchema>(hashTable_->rows(), keyTypes_);
    // The serializer probes restored keys back through this table and must
    // not outlive it; ownership here makes that a structural guarantee for
    // every copy handed out.
    keySerializer_ = std::make_shared<RowContainerStateKeySerializer>(
        schema_.get(), pool_, [this](std::vector<VectorPtr> keyColumns) {
          return probeKey(std::move(keyColumns));
        });
    // The restore door's private lookup; references the table's hashers
    // (the GroupingSet wiring), so the table outlives it by declaration
    // order.
    probeKeyLookup_ =
        std::make_unique<exec::HashLookup>(hashTable_->hashers(), pool_);
  }

  // KeyIndex:
  void probe(
      std::vector<VectorPtr> keyColumns,
      exec::HashLookup& lookup,
      raw_vector<uint64_t>& stableHashes) override {
    const auto numInput = keyColumns.front()->size();
    const auto keyInput = std::make_shared<RowVector>(
        pool_,
        keyRowType_,
        BufferPtr(nullptr),
        numInput,
        std::move(keyColumns));
    SelectivityVector activeRows(numInput);
    runStableHashChain(*keyInput, activeRows, stableHashes);
    hashTable_->prepareForGroupProbe(
        lookup,
        keyInput,
        activeRows,
        exec::BaseHashTable::kNoSpillInputStartPartitionBit);
    // Null keys probe like any other key and form their own group
    // (HashTable<false>), so lookup.rows is never empty here for non-empty
    // input.
    hashTable_->groupProbe(
        lookup, exec::BaseHashTable::kNoSpillInputStartPartitionBit);
  }

  RowContainerStateKey probeKey(std::vector<VectorPtr> keyColumns) override {
    for (const auto& column : keyColumns) {
      VELOX_CHECK_EQ(1, column->size(), "probeKey probes exactly one key row");
    }
    // The restore door: one key row straight through the probe machinery
    // on this table's own lookup, without touching any client's scratch.
    const auto keyInput = std::make_shared<RowVector>(
        pool_, keyRowType_, BufferPtr(nullptr), 1, std::move(keyColumns));
    SelectivityVector rows(1);
    raw_vector<uint64_t> hashes(pool_);
    hashes.resize(1);
    runStableHashChain(*keyInput, rows, hashes);
    hashTable_->prepareForGroupProbe(
        *probeKeyLookup_,
        keyInput,
        rows,
        exec::BaseHashTable::kNoSpillInputStartPartitionBit);
    hashTable_->groupProbe(
        *probeKeyLookup_, exec::BaseHashTable::kNoSpillInputStartPartitionBit);
    return RowContainerStateKey(
        schema_.get(), probeKeyLookup_->hits[0], hashes[0], maxParallelism_);
  }

  exec::RowContainer* keyRows() override {
    return hashTable_->rows();
  }

  const RowContainerKeySchema* keySchema() const override {
    return schema_.get();
  }

  uint32_t maxParallelism() const override {
    return maxParallelism_;
  }

  std::shared_ptr<RowContainerStateKeySerializer> keySerializer() override {
    return keySerializer_;
  }

  std::unique_ptr<exec::HashLookup> newLookup() override {
    return std::make_unique<exec::HashLookup>(hashTable_->hashers(), pool_);
  }

  void eraseKey(char* row) override {
    hashTable_->erase(folly::Range<char**>(&row, 1));
    hashTable_->rows()->eraseRows(folly::Range<char**>(&row, 1));
  }

 private:
  // The key columns re-packed as a row type (c0..cN over the plan-declared
  // key types): the layout every probe call feeds.
  static RowTypePtr makeKeyRowType(const std::vector<TypePtr>& keyTypes) {
    std::vector<std::string> names;
    names.reserve(keyTypes.size());
    for (auto i = 0; i < keyTypes.size(); ++i) {
      names.push_back("c" + std::to_string(i));
    }
    return ROW(std::move(names), std::vector<TypePtr>(keyTypes));
  }

  // Stable hash chain, independent of the table's hash mode: identical to
  // the chain a restored key is probed through (keySerializer()), so hash
  // and keyGroup agree bit for bit between probe time and restore time.
  // This is deliberately a second hasher set; the table's probe hash
  // values cannot be reused because their meaning changes with the table's
  // hash mode (value IDs in kArray / kNormalizedKey, hashes only in kHash)
  // and the mode changes at runtime.
  void runStableHashChain(
      const RowVector& keyInput,
      const SelectivityVector& activeRows,
      raw_vector<uint64_t>& hashes) {
    hashes.resize(keyInput.size());
    for (auto i = 0; i < stableHashers_.size(); ++i) {
      auto& hasher = *stableHashers_[i];
      auto key = keyInput.childAt(i);
      hasher.decode(*key, activeRows);
      hasher.hash(activeRows, i > 0, hashes);
    }
  }

  const std::vector<TypePtr> keyTypes_;
  const uint32_t maxParallelism_;
  memory::MemoryPool* pool_;
  const RowTypePtr keyRowType_;

  // Built at construction. 'hashTable_' is destroyed before the lookups,
  // which hold references into the table's hashers.
  std::unique_ptr<exec::HashTable<false>> hashTable_;
  std::vector<std::unique_ptr<exec::VectorHasher>> stableHashers_;
  std::unique_ptr<RowContainerKeySchema> schema_;
  std::shared_ptr<RowContainerStateKeySerializer> keySerializer_;
  std::unique_ptr<exec::HashLookup> probeKeyLookup_;
};

} // namespace facebook::velox::stateful
