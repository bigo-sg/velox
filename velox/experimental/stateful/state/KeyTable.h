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

#include <algorithm>
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
#include "velox/experimental/stateful/state/KeyRowLifecycleObserver.h"
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
/// The key rows carry one trailing BIGINT column after the key columns:
/// the number of state entries referencing the row across all states of
/// the backend. The column is outside the hashers, so it never enters
/// hashing or key identity - the same [key | payload] shape as an
/// aggregation row. The state tables report entry existence to this table
/// (as a KeyRowLifecycleObserver), and advance() reclaims, at a batch
/// boundary, the rows whose count is zero: probing a key alone never leaks
/// a row past the boundary, and removing the last entry of a key retires
/// the row at the next one.
///
/// Owned by the state backend for its lifetime. KeySelectors bind to it
/// through KeyIndex and hold only per-client probe scratch; restore probes
/// through probeKey() on the table's private lookup, so it never disturbs
/// any client's results.
class KeyTable : public KeyIndex,
                 public KeyRowLifecycleObserver<RowContainerStateKey> {
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
    // The rows are the key columns plus the trailing BIGINT entry-refcount
    // column (see the class comment): a dependent column, hashed by none of
    // the hashers. Built through the full constructor because the
    // aggregation factory takes Accumulators, not plain payload types.
    hashTable_ = std::make_unique<exec::HashTable<false>>(
        std::move(probeHashers),
        std::vector<exec::Accumulator>{}, // no accumulators
        std::vector<TypePtr>{BIGINT()}, // the trailing refcount column
        /*allowDuplicates=*/false,
        /*isJoinBuild=*/false,
        /*hasProbedFlag=*/false,
        /*minTableSizeForParallelJoinBuild=*/0,
        pool_);
    refcountOffset_ = hashTable_->rows()
                          ->columnAt(static_cast<int32_t>(keyTypes_.size()))
                          .offset();
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
    registerNewRows(lookup);
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
    registerNewRows(*probeKeyLookup_);
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
    // HashTable::erase unlinks the row from the hash structure and frees
    // the container row (RowContainer::eraseRows) in one call; erasing the
    // row separately here would double free it.
    hashTable_->erase(folly::Range<char**>(&row, 1));
  }

  /// The lifecycle surface the backend's state tables report entry
  /// existence to; it is this table, which owns the refcount column.
  KeyRowLifecycleObserver<RowContainerStateKey>* keyLifecycle() {
    return this;
  }

  // KeyRowLifecycleObserver<RowContainerStateKey>:
  void onEntryAdded(const RowContainerStateKey& key) override {
    ++refCount(key.row());
  }

  void onEntryRemoved(const RowContainerStateKey& key) override {
    auto& count = refCount(key.row());
    VELOX_CHECK_GT(
        count,
        0,
        "Key row refcount underflow: an entry was removed without a matching add");
    if (--count == 0) {
      dirtyRows_.push_back(const_cast<char*>(key.row()));
    }
  }

  /// Batch-boundary key reclamation. A key row is erased - from the hash
  /// table and the row container - when no state entry references it any
  /// more. Rows become candidates at birth and at every zero transition of
  /// their refcount; here the candidates are deduplicated first (the same
  /// row can appear several times: its birth plus repeated zero
  /// transitions - erasing a row twice would be a double free) and then
  /// re-verified, so a row that went to zero and back within the batch
  /// (removed, then re-added) survives. Cached probe results are
  /// invalidated, as a probe would invalidate them.
  void advance() {
    std::sort(dirtyRows_.begin(), dirtyRows_.end());
    dirtyRows_.erase(
        std::unique(dirtyRows_.begin(), dirtyRows_.end()), dirtyRows_.end());
    for (char* row : dirtyRows_) {
      if (refCount(row) == 0) {
        eraseKey(row);
      }
    }
    dirtyRows_.clear();
  }

 private:
  // A freshly inserted key row starts with a zero entry refcount and goes
  // on the dirty list at birth: a key that is probed but never written is
  // reclaimable at the next boundary, exactly like a key whose entries
  // were all removed. groupProbe leaves the new row uninitialized apart
  // from the stored key columns.
  void registerNewRows(const exec::HashLookup& lookup) {
    for (const auto rowNumber : lookup.newGroups) {
      char* row = lookup.hits[rowNumber];
      refCount(row) = 0;
      dirtyRows_.push_back(row);
    }
  }

  // Direct access to the trailing refcount column of a key row; the column
  // has no null semantics, it is always a live int64. The key view hands out
  // a const row pointer (StateKey); the row memory is this table's own and
  // mutable - every probe hands out the same row as char*.
  int64_t& refCount(const char* row) {
    return *reinterpret_cast<int64_t*>(
        const_cast<char*>(row) + refcountOffset_);
  }

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

  // Key-row reclamation state: the offset of the trailing refcount column
  // and the rows that may be unreferenced at the next advance() (born rows
  // and rows whose refcount dropped to zero).
  int32_t refcountOffset_{0};
  std::vector<char*> dirtyRows_;
};

} // namespace facebook::velox::stateful
