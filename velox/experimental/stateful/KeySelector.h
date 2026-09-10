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

#include <climits>
#include <cstdint>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

#include <folly/Range.h>

#include "velox/common/memory/MemoryPool.h"
#include "velox/common/memory/RawVector.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/state/KeySerializer.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {
template <bool ignoreNullKeys>
class HashTable;
struct HashLookup;
class VectorHasher;
} // namespace facebook::velox::exec

namespace facebook::velox::stateful {

/// Groups input rows by user key on the native HashTable groupProbe path.
/// The hash is used only for bucketing; after a hash hit the exact key is
/// compared (RowContainer equals), so distinct keys never merge, not even
/// under hash collisions - the same grouping semantics as batch
/// HashAggregation. This replaces the deprecated partition() behavior that
/// used the hash-derived partition id as the key identity and silently
/// merged keys like BIGINT 391 and 32728 (both fmix64 % INT_MAX = 250707955).
///
/// The key identity exposed to operators is RowContainerStateKey; the probe
/// internals (HashLookup, hits, hashes) stay private to this class.
class KeySelector {
 public:
  /// Constructs a selector over the user key channels of the probe input.
  /// 'keyTypes' are the key column types as declared by the plan; the key
  /// RowContainer, the probe hash table, the hashers and the schema are
  /// built here, at construction, so keys can be restored (through
  /// keySerializer()) before the first input arrives. 'maxParallelism'
  /// feeds StateKey::keyGroup() = hash % maxParallelism and must be the
  /// same in every place that derives key groups (shuffle, probe, snapshot,
  /// restore).
  KeySelector(
      std::vector<column_index_t> keyChannels,
      std::vector<TypePtr> keyTypes,
      uint32_t maxParallelism,
      memory::MemoryPool* pool);

  /// Deprecated. Kept only until the stateful operators migrate to probe();
  /// see partition().
  KeySelector(
      std::unique_ptr<core::PartitionFunction> partitionFunction,
      memory::MemoryPool* pool,
      int numPartitions = INT_MAX);

  ~KeySelector();

  /// Probes 'input' through HashTable::groupProbe. Results are cached until
  /// the next probe. Null keys form their own group (null equals null).
  void probe(const RowVectorPtr& input);

  /// Usage 1 (per-row, the aggregation accumulate path): one key per input
  /// row of the last probe() input. Rows with equal user keys yield the same
  /// key row() pointer.
  folly::Range<const RowContainerStateKey*> keys() const;

  /// Usage 2 (distinct, e.g. join build / rank): the distinct keys of the
  /// last probe() input, in first-occurrence order. Lazily built on first
  /// access after a probe and cached.
  folly::Range<const RowContainerStateKey*> distinctKeys() const;

  /// Input row membership of each distinct key: groupRows()[d] selects
  /// exactly the input rows whose key is distinctKeys()[d]. Same index
  /// domain as distinctKeys(); built together with it.
  const std::vector<SelectivityVector>& groupRows() const;

  /// Row numbers into the last probe() input at which a key was seen as a
  /// new group. Empty when every key already existed before this probe.
  folly::Range<const vector_size_t*> newGroups() const;

  /// The key RowContainer holding one row per distinct key seen so far,
  /// with the user key columns only.
  exec::RowContainer* keyRowContainer() const;

  /// The key schema of keyRowContainer(), available from construction.
  const RowContainerKeySchema* schema() const;

  /// The key serializer for the state backend: serializes keys column-wise
  /// and, on deserialize, probes the reassembled key columns back through
  /// this selector, so restored keys land in the same rows and bucket
  /// entries as runtime keys. The serializer probes through this selector
  /// and must not outlive it.
  std::shared_ptr<RowContainerStateKeySerializer> keySerializer();

  /// Deprecated: partitions 'input' by hash-derived partition id. Distinct
  /// keys with colliding hashes are silently merged; kept only until the
  /// operators migrate to probe().
  std::map<int64_t, RowVectorPtr> partition(const RowVectorPtr& input);

 private:
  // Shared tail of the two probe entries: stable hash chain, groupProbe,
  // and (for probe()) the per-row key construction.
  void probeKeyInput(const RowVectorPtr& keyInput);

  // The restore door behind keySerializer(): probes one key supplied as
  // one-row key columns without touching the per-probe result caches.
  RowContainerStateKey probeKeyColumns(std::vector<VectorPtr> keyColumns);

  void ensureDistinct() const;

  // Deprecated: partition()-only members, default-initialized when the
  // probe()-side constructor is used.
  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  memory::MemoryPool* pool_;
  const int numPartitions_ = INT_MAX;

  // Fixed at construction. 'keyRowType_' is the key columns re-packed as a
  // row: the layout every groupProbe call feeds, built once here.
  const std::vector<column_index_t> keyChannels_ = {};
  const uint32_t maxParallelism_ = 0;
  const std::vector<TypePtr> keyTypes_ = {};
  const RowTypePtr keyRowType_;

  // Built at construction. 'hashTable_' is destroyed before 'lookup_',
  // which holds a reference into the table.
  std::unique_ptr<exec::HashTable<false>> hashTable_;
  std::unique_ptr<exec::HashLookup> lookup_;
  // Independent hasher set for the stable 64-bit hash chain, the same chain
  // a restored key is probed through (keySerializer()). The probe hashers
  // live inside 'hashTable_' and their output cannot be reused: its meaning
  // changes with the table's hash mode (value IDs in kArray /
  // kNormalizedKey, hashes only in kHash) and the mode changes at runtime.
  std::vector<std::unique_ptr<exec::VectorHasher>> stableHashers_;
  std::unique_ptr<RowContainerKeySchema> schema_;

  // Per-probe scratch and cached results, valid until the next probe.
  SelectivityVector activeRows_;
  raw_vector<uint64_t> stableHashes_;
  vector_size_t lastNumInput_ = 0;
  bool probed_ = false;
  std::vector<RowContainerStateKey> keys_;
  mutable std::vector<RowContainerStateKey> distinctKeys_;
  mutable std::vector<SelectivityVector> groupRows_;
  mutable std::unordered_map<const char*, vector_size_t> rowToDistinct_;
  mutable bool distinctValid_ = false;

  // Deprecated: partition() helpers.
  void prepareForInput(const RowVectorPtr& input);

  void allocateIndexBuffers(
      const std::map<int64_t, vector_size_t>& numOfKeys,
      std::map<int64_t, BufferPtr>& keyToIndexBuffers,
      std::map<int64_t, vector_size_t*>& keyToRawIndices);

  RowVectorPtr wrapChildren(
      const RowVectorPtr& input,
      vector_size_t size,
      const BufferPtr& indices);
};

} // namespace facebook::velox::stateful
