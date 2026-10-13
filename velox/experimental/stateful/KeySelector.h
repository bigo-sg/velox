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
#include "velox/experimental/stateful/state/KeyIndex.h"
#include "velox/experimental/stateful/state/KeySerializer.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {
struct HashLookup;
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
/// The probe machinery lives on a backend-owned key table (KeyIndex): this
/// selector contributes its key channels and its per-client probe scratch
/// only, so several selectors can bind to one table without disturbing
/// each other, and restore - which probes through the table itself -
/// leaves every selector's cached results alone. The key identity exposed
/// to operators is RowContainerStateKey; the probe internals (HashLookup,
/// hits, hashes) stay private to this class and the table.
class KeySelector {
 public:
  /// Binds a selector to a backend-owned key table over the user key
  /// channels of the probe input. The key RowContainer, the probe hash
  /// table, the schema, the key serializer and maxParallelism all live on
  /// the bound table; this selector's lookup is its own probe scratch,
  /// created from the table and never shared.
  KeySelector(KeyIndex& keyIndex, std::vector<column_index_t> keyChannels);

  /// Deprecated. Kept only until the stateful operators migrate to probe();
  /// see partition().
  KeySelector(
      std::unique_ptr<core::PartitionFunction> partitionFunction,
      memory::MemoryPool* pool,
      int numPartitions = INT_MAX);

  ~KeySelector();

  /// Probes 'input' through HashTable::groupProbe. Results are cached until
  /// the next probe of this selector. Null keys form their own group (null
  /// equals null).
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
  /// Reflects this selector's own probes only: restore probes through the
  /// bound table's private door and never reaches this list.
  folly::Range<const vector_size_t*> newGroups() const;

  /// The key RowContainer of the bound table, holding one row per distinct
  /// key seen so far, with the user key columns only.
  exec::RowContainer* keyRowContainer() const;

  /// The key schema of keyRowContainer(), available from construction.
  const RowContainerKeySchema* schema() const;

  /// The key serializer over the bound table: serializes keys column-wise
  /// and, on deserialize, probes the reassembled key columns back through
  /// the table's restore door, so restored keys land in the same rows and
  /// bucket entries as runtime keys. Owned by the table (ultimately the
  /// state backend), which it must outlive; two calls return the same
  /// instance.
  std::shared_ptr<RowContainerStateKeySerializer> keySerializer();

  /// Deprecated: partitions 'input' by hash-derived partition id. Distinct
  /// keys with colliding hashes are silently merged; kept only until the
  /// operators migrate to probe().
  std::map<int64_t, RowVectorPtr> partition(const RowVectorPtr& input);

 private:
  void ensureDistinct() const;

  // Deprecated: partition()-only members, default-initialized when the
  // bound constructor is used.
  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  memory::MemoryPool* pool_;
  const int numPartitions_ = INT_MAX;

  // Fixed at construction: the bound table and this selector's key
  // channels into the probe input. Null in the deprecated partition()
  // form.
  KeyIndex* const keyIndex_ = nullptr;
  const std::vector<column_index_t> keyChannels_ = {};

  // This client's probe scratch and cached results, valid until this
  // selector's next probe. The lookup is created from the bound table (it
  // references the table's hashers) and dies with this selector.
  std::unique_ptr<exec::HashLookup> lookup_;
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
