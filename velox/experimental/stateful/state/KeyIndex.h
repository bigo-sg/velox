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
#include <vector>

#include <folly/Range.h>

#include "velox/common/memory/RawVector.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {
struct HashLookup;
} // namespace facebook::velox::exec

namespace facebook::velox::stateful {

class RowContainerStateKeySerializer;

/// Abstract index over the backend-owned key table. A KeySelector binds to
/// this and contributes only its key channels and its per-client probe
/// scratch; the key container, the probe machinery, the key serializer and
/// maxParallelism live on the table. The heap backend implements it over
/// the two-layer structure (key table + per-state StateMaps); a future
/// single-table form would implement the same surface over its key-prefix
/// projection. Keeping the keys in a table of their own is what the states
/// count on: every state of the backend, and restore, addresses entries
/// through this table's key views, so a state cannot fold its values into
/// the key rows (the [key | accumulators] shape of HashAggregation); the
/// key data reaches operators through extractKeyColumn() instead of the
/// container itself.
class KeyIndex {
 public:
  virtual ~KeyIndex() = default;

  /// Probes one batch of key columns (one VectorPtr per key column, all of
  /// size 'numInput') through the table into the caller-owned lookup,
  /// writing the stable-hash chain output of every row into 'stableHashes'.
  /// Per-client scratch: one selector, one lookup; restore has its own
  /// door below and never runs through a client's lookup. With an empty
  /// key set (global state) the key columns are empty and every row is a
  /// direct hit on the table's one key row.
  virtual void probe(
      vector_size_t numInput,
      std::vector<VectorPtr> keyColumns,
      exec::HashLookup& lookup,
      raw_vector<uint64_t>& stableHashes) = 0;

  /// Single-key probe for restore: one-row key columns in, key view out,
  /// never touching any client's per-probe scratch.
  virtual RowContainerStateKey probeKey(std::vector<VectorPtr> keyColumns) = 0;

  /// Copies one key column of the given keys into 'result', in the shape
  /// of RowContainer::extractColumn: for every i, result[i + resultOffset]
  /// receives key column 'keyColumn' of keys[rowNumbers[i]] (positions may
  /// repeat or arrive out of order; a negative position writes null). This
  /// is how operators read key values back - the key rows themselves stay
  /// behind the index. An index with an empty key set has nothing to read
  /// back and leaves 'result' alone.
  virtual void extractKeyColumn(
      folly::Range<const RowContainerStateKey*> keys,
      folly::Range<const vector_size_t*> rowNumbers,
      column_index_t keyColumn,
      vector_size_t resultOffset,
      const VectorPtr& result) = 0;

  /// The key schema of the key rows, available from construction.
  virtual const RowContainerKeySchema* keySchema() const = 0;

  /// Defines the key-group space, hash % maxParallelism, and stays stable
  /// across rescale. The table is its single source: every place that
  /// derives key groups reads it from here.
  virtual uint32_t maxParallelism() const = 0;

  /// Serializer over this table; owned here, so its lifetime is the table
  /// owner's (the state backend).
  virtual std::shared_ptr<RowContainerStateKeySerializer> keySerializer() = 0;

  /// A fresh lookup bound to the table's hashers, the scratch a bound
  /// client (KeySelector) probes through.
  virtual std::unique_ptr<exec::HashLookup> newLookup() = 0;
};

} // namespace facebook::velox::stateful
