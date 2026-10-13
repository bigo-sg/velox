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

#include "velox/common/memory/RawVector.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/vector/BaseVector.h"

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
/// projection.
class KeyIndex {
 public:
  virtual ~KeyIndex() = default;

  /// Probes one batch of key columns (one VectorPtr per key column, all of
  /// equal size) through the table into the caller-owned lookup, writing
  /// the stable-hash chain output of every row into 'stableHashes'.
  /// Per-client scratch: one selector, one lookup; restore has its own
  /// door below and never runs through a client's lookup.
  virtual void probe(
      std::vector<VectorPtr> keyColumns,
      exec::HashLookup& lookup,
      raw_vector<uint64_t>& stableHashes) = 0;

  /// Single-key probe for restore: one-row key columns in, key view out,
  /// never touching any client's per-probe scratch.
  virtual RowContainerStateKey probeKey(std::vector<VectorPtr> keyColumns) = 0;

  /// The key RowContainer holding one row per distinct key seen so far,
  /// with the user key columns only.
  virtual exec::RowContainer* keyRows() = 0;

  /// The key schema of keyRows(), available from construction.
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

  /// Physically removes a key row from the table (unlinks it from the hash
  /// table and returns its memory to the container). Batch-boundary
  /// reclamation only - never between a probe and the consumption of its
  /// results.
  virtual void eraseKey(char* row) = 0;
};

} // namespace facebook::velox::stateful
