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

#include "velox/core/PlanNode.h"
#include "velox/exec/VectorHasher.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::stateful {

/// Routes each row to the partition that owns the row's key group, with
/// the same two-step assignment the keyed state backend derives key
/// groups with: hash the key columns through the same VectorHasher chain
/// the state side probes through (KeyTable's stable hash chain), fold the
/// hash into a key group (hash % maxParallelism), then map the key group
/// to a partition by contiguous ranges (keyGroup * numPartitions /
/// maxParallelism, integer division).
///
/// The second step is a range mapping, never hash % numPartitions. A
/// partition's ownership is the contiguous key-group range
/// [ceil(i * maxP / P), floor(((i + 1) * maxP - 1) / P)], and a modulo
/// would scatter those groups across the whole key-group space, so after
/// any state redistribution the records of a key and the subtask holding
/// the key's state would land apart. Ranges also keep rescale as range
/// splits and merges instead of a full reshuffle of every key.
class KeyGroupPartitionFunction : public core::PartitionFunction {
 public:
  KeyGroupPartitionFunction(
      int numPartitions,
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& keyChannels,
      uint32_t maxParallelism);

  std::optional<uint32_t> partition(
      const RowVector& input,
      std::vector<uint32_t>& partitions) override;

 private:
  const int numPartitions_;
  const uint32_t maxParallelism_;
  std::vector<std::unique_ptr<exec::VectorHasher>> hashers_;

  // Reusable memory.
  SelectivityVector rows_;
  raw_vector<uint64_t> hashes_;
};

/// Factory for KeyGroupPartitionFunction. 'maxParallelism' must be the
/// same value every other key-group derivation uses (the state backend's
/// key table, snapshots, restores), or routing and state ownership drift
/// apart; it is a spec field because create() only receives the partition
/// count.
class KeyGroupPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  KeyGroupPartitionFunctionSpec(
      RowTypePtr inputType,
      std::vector<column_index_t> keyChannels,
      uint32_t maxParallelism)
      : inputType_(std::move(inputType)),
        keyChannels_(std::move(keyChannels)),
        maxParallelism_(maxParallelism) {}

  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions,
      bool localExchange) const override;

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& obj,
      void* context);

 private:
  const RowTypePtr inputType_;
  const std::vector<column_index_t> keyChannels_;
  const uint32_t maxParallelism_;
};

} // namespace facebook::velox::stateful
