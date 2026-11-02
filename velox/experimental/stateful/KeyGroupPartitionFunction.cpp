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
#include "velox/experimental/stateful/KeyGroupPartitionFunction.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::stateful {

KeyGroupPartitionFunction::KeyGroupPartitionFunction(
    int numPartitions,
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    uint32_t maxParallelism)
    : numPartitions_(numPartitions), maxParallelism_(maxParallelism) {
  VELOX_CHECK_GT(maxParallelism_, 0, "maxParallelism must be positive");
  hashers_.reserve(keyChannels.size());
  for (const auto channel : keyChannels) {
    hashers_.emplace_back(
        exec::VectorHasher::create(inputType->childAt(channel), channel));
  }
}

std::optional<uint32_t> KeyGroupPartitionFunction::partition(
    const RowVector& input,
    std::vector<uint32_t>& partitions) {
  const auto size = input.size();
  rows_.resize(size);
  rows_.setAll();

  // The same per-column decode-and-hash chain the state side runs over
  // the key columns (KeyTable::runStableHashChain). The channel number is
  // not mixed into the hash value, so hashes here are bit-identical to
  // the state side's for the same key values, in the same column order.
  hashes_.resize(size);
  for (auto i = 0; i < hashers_.size(); ++i) {
    hashers_[i]->decode(*input.childAt(hashers_[i]->channel()), rows_);
    hashers_[i]->hash(rows_, i > 0, hashes_);
  }

  partitions.resize(size);
  for (auto i = 0; i < size; ++i) {
    const auto keyGroup = static_cast<uint32_t>(hashes_[i] % maxParallelism_);
    // 64-bit arithmetic: keyGroup * numPartitions can exceed 32 bits for
    // large maxParallelism. This is the ownership range mapping, the dual
    // of KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex.
    partitions[i] = static_cast<uint32_t>(
        keyGroup * static_cast<uint64_t>(numPartitions_) / maxParallelism_);
  }
  return std::nullopt;
}

std::unique_ptr<core::PartitionFunction> KeyGroupPartitionFunctionSpec::create(
    int numPartitions,
    bool localExchange) const {
  // Key-group ownership is a cross-subtask concern; there is no
  // local-exchange variant of it.
  VELOX_CHECK(!localExchange, "Key-group routing has no local-exchange mode");
  return std::make_unique<KeyGroupPartitionFunction>(
      numPartitions, inputType_, keyChannels_, maxParallelism_);
}

std::string KeyGroupPartitionFunctionSpec::toString() const {
  std::ostringstream keys;
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    if (i > 0) {
      keys << ", ";
    }
    keys << inputType_->nameOf(keyChannels_[i]);
  }
  return fmt::format(
      "KEY_GROUP({}, maxParallelism={})", keys.str(), maxParallelism_);
}

folly::dynamic KeyGroupPartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "KeyGroupPartitionFunctionSpec";
  obj["inputType"] = inputType_->serialize();
  obj["keyChannels"] = ISerializable::serialize(keyChannels_);
  obj["maxParallelism"] = maxParallelism_;
  return obj;
}

// static
core::PartitionFunctionSpecPtr KeyGroupPartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const auto keys = ISerializable::deserialize<std::vector<column_index_t>>(
      obj["keyChannels"], context);
  const auto maxParallelism =
      static_cast<uint32_t>(obj["maxParallelism"].asInt());
  return std::make_shared<KeyGroupPartitionFunctionSpec>(
      ISerializable::deserialize<RowType>(obj["inputType"]),
      keys,
      maxParallelism);
}

} // namespace facebook::velox::stateful
