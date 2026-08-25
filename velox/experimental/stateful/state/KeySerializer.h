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
#include "velox/vector/SelectivityVector.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/exec/VectorHasher.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

/// Serializes a RowContainerStateKey to / from bytes, reusing the
/// RowContainer's built-in row serialization (the same format as spilling,
/// see RowContainer::extractSerializedRows / storeSerializedRow).
///
/// deserialize() writes the key as a new row into the schema's key
/// RowContainer and recomputes the hash with the same VectorHasher chain
/// used by HashPartitionFunction and HashTable (first column mix=false,
/// following columns mix=true). This keeps keyGroup consistent with
/// shuffle-time partitioning across restarts. Used on the snapshot / restore
/// path only, not on the hot path.
class RowContainerStateKeySerializer
    : public TypeSerializer<RowContainerStateKey> {
 public:
  RowContainerStateKeySerializer(
      const RowContainerKeySchema* schema,
      uint32_t maxParallelism,
      memory::MemoryPool* pool)
      : schema_(schema), maxParallelism_(maxParallelism), pool_(pool) {
    hashers_.reserve(schema_->keyTypes().size());
    for (vector_size_t i = 0; i < schema_->keyTypes().size(); ++i) {
      hashers_.emplace_back(
          exec::VectorHasher::create(schema_->keyTypes()[i], i));
    }
  }

  std::string serialize(const RowContainerStateKey& key) override {
    auto result = BaseVector::create(VARBINARY(), 1, pool_);
    char* row = const_cast<char*>(key.row());
    schema_->container()->extractSerializedRows(
        folly::Range<char**>(&row, 1), result);
    auto* flat = result->as<FlatVector<StringView>>();
    const auto& value = flat->valueAt(0);
    return std::string(value.data(), value.size());
  }

  RowContainerStateKey deserialize(const std::string& str) override {
    auto input = BaseVector::create(VARBINARY(), 1, pool_);
    auto* flat = input->as<FlatVector<StringView>>();
    flat->set(0, StringView(str.data(), str.size()));
    char* row = schema_->container()->newRow();
    schema_->container()->storeSerializedRow(*flat, 0, row);
    return RowContainerStateKey(schema_, row, hashRow(row), maxParallelism_);
  }

 private:
  // Computes the 64-bit hash of a key row by extracting each key column
  // into a one-row vector and running the VectorHasher chain, mirroring
  // HashPartitionFunction::partition.
  uint64_t hashRow(const char* row) {
    const SelectivityVector rows(1);
    raw_vector<uint64_t> hashes(1);
    for (vector_size_t i = 0; i < schema_->keyTypes().size(); ++i) {
      auto column = BaseVector::create(schema_->keyTypes()[i], 1, pool_);
      const char* rowPtr = row;
      schema_->container()->extractColumn(&rowPtr, 1, i, column);
      hashers_[i]->decode(*column, rows);
      hashers_[i]->hash(rows, i > 0, hashes);
    }
    return hashes[0];
  }

  const RowContainerKeySchema* schema_;
  const uint32_t maxParallelism_;
  memory::MemoryPool* pool_;
  std::vector<std::unique_ptr<exec::VectorHasher>> hashers_;
};

} // namespace facebook::velox::stateful
