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
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "velox/common/memory/MemoryPool.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/CheckpointStream.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

/// Serializes a RowContainerStateKey column-wise: one length-prefixed
/// component per key column, each a null flag byte followed by the per-type
/// value serializer's payload (the framing of SharedPtrSerializer). The
/// bytes carry the column values only; the key row itself is not part of
/// them, so any backend that can reassemble the columns can consume them -
/// the heap checkpoint today, the composite key of the RocksDB backend
/// later.
///
/// deserialize() does not build the key row itself: it reassembles the
/// columns into one-row vectors and hands them to 'probe', the grouping
/// entry supplied by the KeySelector that owns the key container. The row
/// comes back from the hash table, so a restored key and a runtime-probed
/// key of the same value are the same row and the same bucket entry (no
/// duplicate rows after restore), and the hash / key group are computed by
/// the same stable hasher chain as at probe time. Snapshot / restore path
/// only, not the hot path.
class RowContainerStateKeySerializer
    : public TypeSerializer<RowContainerStateKey> {
 public:
  /// Probes the one-row key columns and returns the resulting table row as
  /// a StateKey; supplied by the owning KeySelector.
  using Probe =
      std::function<RowContainerStateKey(std::vector<VectorPtr> keyColumns)>;

  RowContainerStateKeySerializer(
      const RowContainerKeySchema* schema,
      memory::MemoryPool* pool,
      Probe probe)
      : schema_(schema), pool_(pool), probe_(std::move(probe)) {
    columnSerializers_.reserve(schema_->keyTypes().size());
    for (const auto& type : schema_->keyTypes()) {
      columnSerializers_.push_back(createSerializer(type, false, pool_));
    }
  }

  std::string schema() const override {
    std::string schema;
    for (const auto& keyType : schema_->keyTypes()) {
      if (!schema.empty()) {
        schema += ",";
      }
      schema += keyType->toString();
    }
    return "(" + schema + ")";
  }

  std::string serialize(const RowContainerStateKey& key) override {
    std::string bytes;
    CheckpointWriter writer(bytes);
    for (size_t i = 0; i < columnSerializers_.size(); ++i) {
      writer.writeBytes(serializeColumn(i, key.row()));
    }
    return bytes;
  }

  RowContainerStateKey deserialize(std::string_view str) override {
    CheckpointReader reader(str.data(), str.size());
    std::vector<VectorPtr> keyColumns;
    keyColumns.reserve(schema_->keyTypes().size());
    for (size_t i = 0; i < columnSerializers_.size(); ++i) {
      auto column = BaseVector::create(schema_->keyTypes()[i], 1, pool_);
      setColumn(i, reader.readBytes(), column);
      keyColumns.push_back(std::move(column));
    }
    VELOX_CHECK(reader.atEnd(), "Corrupt checkpoint: trailing bytes in a key");
    return probe_(std::move(keyColumns));
  }

 private:
  // One key column as [null flag byte][value payload], or the flag byte
  // alone when the column is null (null equals null at probe time).
  std::string serializeColumn(size_t column, const char* row) {
    switch (schema_->keyTypes()[column]->kind()) {
#define VELOX_STATEFUL_KEY_COLUMN(KIND)               \
  case TypeKind::KIND: {                              \
    using D = TypeTraits<TypeKind::KIND>::NativeType; \
    return serializeColumnAs<D>(column, row);         \
  }
      VELOX_STATEFUL_KEY_COLUMN(BOOLEAN)
      VELOX_STATEFUL_KEY_COLUMN(TINYINT)
      VELOX_STATEFUL_KEY_COLUMN(SMALLINT)
      VELOX_STATEFUL_KEY_COLUMN(INTEGER)
      VELOX_STATEFUL_KEY_COLUMN(BIGINT)
      VELOX_STATEFUL_KEY_COLUMN(HUGEINT)
      VELOX_STATEFUL_KEY_COLUMN(REAL)
      VELOX_STATEFUL_KEY_COLUMN(DOUBLE)
      VELOX_STATEFUL_KEY_COLUMN(VARCHAR)
      VELOX_STATEFUL_KEY_COLUMN(VARBINARY)
      VELOX_STATEFUL_KEY_COLUMN(TIMESTAMP)
#undef VELOX_STATEFUL_KEY_COLUMN
      default:
        VELOX_NYI(
            "State key column type {} is not supported yet",
            schema_->keyTypes()[column]->kindName());
    }
  }

  template <typename D>
  std::string serializeColumnAs(size_t column, const char* row) {
    auto values = BaseVector::create(schema_->keyTypes()[column], 1, pool_);
    const char* rowPtr = row;
    schema_->container()->extractColumn(&rowPtr, 1, column, values);
    auto* flat = values->as<FlatVector<D>>();
    if (flat->isNullAt(0)) {
      return std::string(1, static_cast<char>(0));
    }
    if constexpr (std::is_same_v<D, Timestamp>) {
      // The raw (seconds, nanos) pair: the millis-rounded value encoding
      // would change the key identity and, with it, the hash and key group.
      static_assert(
          std::is_trivially_copyable_v<Timestamp> && sizeof(Timestamp) == 16);
      std::string bytes(1, static_cast<char>(1));
      const auto& value = flat->valueAt(0);
      bytes.append(reinterpret_cast<const char*>(&value), sizeof(Timestamp));
      return bytes;
    } else {
      auto serializer = std::static_pointer_cast<TypeSerializer<D>>(
          columnSerializers_[column]);
      return std::string(1, static_cast<char>(1)) +
          serializer->serialize(flat->valueAt(0));
    }
  }

  void
  setColumn(size_t column, std::string_view bytes, const VectorPtr& values) {
    switch (schema_->keyTypes()[column]->kind()) {
#define VELOX_STATEFUL_KEY_COLUMN(KIND)               \
  case TypeKind::KIND: {                              \
    using D = TypeTraits<TypeKind::KIND>::NativeType; \
    setColumnAs<D>(column, bytes, values);            \
    return;                                           \
  }
      VELOX_STATEFUL_KEY_COLUMN(BOOLEAN)
      VELOX_STATEFUL_KEY_COLUMN(TINYINT)
      VELOX_STATEFUL_KEY_COLUMN(SMALLINT)
      VELOX_STATEFUL_KEY_COLUMN(INTEGER)
      VELOX_STATEFUL_KEY_COLUMN(BIGINT)
      VELOX_STATEFUL_KEY_COLUMN(HUGEINT)
      VELOX_STATEFUL_KEY_COLUMN(REAL)
      VELOX_STATEFUL_KEY_COLUMN(DOUBLE)
      VELOX_STATEFUL_KEY_COLUMN(VARCHAR)
      VELOX_STATEFUL_KEY_COLUMN(VARBINARY)
      VELOX_STATEFUL_KEY_COLUMN(TIMESTAMP)
#undef VELOX_STATEFUL_KEY_COLUMN
      default:
        VELOX_NYI(
            "State key column type {} is not supported yet",
            schema_->keyTypes()[column]->kindName());
    }
  }

  template <typename D>
  void
  setColumnAs(size_t column, std::string_view bytes, const VectorPtr& values) {
    VELOX_CHECK(
        !bytes.empty(), "Corrupt checkpoint: missing key column null flag");
    auto* flat = values->as<FlatVector<D>>();
    if (bytes[0] != 1) {
      flat->setNull(0, true);
      return;
    }
    if constexpr (std::is_same_v<D, Timestamp>) {
      VELOX_CHECK_EQ(
          bytes.size() - 1,
          sizeof(Timestamp),
          "Corrupt checkpoint: truncated timestamp key column");
      D value;
      std::memcpy(&value, bytes.data() + 1, sizeof(Timestamp));
      flat->set(0, value);
    } else {
      auto serializer = std::static_pointer_cast<TypeSerializer<D>>(
          columnSerializers_[column]);
      // FlatVector::set copies non-inline strings out of the checkpoint
      // buffer, so the borrowed StringView is safe here.
      flat->set(0, serializer->deserialize(bytes.substr(1)));
    }
  }

  const RowContainerKeySchema* schema_;
  memory::MemoryPool* pool_;
  const Probe probe_;
  std::vector<TypeSerializerPtr> columnSerializers_;
};

} // namespace facebook::velox::stateful
