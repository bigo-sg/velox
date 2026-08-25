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
#include <vector>
#include "velox/exec/RowContainer.h"
#include "velox/type/HugeInt.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"

namespace facebook::velox::stateful {

/// Abstract identity of a state key, orthogonal to Namespace: a StateKey
/// answers "which key", a Namespace answers "which logical partition of that
/// key's state (e.g. which window)". Relevant to the key type K of Flink's
/// AbstractKeyedStateBackend<K>.
class StateKey {
 public:
  virtual ~StateKey() = default;

  /// Value-based equality on the user key columns. Null equals null.
  virtual bool equals(const StateKey& other) const = 0;

  /// 64-bit hash of the user key value. Implementations cache the hash at
  /// construction.
  virtual uint64_t hash() const = 0;

  /// Derived attribute: hash() % maxParallelism. Deterministic for the same
  /// key value, so key-group assignment is stable across rescale as long as
  /// maxParallelism is unchanged. Used by shuffle, probe, snapshot and
  /// restore, which all share the same hash.
  virtual uint32_t keyGroup() const = 0;
};

/// Per-operator description of the key row layout: the key RowContainer plus
/// the user key column types. A single instance is shared by all
/// RowContainerStateKeys of one operator so that each key only carries a row
/// pointer plus the cached hash and keyGroup. The container itself is owned
/// and mutated by the operator (new key rows on deserialize / restore), so it
/// is exposed non-const; RowContainerStateKey remains an immutable view.
class RowContainerKeySchema {
 public:
  RowContainerKeySchema(
      exec::RowContainer* container,
      std::vector<TypePtr> keyTypes)
      : container_(container),
        keyTypes_(std::move(keyTypes)) {}

  /// Column-wise value equality of two rows of the container. Null equals
  /// null. Supported key column types: fixed-width scalars and varchar /
  /// varbinary; complex types (array / map / row) are not supported yet.
  bool rowsEqual(const char* lhs, const char* rhs) const;

  exec::RowContainer* container() const {
    return container_;
  }

  const std::vector<TypePtr>& keyTypes() const {
    return keyTypes_;
  }

 private:
  exec::RowContainer* const container_;
  const std::vector<TypePtr> keyTypes_;
};

/// Immutable view over one row of the key RowContainer. The row is owned by
/// the container (view semantics). hash and keyGroup are computed once, at
/// construction, and cached. Instances are cheap to copy.
///
/// Note: the design doc's constructor (row, hash, maxParallelism) is
/// extended with the schema pointer because value equality needs the column
/// layout and types, which are not derivable from a raw row pointer.
class RowContainerStateKey : public StateKey {
 public:
  RowContainerStateKey(
      const RowContainerKeySchema* schema,
      const char* row,
      uint64_t hash,
      uint32_t maxParallelism)
      : schema_(schema),
        row_(row),
        hash_(hash),
        keyGroup_(static_cast<uint32_t>(hash % maxParallelism)) {}

  bool equals(const StateKey& other) const override;

  uint64_t hash() const override {
    return hash_;
  }

  uint32_t keyGroup() const override {
    return keyGroup_;
  }

  const char* row() const {
    return row_;
  }

  const RowContainerKeySchema* schema() const {
    return schema_;
  }

 private:
  const RowContainerKeySchema* schema_;
  const char* row_;
  const uint64_t hash_;
  const uint32_t keyGroup_;
};

inline bool RowContainerStateKey::equals(const StateKey& other) const {
  auto* otherKey = dynamic_cast<const RowContainerStateKey*>(&other);
  return otherKey != nullptr && otherKey->schema_ == schema_ &&
      schema_->rowsEqual(row_, otherKey->row_);
}

inline bool RowContainerKeySchema::rowsEqual(
    const char* lhs,
    const char* rhs) const {
  for (size_t i = 0; i < keyTypes_.size(); ++i) {
    const auto column = container_->columnAt(i);
    // For a non-nullable column nullMask() is 0, so the check is always
    // false and always safe (see RowColumn::PackOffsets).
    const bool lhsNull =
        (lhs[column.nullByte()] & column.nullMask()) != 0;
    const bool rhsNull =
        (rhs[column.nullByte()] & column.nullMask()) != 0;
    if (lhsNull != rhsNull) {
      return false;
    }
    if (lhsNull) {
      continue;
    }
    const char* lhsValue = lhs + column.offset();
    const char* rhsValue = rhs + column.offset();
    switch (keyTypes_[i]->kind()) {
#define VELOX_STATEFUL_COMPARE_FIXED_WIDTH(KIND)                       \
  case TypeKind::KIND: {                                               \
    using T = TypeTraits<TypeKind::KIND>::NativeType;                  \
    if (*reinterpret_cast<const T*>(lhsValue) !=                       \
        *reinterpret_cast<const T*>(rhsValue)) {                       \
      return false;                                                    \
    }                                                                  \
    break;                                                             \
  }
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(BOOLEAN)
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(TINYINT)
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(SMALLINT)
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(INTEGER)
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(BIGINT)
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(HUGEINT)
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(REAL)
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(DOUBLE)
      VELOX_STATEFUL_COMPARE_FIXED_WIDTH(TIMESTAMP)
#undef VELOX_STATEFUL_COMPARE_FIXED_WIDTH
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        const auto& lhsView =
            *reinterpret_cast<const StringView*>(lhsValue);
        const auto& rhsView =
            *reinterpret_cast<const StringView*>(rhsValue);
        if (lhsView.size() != rhsView.size() ||
            std::memcmp(lhsView.data(), rhsView.data(), lhsView.size()) !=
                0) {
          return false;
        }
        break;
      }
      default:
        VELOX_NYI(
            "State key column type {} is not supported yet",
            keyTypes_[i]->kindName());
    }
  }
  return true;
}

} // namespace facebook::velox::stateful
