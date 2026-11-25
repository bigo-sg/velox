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
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::stateful {

/// Compact row storage of the rank operator (design P1 option a): one row
/// as one contiguous byte blob -
///   [null bitmap ceil(C/8) bytes, padded to 8B alignment]
///   [fixed area: one slot per input column, 8B for the fixed-width scalars,
///    16B for TIMESTAMP (i64 seconds + u32 nanos + pad, the raw pair of
///    ValueSerializer<Timestamp>), 16B int128 for HUGEINT, and 16B
///    {u32 varOffset, u32 length} for VARCHAR/VARBINARY pointing into ...]
///   [varlen area: the VARCHAR/VARBINARY byte strings, contiguous]
/// little-endian, column order = input column order. Slot widths and offsets
/// are computed once per input row type (CompactRowLayout), so a field read
/// is an O(1) fixed-offset load with no whole-row deserialization - the sort
/// key channel reads of the comparator hot path (design alignment #60).
class CompactRowData {
 public:
  /// Not directly constructible: makeCompactRow is the only creator (it
  /// fuses the shared_ptr control block, this header and the row bytes into
  /// one heap block).
  CompactRowData(uint32_t size, std::string_view blob);

  /// Total row bytes (bitmap + fixed + varlen).
  uint32_t size() const {
    return size_;
  }

  /// The row bytes start right after this header (8B aligned).
  const char* bytes() const {
    return reinterpret_cast<const char*>(this) + sizeof(CompactRowData);
  }

 private:
  const uint32_t size_;
  // 4 bytes of padding keep sizeof(CompactRowData) at 8 so the row bytes
  // stay 8B aligned; the row blob itself lives in the trailing region of
  // the one heap allocation makeCompactRow performs.
};

using CompactRowPtr = std::shared_ptr<const CompactRowData>;

/// Creates one compact row in exactly one heap allocation: the
/// shared_ptr control block, the CompactRowData header and the row bytes
/// share a single block (std::allocate_shared over an allocator that
/// over-sizes the one allocation by the row length) - 60M rows carry no
/// separate control-block tax (design S3.1 allocation discipline).
CompactRowPtr makeCompactRow(std::string_view rowBytes);

/// Per-input-row-type slot offset table. Built once per operator; every
/// encode, field read and comparison goes through it.
class CompactRowLayout {
 public:
  explicit CompactRowLayout(const RowTypePtr& inputType);

  const RowTypePtr& inputType() const {
    return inputType_;
  }

  /// True when 'channel' stores its bytes in the varlen area.
  bool isVarlen(column_index_t channel) const {
    return varlenChannels_[channel];
  }

  /// Slot read dispatch - the one entry the comparison cores and the emit
  /// column decoder both use: fixed-width scalars (incl. the 16B int128)
  /// through one memcpy, VARCHAR/VARBINARY and TIMESTAMP through their
  /// named readers.
  template <typename T>
  T valueAt(const CompactRowData& row, column_index_t channel) const {
    if constexpr (std::is_same_v<T, StringView>) {
      return stringAt(row, channel);
    } else if constexpr (std::is_same_v<T, Timestamp>) {
      return timestampAt(row, channel);
    } else {
      static_assert(
          std::is_arithmetic_v<T>,
          "valueAt covers the compact layout's supported kinds only");
      T value;
      std::memcpy(&value, row.bytes() + slotOffsets_[channel], sizeof(T));
      return value;
    }
  }

  /// VARCHAR/VARBINARY slot read: a StringView into the row's own varlen
  /// area; the CompactRowPtr keeps the row bytes alive.
  StringView stringAt(const CompactRowData& row, column_index_t channel) const {
    const uint32_t varOffset = scalarAt<uint32_t>(row, channel);
    const uint32_t length = scalarAt<uint32_t>(row, channel, 4);
    return StringView(row.bytes() + varOffset, length);
  }

  /// TIMESTAMP slot read (i64 seconds + u32 nanos).
  Timestamp timestampAt(const CompactRowData& row, column_index_t channel)
      const {
    int64_t seconds;
    uint32_t nanos;
    std::memcpy(&seconds, row.bytes() + slotOffsets_[channel], sizeof(int64_t));
    std::memcpy(
        &nanos, row.bytes() + slotOffsets_[channel] + 8, sizeof(uint32_t));
    return Timestamp(seconds, nanos);
  }

  /// HUGEINT slot read (int128).
  int128_t hugeintAt(const CompactRowData& row, column_index_t channel) const {
    int128_t value;
    std::memcpy(&value, row.bytes() + slotOffsets_[channel], sizeof(int128_t));
    return value;
  }

  bool isNullAt(const CompactRowData& row, column_index_t channel) const {
    const auto byte = static_cast<uint8_t>(row.bytes()[channel / 8]);
    return (byte & (1U << (channel % 8))) != 0;
  }

  /// Encodes input row 'row' out of the batch-decoded columns into one
  /// contiguous row-byte blob (makeCompactRow wraps it into a CompactRowPtr).
  std::string encodeRow(
      const std::vector<DecodedVector>& columns,
      vector_size_t row) const;

 private:
  template <typename T>
  T scalarAt(
      const CompactRowData& row,
      column_index_t channel,
      size_t slotByte = 0) const {
    T value;
    std::memcpy(
        &value, row.bytes() + slotOffsets_[channel] + slotByte, sizeof(T));
    return value;
  }

  static uint32_t slotWidthOf(const TypePtr& type);

  const RowTypePtr inputType_;
  const size_t numColumns_;
  const size_t bitmapBytes_;
  std::vector<uint32_t> slotOffsets_;
  std::vector<bool> varlenChannels_;
};

/// Sort-key comparison cores over compact rows (design alignment #60/#72):
/// same channel walk, null-ordering and typed value compare as the stored-
/// vector comparator (Flink GenerateUtils.generateRowCompare: the null
/// result is final and never negated by a descending direction) - only the
/// stored side's reads switch to fixed-offset slot loads.
///
/// Input x stored: 'decoded' holds the whole input batch, 'inputRow' is the
/// batch row; the stored side reads through 'layout'.
int compareSortKeysInputVsStored(
    const CompactRowLayout& layout,
    const std::vector<DecodedVector>& decoded,
    vector_size_t inputRow,
    const std::vector<column_index_t>& channels,
    const std::vector<bool>& ascending,
    const std::vector<bool>& nullsFirst,
    const CompactRowData& stored);

/// Stored x stored: the lazy-rebuild path (insertionPosition) compares two
/// rows that both live as compact rows.
int compareSortKeysStoredVsStored(
    const CompactRowLayout& layout,
    const std::vector<column_index_t>& channels,
    const std::vector<bool>& ascending,
    const std::vector<bool>& nullsFirst,
    const CompactRowData& lhs,
    const CompactRowData& rhs);

/// Serializes a CompactRowPtr for the checkpoint plane (design S6): a null
/// flag byte followed by the row bytes verbatim (the SharedPtrSerializer
/// null-flag shape, TypeSerializer.h), schema id "compact-row:v1:<inputType>"
/// - the v1 marker bumps on any layout change (design limit L2).
/// deserialize copies the borrowed view into a fresh single-block row, so
/// nothing retains the checkpoint buffer (the TypeSerializer view
/// contract).
class CompactRowSerializer : public TypeSerializer<CompactRowPtr> {
 public:
  explicit CompactRowSerializer(RowTypePtr inputType)
      : inputType_(std::move(inputType)) {}

  std::string schemaId() const override {
    return "compact-row:v1:" + inputType_->toString();
  }

  std::string serialize(const CompactRowPtr& value) override;

  CompactRowPtr deserialize(std::string_view str) override;

 private:
  const RowTypePtr inputType_;
};

inline std::shared_ptr<CompactRowSerializer> makeCompactRowSerializer(
    RowTypePtr inputType) {
  return std::make_shared<CompactRowSerializer>(std::move(inputType));
}

} // namespace facebook::velox::stateful
