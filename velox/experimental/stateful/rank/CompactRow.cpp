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
#include "velox/experimental/stateful/rank/CompactRow.h"

#include <cstring>
#include <limits>

namespace facebook::velox::stateful {

namespace {

// The supported fixed-width kinds of the compact layout: exactly the
// comparator's scalar set (RankOperator compareScalarValues) plus DATE
// (INTEGER kind) and DECIMAL (BIGINT/HUGEINT kind by precision) - anything
// else fails loud at layout construction instead of on the first encode.
void checkColumnKind(const TypePtr& type, column_index_t channel) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
    case TypeKind::HUGEINT:
      return;
    default:
      VELOX_UNSUPPORTED(
          "Compact row storage does not support column {} of type {}",
          channel,
          type->toString());
  }
}

// Typed value compare of two same-kind scalars: the value part of Flink
// GenerateUtils.generateRowCompare (the null ordering lives in the compare
// cores below).
template <typename T>
int compareValues(T a, T b) {
  return a < b ? -1 : (b < a ? 1 : 0);
}

// The one-allocation allocator of makeCompactRow: allocate_shared hands it
// (rebound to the shared_ptr control block type) exactly one allocate(1)
// call, which we over-size by 'extra' bytes - the control block, the
// CompactRowData header and the row bytes share this single block (design
// S3.1 allocation discipline).
template <typename T>
struct CompactRowAllocator {
  using value_type = T;

  explicit CompactRowAllocator(size_t extraBytes) : extra(extraBytes) {}

  template <typename U>
  CompactRowAllocator(const CompactRowAllocator<U>& other) noexcept
      : extra(other.extra) {}

  T* allocate(size_t n) {
    VELOX_CHECK_EQ(n, 1, "CompactRowAllocator hands out single objects");
    return static_cast<T*>(::operator new(n * sizeof(T) + extra));
  }

  void deallocate(T* p, size_t n) noexcept {
    ::operator delete(p, n * sizeof(T) + extra);
  }

  bool operator==(const CompactRowAllocator& other) const noexcept {
    return extra == other.extra;
  }

  bool operator!=(const CompactRowAllocator& other) const noexcept {
    return extra != other.extra;
  }

  size_t extra;
};

static_assert(
    std::is_same_v<
        CompactRowAllocator<CompactRowData>::value_type,
        CompactRowData>,
    "CompactRowAllocator keeps its value type");

int compareScalarInputVsStored(
    const TypePtr& type,
    const DecodedVector& decoded,
    vector_size_t inputRow,
    const CompactRowLayout& layout,
    const CompactRowData& stored,
    column_index_t channel) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return compareValues<bool>(
          decoded.valueAt<bool>(inputRow),
          layout.valueAt<bool>(stored, channel));
    case TypeKind::TINYINT:
      return compareValues<int8_t>(
          decoded.valueAt<int8_t>(inputRow),
          layout.valueAt<int8_t>(stored, channel));
    case TypeKind::SMALLINT:
      return compareValues<int16_t>(
          decoded.valueAt<int16_t>(inputRow),
          layout.valueAt<int16_t>(stored, channel));
    case TypeKind::INTEGER:
      return compareValues<int32_t>(
          decoded.valueAt<int32_t>(inputRow),
          layout.valueAt<int32_t>(stored, channel));
    case TypeKind::BIGINT:
      return compareValues<int64_t>(
          decoded.valueAt<int64_t>(inputRow),
          layout.valueAt<int64_t>(stored, channel));
    case TypeKind::HUGEINT:
      return compareValues<int128_t>(
          decoded.valueAt<int128_t>(inputRow),
          layout.hugeintAt(stored, channel));
    case TypeKind::REAL:
      return compareValues<float>(
          decoded.valueAt<float>(inputRow),
          layout.valueAt<float>(stored, channel));
    case TypeKind::DOUBLE:
      return compareValues<double>(
          decoded.valueAt<double>(inputRow),
          layout.valueAt<double>(stored, channel));
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return compareValues<StringView>(
          decoded.valueAt<StringView>(inputRow),
          layout.stringAt(stored, channel));
    case TypeKind::TIMESTAMP:
      return compareValues<Timestamp>(
          decoded.valueAt<Timestamp>(inputRow),
          layout.timestampAt(stored, channel));
    default:
      VELOX_UNSUPPORTED(
          "Rank sort key type {} is not supported", type->toString());
  }
}

int compareScalarStoredVsStored(
    const TypePtr& type,
    const CompactRowLayout& layout,
    column_index_t channel,
    const CompactRowData& lhs,
    const CompactRowData& rhs) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return compareValues<bool>(
          layout.valueAt<bool>(lhs, channel),
          layout.valueAt<bool>(rhs, channel));
    case TypeKind::TINYINT:
      return compareValues<int8_t>(
          layout.valueAt<int8_t>(lhs, channel),
          layout.valueAt<int8_t>(rhs, channel));
    case TypeKind::SMALLINT:
      return compareValues<int16_t>(
          layout.valueAt<int16_t>(lhs, channel),
          layout.valueAt<int16_t>(rhs, channel));
    case TypeKind::INTEGER:
      return compareValues<int32_t>(
          layout.valueAt<int32_t>(lhs, channel),
          layout.valueAt<int32_t>(rhs, channel));
    case TypeKind::BIGINT:
      return compareValues<int64_t>(
          layout.valueAt<int64_t>(lhs, channel),
          layout.valueAt<int64_t>(rhs, channel));
    case TypeKind::HUGEINT:
      return compareValues<int128_t>(
          layout.hugeintAt(lhs, channel), layout.hugeintAt(rhs, channel));
    case TypeKind::REAL:
      return compareValues<float>(
          layout.valueAt<float>(lhs, channel),
          layout.valueAt<float>(rhs, channel));
    case TypeKind::DOUBLE:
      return compareValues<double>(
          layout.valueAt<double>(lhs, channel),
          layout.valueAt<double>(rhs, channel));
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return compareValues<StringView>(
          layout.stringAt(lhs, channel), layout.stringAt(rhs, channel));
    case TypeKind::TIMESTAMP:
      return compareValues<Timestamp>(
          layout.timestampAt(lhs, channel), layout.timestampAt(rhs, channel));
    default:
      VELOX_UNSUPPORTED(
          "Rank sort key type {} is not supported", type->toString());
  }
}

} // namespace

CompactRowData::CompactRowData(uint32_t size, std::string_view blob)
    : size_(size) {
  VELOX_CHECK_EQ(blob.size(), size, "Compact row size word and blob disagree");
  // Writes into the trailing region of the single allocation the custom
  // allocator over-sized for exactly this payload (see makeCompactRow).
  std::memcpy(
      reinterpret_cast<char*>(this) + sizeof(CompactRowData),
      blob.data(),
      size);
}

CompactRowPtr makeCompactRow(std::string_view rowBytes) {
  VELOX_CHECK_LE(
      rowBytes.size(),
      std::numeric_limits<uint32_t>::max(),
      "Compact row exceeds the u32 size word");
  // One allocation for the shared_ptr control block + the CompactRowData
  // header + the row bytes: allocate_shared places the control block and
  // the object into the block this allocator returns, over-sized by the
  // row length so the header's trailing region holds the bytes.
  return std::allocate_shared<CompactRowData>(
      CompactRowAllocator<CompactRowData>(rowBytes.size()),
      static_cast<uint32_t>(rowBytes.size()),
      rowBytes);
}

CompactRowLayout::CompactRowLayout(const RowTypePtr& inputType)
    : inputType_(inputType),
      numColumns_(inputType->size()),
      // Null bitmap: one bit per column, padded to 8B alignment so the
      // fixed slots stay aligned (design S3.1: bitmap padded to 8B).
      bitmapBytes_(((numColumns_ + 7) / 8 + 7) / 8 * 8),
      slotOffsets_(numColumns_),
      varlenChannels_(numColumns_, false) {
  VELOX_CHECK_GE(numColumns_, 1, "A compact row layout needs a column");
  uint32_t offset = static_cast<uint32_t>(bitmapBytes_);
  for (column_index_t c = 0; c < numColumns_; ++c) {
    const auto& type = inputType_->childAt(c);
    checkColumnKind(type, c);
    slotOffsets_[c] = offset;
    varlenChannels_[c] =
        (type->kind() == TypeKind::VARCHAR ||
         type->kind() == TypeKind::VARBINARY);
    offset += slotWidthOf(type);
  }
  // The varlen area starts where the fixed area ends; encodeRow appends the
  // strings there and stores the absolute row offset in each varchar slot.
}

// static
uint32_t CompactRowLayout::slotWidthOf(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      // Fixed-width scalars: 8B-aligned slots (design S3.1).
      return 8;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      // {u32 varOffset, u32 length} locator slot (design S3.1).
      return 16;
    case TypeKind::TIMESTAMP:
      // i64 seconds + u32 nanos + 4B pad, the raw ValueSerializer<Timestamp>
      // pair (design S3.1).
      return 16;
    case TypeKind::HUGEINT:
      return 16;
    default:
      VELOX_UNSUPPORTED(
          "Compact row storage does not support type {}", type->toString());
  }
}

std::string CompactRowLayout::encodeRow(
    const std::vector<DecodedVector>& columns,
    vector_size_t row) const {
  VELOX_CHECK_EQ(
      columns.size(),
      numColumns_,
      "Compact row encode needs one decoded vector per input column");
  // Varlen total first: the row is one contiguous blob sized exactly once.
  size_t varlenBytes = 0;
  for (column_index_t c = 0; c < numColumns_; ++c) {
    if (varlenChannels_[c] && !columns[c].isNullAt(row)) {
      varlenBytes += columns[c].valueAt<StringView>(row).size();
    }
  }
  const size_t rowSize = slotOffsets_[numColumns_ - 1] +
      slotWidthOf(inputType_->childAt(numColumns_ - 1)) + varlenBytes;
  VELOX_CHECK_LE(
      rowSize,
      std::numeric_limits<uint32_t>::max(),
      "Compact row exceeds the u32 size word");
  std::string out(rowSize, '\0');
  auto* raw = out.data();

  // Null bitmap (the rest of the bitmap padding stays zero).
  for (column_index_t c = 0; c < numColumns_; ++c) {
    if (columns[c].isNullAt(row)) {
      raw[c / 8] |= static_cast<char>(1U << (c % 8));
    }
  }
  // Fixed slots + varlen bytes. Little-endian hosts (velox's supported
  // platforms) make memcpy the little-endian convention of the checkpoint
  // chain (TypeSerializer.h littleEndianBytes).
  uint32_t varOffset = slotOffsets_[numColumns_ - 1] +
      slotWidthOf(inputType_->childAt(numColumns_ - 1));
  for (column_index_t c = 0; c < numColumns_; ++c) {
    const auto& type = inputType_->childAt(c);
    char* slot = raw + slotOffsets_[c];
    if (columns[c].isNullAt(row)) {
      continue;
    }
    switch (type->kind()) {
      case TypeKind::BOOLEAN: {
        const auto value = columns[c].valueAt<bool>(row);
        std::memcpy(slot, &value, sizeof(bool));
        break;
      }
      case TypeKind::TINYINT: {
        const auto value = columns[c].valueAt<int8_t>(row);
        std::memcpy(slot, &value, sizeof(int8_t));
        break;
      }
      case TypeKind::SMALLINT: {
        const auto value = columns[c].valueAt<int16_t>(row);
        std::memcpy(slot, &value, sizeof(int16_t));
        break;
      }
      case TypeKind::INTEGER: {
        const auto value = columns[c].valueAt<int32_t>(row);
        std::memcpy(slot, &value, sizeof(int32_t));
        break;
      }
      case TypeKind::BIGINT: {
        const auto value = columns[c].valueAt<int64_t>(row);
        std::memcpy(slot, &value, sizeof(int64_t));
        break;
      }
      case TypeKind::REAL: {
        const auto value = columns[c].valueAt<float>(row);
        std::memcpy(slot, &value, sizeof(float));
        break;
      }
      case TypeKind::DOUBLE: {
        const auto value = columns[c].valueAt<double>(row);
        std::memcpy(slot, &value, sizeof(double));
        break;
      }
      case TypeKind::HUGEINT: {
        const auto value = columns[c].valueAt<int128_t>(row);
        std::memcpy(slot, &value, sizeof(int128_t));
        break;
      }
      case TypeKind::TIMESTAMP: {
        const auto value = columns[c].valueAt<Timestamp>(row);
        const int64_t seconds = value.getSeconds();
        const uint32_t nanos = static_cast<uint32_t>(value.getNanos());
        std::memcpy(slot, &seconds, sizeof(int64_t));
        std::memcpy(slot + sizeof(int64_t), &nanos, sizeof(uint32_t));
        break;
      }
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        const auto value = columns[c].valueAt<StringView>(row);
        std::memcpy(slot, &varOffset, sizeof(uint32_t));
        const uint32_t length = static_cast<uint32_t>(value.size());
        std::memcpy(slot + sizeof(uint32_t), &length, sizeof(uint32_t));
        std::memcpy(raw + varOffset, value.data(), value.size());
        varOffset += length;
        break;
      }
      default:
        VELOX_UNSUPPORTED(
            "Compact row storage does not support type {}", type->toString());
    }
  }
  return out;
}

int compareSortKeysInputVsStored(
    const CompactRowLayout& layout,
    const std::vector<DecodedVector>& decoded,
    vector_size_t inputRow,
    const std::vector<column_index_t>& channels,
    const std::vector<bool>& ascending,
    const std::vector<bool>& nullsFirst,
    const CompactRowData& stored) {
  for (size_t i = 0; i < channels.size(); ++i) {
    const auto channel = channels[i];
    const bool leftNull = decoded[channel].isNullAt(inputRow);
    const bool rightNull = layout.isNullAt(stored, channel);
    // Flink GenerateUtils.generateRowCompare: the null ordering result is
    // final and not negated by a descending direction.
    if (leftNull || rightNull) {
      if (leftNull && rightNull) {
        continue;
      }
      return leftNull ? (nullsFirst[i] ? -1 : 1) : (nullsFirst[i] ? 1 : -1);
    }
    const int cmp = compareScalarInputVsStored(
        layout.inputType()->childAt(channel),
        decoded[channel],
        inputRow,
        layout,
        stored,
        channel);
    if (cmp != 0) {
      return ascending[i] ? cmp : -cmp;
    }
  }
  return 0;
}

int compareSortKeysStoredVsStored(
    const CompactRowLayout& layout,
    const std::vector<column_index_t>& channels,
    const std::vector<bool>& ascending,
    const std::vector<bool>& nullsFirst,
    const CompactRowData& lhs,
    const CompactRowData& rhs) {
  for (size_t i = 0; i < channels.size(); ++i) {
    const auto channel = channels[i];
    const bool leftNull = layout.isNullAt(lhs, channel);
    const bool rightNull = layout.isNullAt(rhs, channel);
    if (leftNull || rightNull) {
      if (leftNull && rightNull) {
        continue;
      }
      return leftNull ? (nullsFirst[i] ? -1 : 1) : (nullsFirst[i] ? 1 : -1);
    }
    const int cmp = compareScalarStoredVsStored(
        layout.inputType()->childAt(channel), layout, channel, lhs, rhs);
    if (cmp != 0) {
      return ascending[i] ? cmp : -cmp;
    }
  }
  return 0;
}

std::string CompactRowSerializer::serialize(const CompactRowPtr& value) {
  if (value == nullptr) {
    return std::string(1, static_cast<char>(0));
  }
  // The null flag byte followed by the row bytes verbatim (the
  // SharedPtrSerializer null-flag shape, TypeSerializer.h).
  std::string out(1, static_cast<char>(1));
  out.append(value->bytes(), value->size());
  return out;
}

CompactRowPtr CompactRowSerializer::deserialize(std::string_view str) {
  VELOX_CHECK(!str.empty(), "Missing null flag in the compact row payload");
  const char flag = str[0];
  VELOX_CHECK(
      flag == 0 || flag == 1,
      "Corrupt checkpoint: compact row null flag must be 0 or 1, got {}",
      static_cast<int>(flag));
  if (flag == 0) {
    return nullptr;
  }
  // The borrowed view never outlives this call: the row bytes copy into a
  // fresh single-block row (the TypeSerializer view contract, design S3.1).
  return makeCompactRow(str.substr(1));
}

} // namespace facebook::velox::stateful
