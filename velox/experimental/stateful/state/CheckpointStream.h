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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

#include <folly/Portability.h>
#include <folly/hash/Checksum.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::stateful {

/// Magic at the head of every keyed-state checkpoint stream.
constexpr std::string_view kCheckpointMagic = "VSKV";

/// Version of the keyed-state checkpoint byte format. It changes only when
/// the layout described in this header changes; streams of other versions
/// are rejected at restore.
constexpr uint8_t kCheckpointFormatVersion = 1;

/// Bits of the header flags byte.
/// Little-endian: every integer and every length prefix in the stream is
/// little-endian. Always set by this writer — the format has no big-endian
/// variant; the bit makes the byte order explicit on the wire.
constexpr uint8_t kCheckpointFlagLittleEndian = 0x01;
/// The stream ends with a CRC-32 trailer covering every byte before it.
/// Storage layers that verify integrity themselves (HDFS and its .crc side
/// files, for instance) allow the writer to turn the trailer off; the
/// reader honors the flag either way.
constexpr uint8_t kCheckpointFlagCrcTrailer = 0x02;

/// Size of the fixed header: the magic, the version, the flags and the
/// three key-group labels.
constexpr size_t kCheckpointHeaderSize =
    kCheckpointMagic.size() + 2 + 3 * sizeof(uint32_t);

/// Fixed-width primitives travel as their little-endian byte image; this
/// pair is the single encode/decode point of that rule.
template <typename T>
inline std::string littleEndianBytes(const T& value) {
  static_assert(
      std::is_trivially_copyable_v<T>, "Expected a fixed-width primitive");
  char bytes[sizeof(T)];
  std::memcpy(bytes, &value, sizeof(T));
  if constexpr (sizeof(T) > 1) {
    if (!folly::kIsLittleEndian) {
      std::reverse(bytes, bytes + sizeof(T));
    }
  }
  return std::string(bytes, sizeof(T));
}

template <typename T>
inline T littleEndianValue(std::string_view bytes) {
  static_assert(
      std::is_trivially_copyable_v<T>, "Expected a fixed-width primitive");
  VELOX_CHECK_EQ(
      bytes.size(),
      sizeof(T),
      "Corrupt checkpoint: expected a {}-byte component, got {}",
      sizeof(T),
      bytes.size());
  char buffer[sizeof(T)];
  std::memcpy(buffer, bytes.data(), sizeof(T));
  if constexpr (sizeof(T) > 1) {
    if (!folly::kIsLittleEndian) {
      std::reverse(buffer, buffer + sizeof(T));
    }
  }
  T value;
  std::memcpy(&value, buffer, sizeof(T));
  return value;
}

/// CRC-32 (the zlib polynomial) of the given bytes: the value the stream
/// trailer carries. folly::crc32 is the raw chainable register value; the
/// xor finalizes it to the standard checksum ("123456789" hashes to
/// 0xCBF43926).
inline uint32_t checkpointCrc32(std::string_view bytes) {
  return folly::crc32(
             reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()) ^
      0xFFFFFFFF;
}

/// The fixed header every stream starts with. The three labels describe the
/// writer's key-group space: 'maxParallelism' defines the space (hash %
/// maxParallelism) and stays constant across rescale; 'startKeyGroup' and
/// 'numKeyGroups' describe the sub-range the stream covers.
struct CheckpointHeader {
  uint8_t flags{};
  uint32_t maxParallelism{};
  uint32_t startKeyGroup{};
  uint32_t numKeyGroups{};
};

/// Appends the keyed-state checkpoint stream to a std::string. All
/// integers and length prefixes are little-endian; variable-length
/// components are length-prefixed. The int32 placeholder / patch pair
/// serves the count-before-entries framing of a state block: the count
/// position is reserved before the entries are traversed and patched
/// afterwards, as in Flink's snapshot writers.
class CheckpointWriter {
 public:
  explicit CheckpointWriter(std::string& out) : out_(out) {}

  void writeUInt8(uint8_t value) {
    out_.push_back(static_cast<char>(value));
  }

  void writeUInt32(uint32_t value) {
    out_.append(littleEndianBytes(value));
  }

  void writeInt32(int32_t value) {
    writeUInt32(static_cast<uint32_t>(value));
  }

  /// Appends a length-prefixed byte component.
  void writeBytes(std::string_view bytes) {
    writeUInt32(static_cast<uint32_t>(bytes.size()));
    out_.append(bytes.data(), bytes.size());
  }

  /// Appends an int32 placeholder and returns its position for patchInt32().
  size_t writeInt32Placeholder() {
    const size_t position = out_.size();
    writeInt32(0);
    return position;
  }

  void patchInt32(size_t position, int32_t value) {
    const auto encoded = littleEndianBytes(static_cast<uint32_t>(value));
    std::memcpy(out_.data() + position, encoded.data(), encoded.size());
  }

 private:
  std::string& out_;
};

/// Reads back what CheckpointWriter wrote. Every read is bounds-checked.
class CheckpointReader {
 public:
  CheckpointReader(const char* data, size_t size) : data_(data), size_(size) {}

  uint8_t readUInt8() {
    return static_cast<uint8_t>(view(1)[0]);
  }

  uint32_t readUInt32() {
    return littleEndianValue<uint32_t>(view(sizeof(uint32_t)));
  }

  int32_t readInt32() {
    return static_cast<int32_t>(readUInt32());
  }

  /// Reads a length-prefixed byte component as a zero-copy view into the
  /// underlying buffer; the buffer must outlive the view (restore() holds
  /// the checkpoint bytes for the whole call).
  std::string_view readBytes() {
    const auto length = readUInt32();
    return view(length);
  }

  bool atEnd() const {
    return offset_ == size_;
  }

 private:
  std::string_view view(size_t length) {
    VELOX_CHECK_LE(
        length,
        remaining(),
        "Corrupt checkpoint: component of {} bytes exceeds the remaining {} bytes",
        length,
        remaining());
    std::string_view bytes(data_ + offset_, length);
    offset_ += length;
    return bytes;
  }

  size_t remaining() const {
    return size_ - offset_;
  }

  const char* data_;
  const size_t size_;
  size_t offset_{0};
};

inline void writeCheckpointHeader(
    CheckpointWriter& writer,
    const CheckpointHeader& header) {
  for (const char c : kCheckpointMagic) {
    writer.writeUInt8(static_cast<uint8_t>(c));
  }
  writer.writeUInt8(kCheckpointFormatVersion);
  writer.writeUInt8(header.flags);
  writer.writeUInt32(header.maxParallelism);
  writer.writeUInt32(header.startKeyGroup);
  writer.writeUInt32(header.numKeyGroups);
}

/// Reads and validates the fixed header — the magic, the version, the
/// little-endian flag; the remaining flags are returned for the caller to
/// act on.
inline CheckpointHeader readCheckpointHeader(CheckpointReader& reader) {
  for (const char c : kCheckpointMagic) {
    VELOX_CHECK_EQ(
        reader.readUInt8(),
        static_cast<uint8_t>(c),
        "Corrupt checkpoint: bad magic, not a keyed-state checkpoint stream");
  }
  VELOX_CHECK_EQ(
      reader.readUInt8(),
      kCheckpointFormatVersion,
      "Unsupported checkpoint format version");
  CheckpointHeader header;
  header.flags = reader.readUInt8();
  VELOX_CHECK(
      (header.flags & kCheckpointFlagLittleEndian) != 0,
      "Corrupt checkpoint: checkpoint streams are little-endian only");
  header.maxParallelism = reader.readUInt32();
  header.startKeyGroup = reader.readUInt32();
  header.numKeyGroups = reader.readUInt32();
  return header;
}

/// One entry is always three length-prefixed frames — namespace, key,
/// payload — whatever the state kind; kind-specific structure lives inside
/// the payload frame. This pair is the single read/write path for entries,
/// shared by parsing and skipping, so the layout has exactly one
/// definition.
inline void writeEntryFrames(
    CheckpointWriter& writer,
    std::string_view ns,
    std::string_view key,
    std::string_view payload) {
  writer.writeBytes(ns);
  writer.writeBytes(key);
  writer.writeBytes(payload);
}

struct CheckpointEntryFrames {
  std::string_view ns;
  std::string_view key;
  std::string_view payload;
};

inline CheckpointEntryFrames readEntryFrames(CheckpointReader& reader) {
  CheckpointEntryFrames frames;
  frames.ns = reader.readBytes();
  frames.key = reader.readBytes();
  frames.payload = reader.readBytes();
  return frames;
}

} // namespace facebook::velox::stateful
