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
#include <string>
#include <string_view>

#include <folly/hash/Checksum.h>

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/state/ByteStream.h"

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

/// Appends the keyed-state checkpoint stream using the shared byte codec.
class CheckpointWriter : public ByteWriter {
 public:
  using ByteWriter::ByteWriter;

  /// Reserve the count before traversing a state's entries, then patch it.
  size_t writeInt32Placeholder() {
    const size_t position = out_.size();
    writeInt32(0);
    return position;
  }

  void patchInt32(size_t position, int32_t value) {
    VELOX_CHECK_LE(position, out_.size());
    VELOX_CHECK_LE(sizeof(int32_t), out_.size() - position);
    const auto encoded = littleEndianBytes(static_cast<uint32_t>(value));
    out_.replace(position, encoded.size(), encoded);
  }
};

/// Reads the keyed-state checkpoint stream using the shared byte codec.
class CheckpointReader : public ByteReader {
 public:
  using ByteReader::ByteReader;
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
