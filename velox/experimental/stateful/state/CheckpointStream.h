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
#include <string>
#include <string_view>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::stateful {

/// Version of the keyed-state checkpoint byte format.
constexpr int32_t kCheckpointFormatVersion = 1;

/// Appends the keyed-state checkpoint format to a std::string. Primitives
/// are written in host byte order; variable-length components are
/// length-prefixed. The int32 placeholder / patch pair serves the
/// count-before-entries framing of a key-group block: the count position is
/// reserved before the entries are traversed and patched afterwards, as in
/// Flink's snapshot writers.
class CheckpointWriter {
 public:
  explicit CheckpointWriter(std::string& out) : out_(out) {}

  void writeInt32(int32_t value) {
    out_.append(reinterpret_cast<const char*>(&value), sizeof(value));
  }

  void writeInt16(int16_t value) {
    out_.append(reinterpret_cast<const char*>(&value), sizeof(value));
  }

  /// Appends a length-prefixed byte component.
  void writeBytes(std::string_view bytes) {
    writeInt32(static_cast<int32_t>(bytes.size()));
    out_.append(bytes.data(), bytes.size());
  }

  /// Appends an int32 placeholder and returns its position for patchInt32().
  size_t writeInt32Placeholder() {
    const size_t position = out_.size();
    writeInt32(0);
    return position;
  }

  void patchInt32(size_t position, int32_t value) {
    std::memcpy(out_.data() + position, &value, sizeof(value));
  }

 private:
  std::string& out_;
};

/// Reads back what CheckpointWriter wrote. Every read is bounds-checked.
class CheckpointReader {
 public:
  CheckpointReader(const char* data, size_t size) : data_(data), size_(size) {}

  int32_t readInt32() {
    return read<int32_t>();
  }

  int16_t readInt16() {
    return read<int16_t>();
  }

  /// Reads a length-prefixed byte component as a zero-copy view into the
  /// underlying buffer; the buffer must outlive the view (restore() holds
  /// the checkpoint bytes for the whole call).
  std::string_view readBytes() {
    const int32_t length = readInt32();
    VELOX_CHECK_GE(length, 0, "Corrupt checkpoint: negative component length");
    VELOX_CHECK_LE(
        static_cast<size_t>(length),
        remaining(),
        "Corrupt checkpoint: component of {} bytes exceeds the remaining {} bytes",
        length,
        remaining());
    std::string_view bytes(data_ + offset_, length);
    offset_ += length;
    return bytes;
  }

  bool atEnd() const {
    return offset_ == size_;
  }

 private:
  template <typename T>
  T read() {
    VELOX_CHECK_GE(
        remaining(),
        sizeof(T),
        "Corrupt checkpoint: truncated primitive, {} bytes remaining",
        remaining());
    T value;
    std::memcpy(&value, data_ + offset_, sizeof(T));
    offset_ += sizeof(T);
    return value;
  }

  size_t remaining() const {
    return size_ - offset_;
  }

  const char* data_;
  const size_t size_;
  size_t offset_{0};
};

} // namespace facebook::velox::stateful
