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

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::stateful {

/// Fixed-width integers and length prefixes use little-endian byte order.
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
      "Expected a {}-byte component, got {}",
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

/// Writes little-endian primitives and length-prefixed byte components.
class ByteWriter {
 public:
  explicit ByteWriter(std::string& out) : out_(out) {}

  void writeUInt8(uint8_t value) {
    out_.push_back(static_cast<char>(value));
  }

  void writeUInt32(uint32_t value) {
    out_.append(littleEndianBytes(value));
  }

  void writeInt32(int32_t value) {
    writeUInt32(static_cast<uint32_t>(value));
  }

  void writeBytes(std::string_view bytes) {
    VELOX_CHECK_LE(bytes.size(), UINT32_MAX);
    writeUInt32(static_cast<uint32_t>(bytes.size()));
    out_.append(bytes.data(), bytes.size());
  }

 protected:
  std::string& out_;
};

/// Reads the corresponding components as views into the caller-owned buffer.
class ByteReader {
 public:
  ByteReader(const char* data, size_t size) : data_(data), size_(size) {}

  uint8_t readUInt8() {
    return static_cast<uint8_t>(view(1)[0]);
  }

  uint32_t readUInt32() {
    return littleEndianValue<uint32_t>(view(sizeof(uint32_t)));
  }

  int32_t readInt32() {
    return static_cast<int32_t>(readUInt32());
  }

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
        size_ - offset_,
        "Component of {} bytes exceeds the remaining {} bytes",
        length,
        size_ - offset_);
    std::string_view bytes(data_ + offset_, length);
    offset_ += length;
    return bytes;
  }

  const char* data_;
  const size_t size_;
  size_t offset_{0};
};

} // namespace facebook::velox::stateful
