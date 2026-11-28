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
#include <sstream>
#include <string>

#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"

namespace facebook::velox::stateful {

/// Namespace for aligned windows (TUMBLE / HOP / CUMULATE): the window is
/// identified by the slice-end epoch-millis, mirroring Flink's Long
/// namespace for slicing window processors. Two slices are equal iff their
/// sliceEnd values are equal.
class SliceEndNs : public Namespace {
 public:
  explicit SliceEndNs(int64_t sliceEnd) : sliceEnd_(sliceEnd) {}

  int64_t sliceEnd() const {
    return sliceEnd_;
  }

  bool equals(const Namespace& other) const override {
    auto* that = dynamic_cast<const SliceEndNs*>(&other);
    return that != nullptr && sliceEnd_ == that->sliceEnd_;
  }

  /// Value equality (timer infrastructure compares namespaces by
  /// operator==, TimerHeapInternalTimer).
  bool operator==(const SliceEndNs& other) const {
    return sliceEnd_ == other.sliceEnd_;
  }

  uint64_t hash() const override {
    // Mix the full 64 bits into the (K, N) composite hash: a plain
    // low-bits identity would collide with the key hash on every probe.
    uint64_t h = static_cast<uint64_t>(sliceEnd_);
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    return h;
  }

 private:
  int64_t sliceEnd_;
};

} // namespace facebook::velox::stateful

namespace facebook::velox::stateful {

/// Serializes SliceEndNs as a fixed-width int64 frame.
class SliceEndNsSerializer : public TypeSerializer<SliceEndNs> {
 public:
  std::string schemaId() const override {
    return "slice-end";
  }

  std::string serialize(const SliceEndNs& ns) override {
    std::string out;
    out.resize(sizeof(int64_t));
    int64_t v = ns.sliceEnd();
    std::memcpy(out.data(), &v, sizeof(int64_t));
    return out;
  }

  SliceEndNs deserialize(std::string_view str) override {
    VELOX_CHECK_EQ(
        str.size(), sizeof(int64_t), "SliceEndNs frame must be 8 bytes");
    int64_t sliceEnd;
    std::memcpy(&sliceEnd, str.data(), sizeof(int64_t));
    return SliceEndNs(sliceEnd);
  }
};

template <>
struct NamespaceSerializerTraits<SliceEndNs> {
  static std::shared_ptr<TypeSerializer<SliceEndNs>> create() {
    return std::make_shared<SliceEndNsSerializer>();
  }
};

} // namespace facebook::velox::stateful
