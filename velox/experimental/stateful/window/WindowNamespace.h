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

/// Namespace for SESSION windows: the window is a (start, end) range,
/// mirroring Flink's TimeWindow namespace. Equality is value-based on both
/// bounds — session merging relocates state to a new window range, so the
/// namespace must compare by range, not by pointer.
class TimeWindowNs : public Namespace {
 public:
  TimeWindowNs(int64_t start, int64_t end) : start_(start), end_(end) {}

  /// Default (0, 0): the heap MapState's stored std::map requires
  /// default-constructible user values (Flink TimeWindow has the same
  /// no-arg constructor for serialization).
  TimeWindowNs() : start_(0), end_(0) {}

  int64_t start() const {
    return start_;
  }

  int64_t end() const {
    return end_;
  }

  bool equals(const Namespace& other) const override {
    auto* that = dynamic_cast<const TimeWindowNs*>(&other);
    return that != nullptr && start_ == that->start_ && end_ == that->end_;
  }

  /// (start, end) lexicographic order, Flink TimeWindow#compareTo; the
  /// ordered in-flight window set (std::map key) relies on it.
  bool operator<(const TimeWindowNs& other) const {
    return start_ < other.start_ ||
        (start_ == other.start_ && end_ < other.end_);
  }

  bool operator==(const TimeWindowNs& other) const {
    return start_ == other.start_ && end_ == other.end_;
  }

  uint64_t hash() const override {
    uint64_t h = static_cast<uint64_t>(start_);
    h = h * 31 + static_cast<uint64_t>(end_);
    h ^= h >> 29;
    h *= 0xbf58476d1ce4e5b3ULL;
    return h;
  }

 private:
  int64_t start_;
  int64_t end_;
};

/// Serializes TimeWindowNs as a (start, end) pair of int64 frames.
class TimeWindowNsSerializer : public TypeSerializer<TimeWindowNs> {
 public:
  std::string schemaId() const override {
    return "time-window";
  }

  std::string serialize(const TimeWindowNs& ns) override {
    std::string out;
    out.resize(2 * sizeof(int64_t));
    int64_t s = ns.start();
    int64_t e = ns.end();
    std::memcpy(out.data(), &s, sizeof(int64_t));
    std::memcpy(out.data() + sizeof(int64_t), &e, sizeof(int64_t));
    return out;
  }

  TimeWindowNs deserialize(std::string_view str) override {
    VELOX_CHECK_EQ(
        str.size(), 2 * sizeof(int64_t), "TimeWindowNs frame must be 16 bytes");
    int64_t start;
    int64_t end;
    std::memcpy(&start, str.data(), sizeof(int64_t));
    std::memcpy(&end, str.data() + sizeof(int64_t), sizeof(int64_t));
    return TimeWindowNs(start, end);
  }
};

template <>
struct NamespaceSerializerTraits<TimeWindowNs> {
  static std::shared_ptr<TypeSerializer<TimeWindowNs>> create() {
    return std::make_shared<TimeWindowNsSerializer>();
  }
};

} // namespace facebook::velox::stateful
