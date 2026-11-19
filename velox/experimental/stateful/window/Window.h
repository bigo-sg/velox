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

#include <algorithm>
#include <string>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/state/Namespace.h"

namespace facebook::velox::stateful {

enum class WindowType : int { HOP = 0, TUMBLE, SESSION, CUMULATIVE };

// This class is relevant to Flink WindowBuffer.
class Window {
 public:
  virtual int64_t maxTimestamp() = 0;

  virtual bool operator<(const Window& other) const = 0;

  virtual std::string toString() const = 0;

  static WindowType getType(const int32_t t) {
    if (t >= 0 && t <= 3) {
      return static_cast<WindowType>(t);
    } else {
      VELOX_FAIL(
          "Window type value {} is illegal, it is not between 0 and 3", t);
    }
  }
};

/// A time window: [start, end) in epoch milliseconds. Also carries the
/// window-namespace identity of the typed keyed state (Namespace.h notes the
/// window operator's namespace "should be named after its domain concept (the
/// window)"): the same object is the state address's second dimension, so a
/// window equals another window exactly when their [start, end) pairs are
/// equal. The dual inheritance is additive - Window's interface and the TVF
/// window operators are untouched (user gate 1 decision #12, scheme 1).
class TimeWindow : public Window, public Namespace {
 public:
  TimeWindow() : start_(-1), end_(-1) {}

  TimeWindow(int64_t start, int64_t end) : start_(start), end_(end) {}

  int64_t maxTimestamp() override {
    return end_ - 1;
  }

  int64_t start() const {
    return start_;
  }

  int64_t end() const {
    return end_;
  }

  bool intersects(const TimeWindow& other) const {
    return start_ <= other.end() && end() >= other.start_;
  }

  TimeWindow cover(const TimeWindow& other) const {
    return TimeWindow(
        std::min(start_, other.start()), std::max(end_, other.end()));
  }

  bool operator<(const Window& other) const override {
    const TimeWindow& otherTimeWindow = static_cast<const TimeWindow&>(other);
    return start_ < otherTimeWindow.start_ ||
        (start_ == otherTimeWindow.start_ && end_ < otherTimeWindow.end_);
  }

  bool operator==(const Window& other) const {
    const TimeWindow& otherTimeWindow = static_cast<const TimeWindow&>(other);
    return (start_ == otherTimeWindow.start_ && end_ == otherTimeWindow.end_);
  }

  std::string toString() const override {
    return "Window[start=" + std::to_string(start_) +
        ", end=" + std::to_string(end_) + "]";
  }

  // Add for checking whether the window is in state.
  bool valid() const {
    return start_ >= 0 && end_ >= start_;
  }

  uint64_t hashCode() const {
    return bits::hashMix(end_, start_);
  }

  /// Namespace identity of the typed keyed state: forwards to the existing
  /// value equality / hash over (start, end).
  bool equals(const Namespace& other) const override {
    auto* otherWindow = dynamic_cast<const TimeWindow*>(&other);
    return otherWindow != nullptr && start_ == otherWindow->start_ &&
        end_ == otherWindow->end_;
  }

  uint64_t hash() const override {
    return hashCode();
  }

 private:
  int64_t start_;
  int64_t end_;
};

} // namespace facebook::velox::stateful

namespace std {
template <>
struct hash<facebook::velox::stateful::TimeWindow> {
  size_t operator()(const facebook::velox::stateful::TimeWindow& w) const {
    // TODO: verify it.
    return hash<int64_t>()(w.start()) ^ (hash<int64_t>()(w.end()) << 1);
  }
};
} // namespace std
