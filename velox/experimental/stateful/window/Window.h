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
#include <string>

namespace facebook::velox::stateful {

// This class is relevant to Flink WindowBuffer.
class Window {
 public:
  virtual long maxTimestamp() = 0;

  virtual bool operator<(const Window& other) const = 0;

  virtual std::string toString() const = 0;
};

class TimeWindow : public Window {
 public:
  TimeWindow() : start_(-1), end_(-1) {}

  TimeWindow(long start, long end) : start_(start), end_(end) {}

  long maxTimestamp() override {
    return end_ - 1;
  }

  long start() const {
    return start_;
  }

  long end() const {
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

 private:
  long start_;
  long end_;
};

} // namespace facebook::velox::stateful

namespace std {
template <>
struct hash<facebook::velox::stateful::TimeWindow> {
  size_t operator()(const facebook::velox::stateful::TimeWindow& w) const {
    // TODO: verify it.
    return hash<long>()(w.start()) ^ (hash<long>()(w.end()) << 1);
  }
};
} // namespace std
