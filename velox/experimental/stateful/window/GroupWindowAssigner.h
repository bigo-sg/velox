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

#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/vector/ComplexVector.h"

#include <set>

namespace facebook::velox::stateful {

// This class is relevant to Flink GroupWindowAssigner.
template <typename W, typename = std::enable_if_t<std::is_base_of_v<Window, W>>>
class GroupWindowAssigner {
 public:
  virtual std::vector<W> assignWindows(
      RowVectorPtr element,
      int64_t timestamp) = 0;
  virtual bool isEventTime() = 0;
};

class MergeResultCollector;
class MergingWindowAssigner : public GroupWindowAssigner<TimeWindow> {
 public:
  virtual void mergeWindows(
      TimeWindow newWindow,
      std::set<TimeWindow>& sortedWindows,
      MergeResultCollector& callback) = 0;
};

using GroupWindowAssignerPtr = std::shared_ptr<GroupWindowAssigner<TimeWindow>>;

class SessionWindowAssigner : public MergingWindowAssigner {
 public:
  SessionWindowAssigner(int64_t gap, bool isEventTime);

  std::vector<TimeWindow> assignWindows(RowVectorPtr element, int64_t timestamp)
      override;

  void mergeWindows(
      TimeWindow newWindow,
      std::set<TimeWindow>& sortedWindows,
      MergeResultCollector& callback);

  bool isEventTime() override {
    return isEventTime_;
  }

 private:
  TimeWindow mergeWindow(
      const TimeWindow& curWindow,
      const TimeWindow& other,
      std::set<TimeWindow>& mergedWindow);

  int64_t gap_;
  bool isEventTime_;
};

// Tumbling group window: [start, start + size) with start aligned to the
// size grid — one window per event, never overlapping, never merging. The
// Java template is Flink 1.19 TumblingWindowAssigner.assignWindows
// (getWindowStartWithOffset(timestamp, offset, size); the legacy group
// window planner exposes no user offset, so the offset is the fixed 0).
class TumblingWindowAssigner : public GroupWindowAssigner<TimeWindow> {
 public:
  TumblingWindowAssigner(int64_t size, bool isEventTime)
      : size_(size), isEventTime_(isEventTime) {}

  std::vector<TimeWindow> assignWindows(
      RowVectorPtr element,
      int64_t timestamp) override;

  bool isEventTime() override {
    return isEventTime_;
  }

 private:
  int64_t size_;
  bool isEventTime_;
};

// Sliding group window (HOP): every event belongs to every size-grid window
// [start, start + size) whose start lies in (timestamp - size, timestamp]
// stepping by slide — windows overlap and never merge. The Java template is
// Flink 1.19 SlidingWindowAssigner.assignWindows: lastStart aligned to the
// slide grid, then backwards while start > timestamp - size.
class SlidingWindowAssigner : public GroupWindowAssigner<TimeWindow> {
 public:
  SlidingWindowAssigner(int64_t size, int64_t slide, bool isEventTime)
      : size_(size), slide_(slide), isEventTime_(isEventTime) {}

  std::vector<TimeWindow> assignWindows(
      RowVectorPtr element,
      int64_t timestamp) override;

  bool isEventTime() override {
    return isEventTime_;
  }

 private:
  int64_t size_;
  int64_t slide_;
  bool isEventTime_;
};

class MergeResultCollector {
 public:
  void merge(TimeWindow mergeResult, std::set<TimeWindow> toBeMerged);

  std::map<TimeWindow, std::set<TimeWindow>>& mergeResults() {
    return mergeResults_;
  }

 private:
  std::map<TimeWindow, std::set<TimeWindow>> mergeResults_;
};
} // namespace facebook::velox::stateful
