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

#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/vector/ComplexVector.h"

#include <set>

namespace facebook::velox::stateful {

// This class is relevent to flink GroupWindowAssigner.
template<typename W, typename = std::enable_if_t<std::is_base_of_v<Window, W>>>
class GroupWindowAssigner {
 public:
  virtual std::vector<W> assignWindows(RowVectorPtr element, long timestamp) = 0;
  virtual bool isEventTime() = 0;
};

class MergeResultCollector;
class MergingWindowAssigner : public GroupWindowAssigner<TimeWindow> {
 public:
  virtual void mergeWindows(
    TimeWindow newWindow, std::set<TimeWindow>& sortedWindows, MergeResultCollector& callback) = 0;
};

using GroupWindowAssignerPtr = std::shared_ptr<GroupWindowAssigner<TimeWindow>>;

class SessionWindowAssigner : public MergingWindowAssigner {
 public:
  SessionWindowAssigner(long gap, bool isEventTime);

  std::vector<TimeWindow> assignWindows(RowVectorPtr element, long timestamp) override;

  void mergeWindows(
      TimeWindow newWindow, std::set<TimeWindow>& sortedWindows, MergeResultCollector& callback);

  bool isEventTime() override {
    return isEventTime_;
  }

 private:
  TimeWindow mergeWindow(
      const TimeWindow& curWindow, const TimeWindow& other, std::set<TimeWindow>& mergedWindow);

  long gap_;
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
