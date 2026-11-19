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
#include "velox/experimental/stateful/window/GroupWindowAssigner.h"
#include <cstdint>

namespace facebook::velox::stateful {
SessionWindowAssigner::SessionWindowAssigner(int64_t gap, bool isEventTime)
    : gap_(gap), isEventTime_(isEventTime) {}

std::vector<TimeWindow> SessionWindowAssigner::assignWindows(
    RowVectorPtr element,
    int64_t timestamp) {
  return {TimeWindow(timestamp, timestamp + gap_)};
}

void SessionWindowAssigner::mergeWindows(
    TimeWindow newWindow,
    std::set<TimeWindow>& sortedWindows,
    MergeResultCollector& callback) {
  // The neighbors of the new window in the sorted in-flight set, the
  // translation of Java NavigableSet::ceiling / floor: the ceiling is the
  // least window >= newWindow, the floor the greatest window below it (the
  // predecessor of the ceiling). The in-flight windows are pairwise
  // disjoint (they merged when they entered), and a session window spans
  // at least the gap, so at most these two can intersect the new window.
  // The previous translation (upper_bound for the ceiling, lower_bound for
  // the floor) examined only windows at-or-after the new window and missed
  // the predecessor entirely - in-flight windows that start before the new
  // window never merged, the common case for ascending event times.
  auto ceiling = sortedWindows.lower_bound(newWindow);
  auto floor = ceiling != sortedWindows.begin() ? std::prev(ceiling)
                                                : sortedWindows.end();

  std::set<TimeWindow> mergedWindows;
  TimeWindow mergeResult = newWindow;
  if (ceiling != sortedWindows.end()) {
    mergeResult = mergeWindow(mergeResult, *ceiling, mergedWindows);
  }
  if (floor != sortedWindows.end()) {
    mergeResult = mergeWindow(mergeResult, *floor, mergedWindows);
  }
  if (!mergedWindows.empty()) {
    // merge happens, add newWindow into the collection as well.
    mergedWindows.insert(newWindow);
    callback.merge(mergeResult, mergedWindows);
  }
}

TimeWindow SessionWindowAssigner::mergeWindow(
    const TimeWindow& curWindow,
    const TimeWindow& other,
    std::set<TimeWindow>& mergedWindow) {
  if (curWindow.intersects(other)) {
    mergedWindow.insert(other);
    return curWindow.cover(other);
  } else {
    return curWindow;
  }
}

void MergeResultCollector::merge(
    TimeWindow mergeResult,
    std::set<TimeWindow> toBeMerged) {
  mergeResults_.insert({mergeResult, toBeMerged});
}
} // namespace facebook::velox::stateful
