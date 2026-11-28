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
#include <map>

#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/experimental/stateful/window/WindowTimezoneUtil.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::tzdb {
class time_zone;
} // namespace facebook::velox::tzdb

namespace facebook::velox::stateful {

/// Zone-aware slice assigner owned by the window TVF aggregation
/// before boundary alignment (Flink semantics), a null rowtime fails
/// loud (Flink SliceAssigners#assignSliceEnd throws; silently dropping
/// Flink-per-window-type mapping (TUMBLE -> size, HOP -> gcd(size,
class WindowSliceAssigner {
 public:
  WindowSliceAssigner(
      int64_t size,
      int64_t step,
      int64_t offset,
      WindowType windowType,
      int rowtimeIndex,
      const tzdb::time_zone* shiftTimeZone)
      : size_(size),
        step_(step),
        offset_(offset),
        windowType_(windowType),
        rowtimeIndex_(rowtimeIndex),
        shiftTimeZone_(shiftTimeZone) {
    // SliceSize per window type (matching Flink SliceAssigners):
    switch (windowType_) {
      case WindowType::HOP:
        sliceSize_ = std::gcd(size_, step_);
        break;
      case WindowType::CUMULATIVE:
        sliceSize_ = step_;
        break;
      default:
        sliceSize_ = size_;
        break;
    }
  }

  std::map<int64_t, RowVectorPtr> assignSliceEnd(const RowVectorPtr& input);

  /// The start of the window ending at 'windowEnd' (TUMBLE: end - size).
  int64_t getWindowStart(int64_t windowEnd) {
    if (windowType_ == WindowType::CUMULATIVE) {
      // Epoch-aligned group start on the maxSize grid; windowEnd - size
      // is only correct at the last step (H4; Flink
      return TimeWindowUtil::getWindowStartWithOffset(
          windowEnd - 1, offset_, size_);
    }
    return windowEnd - size_;
  }

  /// The last window end owning the slice ending at 'sliceEnd'
  /// to sliceEnd - sliceSize + size, Flink SliceAssigners).
  int64_t getLastWindowEnd(int64_t sliceEnd) {
    if (windowType_ == WindowType::HOP) {
      return sliceEnd - sliceSize_ + size_;
    }
    if (windowType_ == WindowType::CUMULATIVE) {
      // A step slice is owned by every cumulative window of its group
      // from its own step to the group end (Flink
      return getWindowStart(sliceEnd) + size_;
    }
    return sliceEnd;
  }

  int64_t getSliceEndInterval() {
    return sliceSize_;
  }

 private:
  int64_t size_;
  int64_t step_;
  int64_t offset_;
  WindowType windowType_;
  int rowtimeIndex_;
  const tzdb::time_zone* shiftTimeZone_;
  int64_t sliceSize_ = 0;
};

} // namespace facebook::velox::stateful
