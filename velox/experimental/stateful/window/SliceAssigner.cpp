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
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include <numeric>

namespace facebook::velox::stateful {

SliceAssigner::SliceAssigner(
    std::unique_ptr<KeySelector> keySelector,
    int64_t size,
    int64_t step,
    int64_t offset,
    WindowType windowType,
    int rowtimeIndex)
    : keySelector_(std::move(keySelector)),
      size_(size),
      step_(step),
      offset_(offset),
      windowType_(windowType),
      rowtimeIndex_(rowtimeIndex) {
  // TODO: calculate sliceSize_ based on windowType.
  sliceSize_ = std::gcd(size, step);
}

std::map<int64_t, RowVectorPtr> SliceAssigner::assignSliceEnd(const RowVectorPtr& input) {
  if (rowtimeIndex_ < 0) {
    int64_t timestampMs = TimeWindowUtil::getCurrentProcessingTime();
    if (windowType_ == WindowType::TUMBLE) {
      int64_t utcTimestamp = TimeWindowUtil::toEpochMillsForTimer(timestampMs, 0);
      int64_t windowStart = stateful::TimeWindowUtil::getWindowStartWithOffset(utcTimestamp, offset_, size_);
      return {{windowStart + size_, input}};
    } else {
      return {{timestampMs, input}};
    }
  }
  return keySelector_->partition(input);
}

int64_t SliceAssigner::getLastWindowEnd(int64_t sliceEnd) {
  if (windowType_ == WindowType::HOP) { // Hopping window
    return sliceEnd - sliceSize_ + size_;
  }
  return sliceEnd;
}

int64_t SliceAssigner::getWindowStart(int64_t windowEnd) {
  return windowEnd - size_;
}

int64_t SliceAssigner::getSliceEndInterval() {
  return sliceSize_;
}

} // namespace facebook::velox::stateful
