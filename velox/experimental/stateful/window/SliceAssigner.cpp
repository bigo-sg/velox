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

#include <numeric>
#include <iostream>

namespace facebook::velox::stateful {

SliceAssigner::SliceAssigner(
    std::unique_ptr<KeySelector> keySelector,
    long size,
    long step,
    long offset,
    int windowType)
    : keySelector_(std::move(keySelector)),
      size_(size),
      step_(step),
      offset_(offset),
      windowType_(windowType) {
  // TODO: calculate sliceSize_ based on windowType.
  sliceSize_ = std::gcd(size, step);
}

std::map<uint32_t, RowVectorPtr> SliceAssigner::assignSliceEnd(const RowVectorPtr& input) {
  return keySelector_->partition(input);
}

long SliceAssigner::getLastWindowEnd(long sliceEnd) {
  if (windowType_ == 0) { // Hopping window
    return sliceEnd - sliceSize_ + size_;
  }
  return sliceEnd;
}

long SliceAssigner::getWindowStart(long windowEnd) {
  return windowEnd - size_;
}

long SliceAssigner::getSliceEndInterval() {
  return sliceSize_;
}

} // namespace facebook::velox::stateful
