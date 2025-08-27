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

#include "velox/experimental/stateful/KeySelector.h"

namespace facebook::velox::stateful {

/// This class is relevent to flink SliceAssginer.
class SliceAssigner {
 public:
  SliceAssigner(
      std::unique_ptr<KeySelector>  keySelector,
      long size,
      long step,
      long offset,
      int windowType,
      int rowtimeIndex);

  std::map<uint32_t, RowVectorPtr> assignSliceEnd(const RowVectorPtr& input);

  long getLastWindowEnd(long sliceEnd);

  long getWindowStart(long windowEnd);

  // Iterable<Long> expiredSlices(long windowEnd);

  long getSliceEndInterval();

 private:

  const std::unique_ptr<KeySelector> keySelector_;
  const long size_;
  const long step_;
  const long offset_;
  const int windowType_; // 0: hopping window, 1: slide window
  long sliceSize_;
  int rowtimeIndex_;
};

} // namespace facebook::velox::stateful
