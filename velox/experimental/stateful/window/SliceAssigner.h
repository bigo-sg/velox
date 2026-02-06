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

#include "velox/experimental/stateful/KeySelector.h"

namespace facebook::velox::stateful {

/// This class is relevant to Flink SliceAssigner.
class SliceAssigner {
 public:
  SliceAssigner(
      std::unique_ptr<KeySelector> keySelector,
      int64_t size,
      int64_t step,
      int64_t offset,
      int windowType,
      int rowtimeIndex);

  std::map<uint32_t, RowVectorPtr> assignSliceEnd(const RowVectorPtr& input);

  int64_t getLastWindowEnd(int64_t sliceEnd);

  int64_t getWindowStart(int64_t windowEnd);

  // Iterable<Long> expiredSlices(int64_t windowEnd);

  int64_t getSliceEndInterval();

 private:
  const std::unique_ptr<KeySelector> keySelector_;
  const int64_t size_;
  const int64_t step_;
  const int64_t offset_;
  const int windowType_; // 0: hopping window, 1: slide window
  int64_t sliceSize_;
  int rowtimeIndex_;
};

} // namespace facebook::velox::stateful
