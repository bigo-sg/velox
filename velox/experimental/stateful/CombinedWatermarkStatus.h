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

#include <climits>
#include <cstdint>
#include <vector>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::stateful {

// This class is relevent to flink CombinedWatermarkStatus.
class CombinedWatermarkStatus {
 public:
  // This class represents a partial watermark from a single input stream.
  class PartialWatermark {
   public:
    bool setWatermark(int64_t watermark) {
      if (watermark < watermark_) {
        // If the new watermark is less than or equal to the current one, we do not update it.
        return false;
      }

      watermark_ = watermark;
      idle_ = false;
      return true;
    }

    bool idle() const {
      return idle_;
    }

    int64_t watermark() const {
      return watermark_;
    }

    void setIdle(bool idle) {
      idle_ = idle;
    }

   private:
    int64_t watermark_ = INT64_MIN;
    bool idle_ = false;
  };

  CombinedWatermarkStatus(int numWatermarks) {
    VELOX_CHECK(numWatermarks > 0, "numWatermarks must be greater than 0");
    partialWatermarks_.resize(numWatermarks);
  }

  bool updateWatermark(int index, int64_t timestamp);

  int64_t getCombinedWatermark();

 private:
  bool updateCombinedWatermark();

  std::vector<PartialWatermark> partialWatermarks_;
  bool idle_ = false;
  int64_t combinedWatermark_ = INT64_MIN;
};

} // namespace facebook::velox::stateful
