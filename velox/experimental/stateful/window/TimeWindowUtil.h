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

#include <list>
#include <map>
#include <memory>
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink TimeWindowUtil.
class TimeWindowUtil {
 public:
  static int64_t getNextTriggerWatermark(
      int64_t currentWatermark,
      int64_t interval,
      int shiftTimezone,
      bool useDayLightSaving);

  static int64_t getWindowStartWithOffset(
      int64_t timestamp,
      int64_t offset,
      int64_t windowSize);

  static bool
  isWindowFired(int64_t windowEnd, int64_t currentProgress, int shiftTimeZone);

  static RowVectorPtr mergeVectors(
      const std::list<RowVectorPtr>& vectors,
      memory::MemoryPool* pool);

  static int64_t toEpochMillsForTimer(int64_t timestamp, int shiftTimeZone);

  static int64_t
  cleanupTime(int64_t maxTimestamp, int64_t allowedLateness_, bool isEventTime);

  static int64_t getCurrentProcessingTime();
};

} // namespace facebook::velox::stateful
