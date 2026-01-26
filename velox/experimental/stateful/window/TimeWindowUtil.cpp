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
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

#include <climits>

namespace facebook::velox::stateful {

// static
int64_t TimeWindowUtil::getNextTriggerWatermark(
    int64_t currentWatermark,
    long interval,
    int shiftTimezone,
    bool useDayLightSaving) {
  if (currentWatermark == INT64_MAX) {
      return currentWatermark;
  }

  int64_t triggerWatermark;
  // consider the DST timezone
  if (useDayLightSaving) {
    // TODO: support time zone
    //long utcWindowStart =
    //          getWindowStartWithOffset(
    //                  toUtcTimestampMills(currentWatermark, shiftTimezone), 0L, interval);
    //triggerWatermark = toEpochMillsForTimer(utcWindowStart + interval - 1, shiftTimezone);
  } else {
    long start = getWindowStartWithOffset(currentWatermark, 0L, interval);
    triggerWatermark = start + interval - 1;
  }

  if (triggerWatermark > currentWatermark) {
    return triggerWatermark;
  } else {
    return triggerWatermark + interval;
  }
}

// static
long TimeWindowUtil::getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
  long remainder = (timestamp - offset) % windowSize;
  // handle both positive and negative cases
  if (remainder < 0) {
      return timestamp - (remainder + windowSize);
  } else {
      return timestamp - remainder;
  }
}

// static
bool TimeWindowUtil::isWindowFired(
    long windowEnd, long currentProgress, int shiftTimeZone) {
  if (windowEnd == LONG_MAX) {
    return false;
  }
  // TODO: support time zone
  long windowTriggerTime = toEpochMillsForTimer(windowEnd - 1, shiftTimeZone);
  return currentProgress >= windowTriggerTime;
}

// static
long TimeWindowUtil::cleanupTime(long maxTimestamp, long allowedLateness, bool isEventTime) {
  if (isEventTime) {
    long cleanupTime = std::max(0L, maxTimestamp + allowedLateness);
    return cleanupTime >= maxTimestamp ? cleanupTime : LONG_MAX;
  } else {
    return std::max(0L, maxTimestamp);
  }
}

// static
long TimeWindowUtil::toEpochMillsForTimer(long timestamp, int shiftTimeZone) {
  // TODO: support time zone
  return timestamp;
}

// static
RowVectorPtr TimeWindowUtil::mergeVectors(
    const std::list<RowVectorPtr>& datas,
    memory::MemoryPool* pool) {
  if (datas.empty()) {
    return nullptr;
  }
  // TODO: refine it
  auto numColumns = datas.front()->childrenSize();
  std::vector<VectorPtr> mergedColumns(numColumns);

  size_t totalRows = 0;
  for (const auto& data : datas) {
    totalRows += data->size();
  }

  auto merged = BaseVector::create<RowVector>(
      datas.front()->type(),
      totalRows,
      pool);

  size_t offset = 0;
  for (auto& data : datas) {
    merged->copy(data.get(), offset, 0, data->size());
    offset += data->size();
  }
  return merged;
}
} // namespace facebook::velox::stateful
