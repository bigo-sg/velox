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
#include <chrono>
#include <cstdint>

#include "velox/common/base/Exceptions.h"
#include "velox/external/tzdb/zoned_time.h"

namespace facebook::velox::stateful {

namespace {

using Millis = std::chrono::milliseconds;

// The instant of a local wall clock per java.time ZonedDateTime semantics
// (Flink's LocalDateTime.atZone): an ambiguous wall clock resolves to the
// FIRST (earlier) occurrence - the pre-transition offset; a nonexistent
// one (DST gap) is shifted forward by the gap, which nets to the same
// pre-transition offset: (lt + gap) - offsetAfter == lt - offsetBefore.
int64_t localToInstantMillis(
    const date::local_time<std::chrono::milliseconds>& lt,
    const tzdb::time_zone* zone) {
  const tzdb::local_info info = zone->get_info(lt);
  return (lt.time_since_epoch() - info.first.offset).count();
}

} // namespace

// static
int64_t TimeWindowUtil::getNextTriggerWatermark(
    int64_t currentWatermark,
    int64_t interval,
    int shiftTimezone,
    bool useDayLightSaving) {
  if (currentWatermark == INT64_MAX) {
    return currentWatermark;
  }

  int64_t triggerWatermark;
  // consider the DST timezone
  if (useDayLightSaving) {
    // TODO: support time zone
  } else {
    int64_t start = getWindowStartWithOffset(currentWatermark, 0L, interval);
    triggerWatermark = start + interval - 1;
  }

  if (triggerWatermark > currentWatermark) {
    return triggerWatermark;
  } else {
    return triggerWatermark + interval;
  }
}

// static
const tzdb::time_zone* TimeWindowUtil::locateTimeZone(const std::string& name) {
  if (name.empty() || name == "UTC") {
    return nullptr; // Identity: window math runs in plain UTC millis.
  }
  try {
    return tzdb::locate_zone(name);
  } catch (const std::exception& e) {
    VELOX_FAIL("Unknown shift time zone '{}': {}", name, e.what());
  }
}

// static
int64_t TimeWindowUtil::toUtcTimestampMills(
    int64_t epochMillis,
    const tzdb::time_zone* zone) {
  if (zone == nullptr || epochMillis == INT64_MAX) {
    return epochMillis;
  }
  const auto sys =
      date::sys_time<std::chrono::milliseconds>(Millis(epochMillis));
  const auto local = zone->to_local(sys);
  return local.time_since_epoch().count();
}

// static
int64_t TimeWindowUtil::toEpochMillsForTimer(
    int64_t utcTimestampMillis,
    const tzdb::time_zone* zone) {
  if (zone == nullptr || utcTimestampMillis == INT64_MAX) {
    return utcTimestampMillis;
  }
  constexpr int64_t kMillisPerHour = 3600LL * 1000;
  const auto lt =
      date::local_time<std::chrono::milliseconds>(Millis(utcTimestampMillis));
  const tzdb::local_info info = zone->get_info(lt);
  const int64_t t1 = localToInstantMillis(lt, zone);
  if (info.result == tzdb::local_info::nonexistent) {
    // Flink: the wall-clock hour covers zero real time (DST gap) -> use
    // the first skipped epoch millis, i.e. floor the gap-shifted instant
    // to the hour boundary (Flink uses truncated %, matched here).
    return t1 - t1 % kMillisPerHour;
  }
  if (info.result == tzdb::local_info::ambiguous) {
    // Flink: one wall-clock hour spans two real hours (DST overlap) ->
    // the later epoch, so both hours of data fire properly.
    return t1 + kMillisPerHour;
  }
  return t1;
}

// static
bool TimeWindowUtil::isWindowFired(
    int64_t windowEnd,
    int64_t currentProgress,
    const tzdb::time_zone* zone) {
  if (windowEnd == INT64_MAX) {
    return false;
  }
  const int64_t windowTriggerTime = toEpochMillsForTimer(windowEnd - 1, zone);
  return currentProgress >= windowTriggerTime;
}

// static
int64_t TimeWindowUtil::getWindowStartWithOffset(
    int64_t timestamp,
    int64_t offset,
    int64_t windowSize) {
  if (windowSize <= 0) {
    return timestamp;
  }
  int64_t remainder = (timestamp - offset) % windowSize;
  // handle both positive and negative cases
  if (remainder < 0) {
    return timestamp - (remainder + windowSize);
  } else {
    return timestamp - remainder;
  }
}

int64_t TimeWindowUtil::getCurrentProcessingTime() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

// static
bool TimeWindowUtil::isWindowFired(
    int64_t windowEnd,
    int64_t currentProgress,
    int shiftTimeZone) {
  if (windowEnd == INT64_MAX) {
    return false;
  }
  // TODO: support time zone
  int64_t windowTriggerTime =
      toEpochMillsForTimer(windowEnd - 1, shiftTimeZone);
  return currentProgress >= windowTriggerTime;
}

// static
int64_t TimeWindowUtil::cleanupTime(
    int64_t maxTimestamp,
    int64_t allowedLateness,
    bool isEventTime) {
  if (isEventTime) {
    int64_t cleanupTime = std::max(0L, maxTimestamp + allowedLateness);
    return cleanupTime >= maxTimestamp ? cleanupTime : INT64_MAX;
  } else {
    return std::max(0L, maxTimestamp);
  }
}

// static
int64_t TimeWindowUtil::toEpochMillsForTimer(
    int64_t timestamp,
    int shiftTimeZone) {
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

  auto merged =
      BaseVector::create<RowVector>(datas.front()->type(), totalRows, pool);

  size_t offset = 0;
  for (auto& data : datas) {
    merged->copy(data.get(), offset, 0, data->size());
    offset += data->size();
  }
  return merged;
}

} // namespace facebook::velox::stateful
