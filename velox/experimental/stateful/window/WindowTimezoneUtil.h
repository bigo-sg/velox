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

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>

#include "velox/common/base/Exceptions.h"
#include "velox/external/tzdb/local_info.h"
#include "velox/external/tzdb/time_zone.h"
#include "velox/external/tzdb/zoned_time.h"

namespace facebook::velox::stateful {

namespace wtutil {

using Millis = std::chrono::milliseconds;

// The instant of a local wall clock per java.time ZonedDateTime semantics
// (Flink's LocalDateTime.atZone): an ambiguous wall clock resolves to the
// FIRST (earlier) occurrence - the pre-transition offset; a nonexistent
// one (DST gap) is shifted forward by the gap, which nets to the same
// pre-transition offset: (lt + gap) - offsetAfter == lt - offsetBefore.
inline int64_t localToInstantMillis(
    const date::local_time<wtutil::Millis>& lt,
    const tzdb::time_zone* zone) {
  const tzdb::local_info info = zone->get_info(lt);
  return (lt.time_since_epoch() - info.first.offset).count();
}

} // namespace wtutil

/// Zone-aware window math for the window TVF aggregation operator.
/// Operator-owned (the framework TimeWindowUtil stays untouched); mirrors
/// Flink TimeWindowUtil function for function, including the DST gap and
/// overlap handling (the DST unit tests pin Flink's javadoc example
/// values).
class WindowTimezoneUtil {
 public:
  /// Flink TimeWindowUtil#locateTimeZone: "" and "UTC" mean no shift
  /// (nullptr keeps the zone-aware overloads identity); unknown names
  /// fail loud.
  static const tzdb::time_zone* locateTimeZone(const std::string& name) {
    if (name.empty() || name == "UTC") {
      return nullptr; // Identity: window math runs in plain UTC millis.
    }
    try {
      return tzdb::locate_zone(name);
    } catch (const std::exception& e) {
      VELOX_FAIL("Unknown shift time zone '{}': {}", name, e.what());
    }
  }

  /// Flink TimeWindowUtil#toUtcTimestampMills: reinterpret the
  /// zone-local wall clock of 'epochMillis' as UTC epoch millis
  /// (input-side shift before window alignment).
  static int64_t toUtcTimestampMills(
      int64_t epochMillis,
      const tzdb::time_zone* zone) {
    if (zone == nullptr || epochMillis == std::numeric_limits<int64_t>::max()) {
      return epochMillis;
    }
    const auto sys =
        date::sys_time<wtutil::Millis>(wtutil::Millis(epochMillis));
    const auto local = zone->to_local(sys);
    return local.time_since_epoch().count();
  }

  /// Flink TimeWindowUtil#toEpochMillsForTimer (zone-aware): the epoch
  /// millis to register a timer at. DST gap -> the wall-clock hour
  /// covers zero real time, floor the gap-shifted instant to the hour
  /// boundary (Flink's truncated %); DST overlap -> one wall-clock hour
  /// spans two real hours, take the later epoch so both hours of data
  /// fire properly.
  static int64_t toEpochMillsForTimer(
      int64_t utcTimestampMillis,
      const tzdb::time_zone* zone) {
    if (zone == nullptr ||
        utcTimestampMillis == std::numeric_limits<int64_t>::max()) {
      return utcTimestampMillis;
    }
    constexpr int64_t kMillisPerHour = 3600LL * 1000;
    const auto lt =
        date::local_time<wtutil::Millis>(wtutil::Millis(utcTimestampMillis));
    const tzdb::local_info info = zone->get_info(lt);
    const int64_t t1 = wtutil::localToInstantMillis(lt, zone);
    if (info.result == tzdb::local_info::nonexistent) {
      return t1 - t1 % kMillisPerHour;
    }
    if (info.result == tzdb::local_info::ambiguous) {
      return t1 + kMillisPerHour;
    }
    return t1;
  }

  /// Flink TimeWindowUtil#isWindowFired (zone-aware).
  static bool isWindowFired(
      int64_t windowEnd,
      int64_t currentProgress,
      const tzdb::time_zone* zone) {
    if (windowEnd == std::numeric_limits<int64_t>::max()) {
      return false;
    }
    const int64_t windowTriggerTime = toEpochMillsForTimer(windowEnd - 1, zone);
    return currentProgress >= windowTriggerTime;
  }

  /// Wall-clock epoch millis (Flink System.currentTimeMillis).
  static int64_t getCurrentProcessingTime() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }
};

} // namespace facebook::velox::stateful
