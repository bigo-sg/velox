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
#include <vector>

#include "folly/CPortability.h"

#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::flinksql {

// Flink CURRENT_TIMESTAMP / NOW(): TIMESTAMP_LTZ(3) semantics. The value is
// an epoch timestamp; rendering with the session timezone happens in the
// Flink sink layer, so the function itself needs no timezone conversion.
// Evaluated per record (Flink streaming semantics); declared
// non-deterministic so the expression compiler never folds it into a
// constant.
template <typename T>
struct FlinkCurrentTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE bool call(out_type<Timestamp>& result) {
    result = Timestamp::now();
    return true;
  }
};

// Flink LOCALTIMESTAMP: TIMESTAMP(3) semantics. The local wall clock in the
// session timezone, evaluated per record. Equivalent to
// Timestamp::now().toTimezone(sessionTz), matching Flink's streaming codegen
// (now + sessionTimeZone.getOffset(now) per record, both taking the offset at
// the row's own instant).
template <typename T>
struct FlinkLocalTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    // Capture the session timezone (a configuration, not a clock value, so
    // per-record evaluation is preserved).
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      timeZone_ = tz::locateZone(sessionTzName);
    } else {
      timeZone_ = tz::locateZone(0); // GMT fallback
    }
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<Timestamp>& result) {
    result = Timestamp::now();
    result.toTimezone(*timeZone_);
    return true;
  }

 private:
  const tz::TimeZone* timeZone_{tz::locateZone(0)};
};

// Flink LOCALTIME: TIME(0) semantics on the INTEGER carrier. The value is
// the milliseconds-of-day part of the session-timezone wall clock,
// evaluated per record. Flink's streaming codegen derives it as
// timestampMillisToTime(localTimestamp.getMillisecond()), i.e.
// (int)(localWallMillis % 86'400'000) (DateTimeUtils.java:1383-1385 +
// CodeGeneratorContext.addReusableRecordLevelLocalTime) — the same value
// this function produces from Timestamp::now().toTimezone(sessionTz).
// The int is the exact RowData representation of a TIME(0) field, so the
// projection flows back to the sink without any carrier conversion.
template <typename T>
struct FlinkLocalTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    // Capture the session timezone (a configuration, not a clock value, so
    // per-record evaluation is preserved) — same shape as
    // FlinkLocalTimestampFunction.
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      timeZone_ = tz::locateZone(sessionTzName);
    } else {
      timeZone_ = tz::locateZone(0); // GMT fallback
    }
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<int32_t>& result) {
    Timestamp now = Timestamp::now();
    now.toTimezone(*timeZone_);
    // Epoch is ~1.7e12 ms in 2026 and zone offsets are within +/-14h, so
    // the dividend is always positive: the modulo lands in
    // [0, 86'400'000) with no negative-wrap handling needed.
    result = static_cast<int32_t>(now.toMillis() % 86'400'000);
    return true;
  }

 private:
  const tz::TimeZone* timeZone_{tz::locateZone(0)};
};

// Flink CURRENT_DATE: the current SQL date under the session time zone,
// which arrives through QueryConfig::sessionTimezone() (gluten-flink
// populates it from Flink's table.local-time-zone). Streaming-mode Flink
// evaluates CURRENT_DATE per record; this implementation reads the clock
// per row, matching.
template <typename TExec>
struct CurrentDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Date>& result) {
    auto now = Timestamp::now();
    if (timeZone_ != nullptr) {
      now.toTimezone(*timeZone_);
    }
    const std::chrono::
        time_point<std::chrono::system_clock, std::chrono::milliseconds>
            localTimepoint(std::chrono::milliseconds(now.toMillis()));
    result = std::chrono::floor<date::days>((localTimepoint).time_since_epoch())
                 .count();
  }

 private:
  const tz::TimeZone* timeZone_ = nullptr;
};

// Flink CURRENT_TIME: the wall-clock time of day under the session time
// zone, as the millisecond-of-day INT — the same encoding Flink RowData
// uses for TIME, so the gluten bridge maps Flink TIME onto velox INTEGER
// and the value crosses unchanged. Reads the clock per row, matching
// streaming-mode Flink.
template <typename TExec>
struct CurrentTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& result) {
    auto now = Timestamp::now();
    if (timeZone_ != nullptr) {
      now.toTimezone(*timeZone_);
    }
    result = static_cast<int32_t>(now.toMillis() % 86'400'000);
  }

 private:
  const tz::TimeZone* timeZone_ = nullptr;
};

} // namespace facebook::velox::functions::flinksql
