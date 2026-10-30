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

#include "velox/functions/Macros.h"

#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/type/Timestamp.h"

#include <date/date.h>
#include <chrono>

#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::functions::flinksql {

namespace detail {

constexpr int64_t kMillisPerDay = 86'400'000;
constexpr int64_t kMicrosPerMilli = 1'000;

// Calendar-aware date add with end-of-month clamping, copied from
// prestosql/DateTimeImpl.h to avoid a cross-namespace include.
// 2022-01-30 + 1 month = 2022-02-28 (clamped).
inline int32_t
addToDate(const int32_t input, const DateTimeUnit unit, const int32_t value) {
  if (value == 0) {
    return input;
  }
  const std::chrono::time_point<std::chrono::system_clock, date::days> inDate{
      date::days(input)};
  std::chrono::time_point<std::chrono::system_clock, date::days> outDate;
  if (unit == DateTimeUnit::kDay) {
    outDate = inDate + date::days(value);
  } else if (unit == DateTimeUnit::kWeek) {
    outDate = inDate + date::days(value * 7);
  } else {
    const date::year_month_day inCalDate(inDate);
    date::year_month_day outCalDate;
    if (unit == DateTimeUnit::kMonth) {
      outCalDate = inCalDate + date::months(value);
    } else if (unit == DateTimeUnit::kQuarter) {
      outCalDate = inCalDate + date::months(3 * value);
    } else if (unit == DateTimeUnit::kYear) {
      outCalDate = inCalDate + date::years(value);
    } else {
      VELOX_UNREACHABLE();
    }
    if (!outCalDate.ok()) {
      outCalDate = outCalDate.year() / outCalDate.month() / date::last;
    }
    outDate = date::sys_days{outCalDate};
  }
  return outDate.time_since_epoch().count();
}

// Timezone-naive wall-calendar timestamp add (Flink semantics).
// Copies the no-timezone overload of addToTimestamp from
// prestosql/DateTimeImpl.h.
inline Timestamp addToTimestamp(
    const Timestamp& timestamp,
    const DateTimeUnit unit,
    const int32_t value) {
  if (value == 0) {
    return timestamp;
  }
  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          inTimestamp(std::chrono::milliseconds(timestamp.toMillis()));
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      outTimestamp;
  switch (unit) {
    case DateTimeUnit::kYear:
    case DateTimeUnit::kQuarter:
    case DateTimeUnit::kMonth:
    case DateTimeUnit::kDay: {
      const int32_t inDate =
          std::chrono::duration_cast<date::days>(inTimestamp.time_since_epoch())
              .count();
      const int32_t outDate = addToDate(inDate, unit, value);
      outTimestamp = inTimestamp + date::days(outDate - inDate);
      break;
    }
    default:
      VELOX_UNREACHABLE();
  }
  Timestamp milliTimestamp =
      Timestamp::fromMillis(outTimestamp.time_since_epoch().count());
  return Timestamp(
      milliTimestamp.getSeconds(),
      milliTimestamp.getNanos() +
          timestamp.getNanos() % static_cast<uint64_t>(kMicrosPerMilli));
}

// Flink adds interval millis to a TIMESTAMP keeping its sub-millisecond part
// (presto's fromMillis(a.toMillis()+b) would truncate it).
inline Timestamp plusMillisKeepMicros(
    const Timestamp& ts,
    const int64_t millis) {
  // Preserve the sub-microsecond nanosecond remainder: toMicros() truncates
  // nanoseconds to microsecond boundaries, so the remainder is saved and
  // re-applied after the arithmetic (sparksql's fromMillis is worse — it
  // truncates to full milliseconds).
  const int64_t subMicroNanos = ts.getNanos() % 1'000;
  Timestamp result =
      Timestamp::fromMicrosNoError(ts.toMicros() + millis * kMicrosPerMilli);
  return Timestamp(result.getSeconds(), result.getNanos() + subMicroNanos);
}

// Flink's DATE + day-time interval keeps the date type. The time-of-day
// part of the interval is truncated toward zero: Calcite evaluates
// date + (intervalMillis / MILLIS_PER_DAY) where the division is Java
// integer division (truncate toward zero). C++ integer division has the
// same semantics, so no adjustment is needed.
inline int32_t plusMillisTruncateToDay(
    const int32_t date,
    const int64_t millis) {
  const int64_t days = millis / kMillisPerDay;
  return static_cast<int32_t>(date + days);
}

} // namespace detail

// Timestamp +/- IntervalDayTime
template <typename T>
struct TimestampPlusIntervalDayTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b) {
    result = detail::plusMillisKeepMicros(a, b);
    return true;
  }
};

template <typename T>
struct TimestampMinusIntervalDayTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b) {
    result = detail::plusMillisKeepMicros(a, -b);
    return true;
  }
};

// Timestamp +/- IntervalYearMonth: Flink uses a timezone-naive wall-calendar
// add with end-of-month clamping; the no-timezone overload of addToTimestamp
// matches exactly (presto's session-timezone round-trip would diverge across
// DST boundaries).
template <typename T>
struct TimestampPlusIntervalYearMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = detail::addToTimestamp(a, DateTimeUnit::kMonth, b);
    return true;
  }
};

template <typename T>
struct TimestampMinusIntervalYearMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = detail::addToTimestamp(a, DateTimeUnit::kMonth, -b);
    return true;
  }
};

// Flink TIMESTAMPADD(HOUR/MINUTE/SECOND, n, DATE) returns a TIMESTAMP:
// days * 86400000 +/- interval millis, with a TIMESTAMP result type.
// Date +/- interval: Flink keeps the DATE type.
template <typename T>
struct DatePlusIntervalDayTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    result = detail::plusMillisTruncateToDay(date, interval);
    return true;
  }
};

template <typename T>
struct DatePlusIntervalYearMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    result = detail::addToDate(date, DateTimeUnit::kMonth, interval);
    return true;
  }
};

template <typename T>
struct DateMinusIntervalDayTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    result = detail::plusMillisTruncateToDay(date, -interval);
    return true;
  }
};

template <typename T>
struct DateMinusIntervalYearMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    result = detail::addToDate(date, DateTimeUnit::kMonth, -interval);
    return true;
  }
};

// Interval * BIGINT for the TIMESTAMPADD(column, ...) rewrite
// ts + (interval * n). Flink wraps silently on extreme products.
template <typename T>
struct IntervalDayTimeMultiplyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<IntervalDayTime>& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<int64_t>& b) {
    result = a * b;
    return true;
  }
};

template <typename T>
struct IntervalYearMonthMultiplyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<IntervalYearMonth>& result,
      const arg_type<IntervalYearMonth>& a,
      const arg_type<int64_t>& b) {
    result = static_cast<int32_t>(static_cast<int64_t>(a) * b);
    return true;
  }
};

} // namespace facebook::velox::functions::flinksql
