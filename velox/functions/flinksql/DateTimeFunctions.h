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

#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::flinksql {

//===---------------------------------------------------------------------===//
// to_date
//===---------------------------------------------------------------------===//

// Testable parse entries, called directly by unit tests.
// flinkParseDate: line-by-line port of Flink parseDate(String); parse
// failure -> NULL, year out of int32 range -> job failure.
// flinkParseDateWithFormat: LENIENT_SIMPLE engine wrapper; parse failure
// -> NULL, illegal format -> job failure (no clock: the yy pivot is fixed).
std::optional<int32_t> flinkParseDate(std::string_view s);
std::optional<int32_t> flinkParseDateWithFormat(
    std::string_view s,
    std::string_view format);

// Shared tail: normalize to GMT, floor-divide to days, range-check int32.
std::optional<int32_t> toDateDays(
    DateTimeResult& dateTimeResult,
    const tz::TimeZone& fallbackTimeZone);

// One-argument to_date: Flink parseDate semantics; NULL on parse failure,
// job failure on a year out of int32 range.
template <typename TExec>
struct ToDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Varchar>& s) {
    auto days = flinkParseDate(std::string_view(s));
    if (!days.has_value()) {
      return false; // NULL
    }
    result = days.value();
    return true;
  }
};

// Two-argument to_date(s, format) over the shared LENIENT_SIMPLE engine;
// known deviations from Flink's JDK engine are pinned by unit tests.
template <typename TExec>
struct ToDateWithFormatFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // Constant format: compiled once per batch; a bad pattern fails the job.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      formatter_ = buildSimpleDateTimeFormatter(
                       std::string_view(format->data(), format->size()),
                       /*lenient=*/true)
                       .thenOrThrow(folly::identity, [](const Status& status) {
                         VELOX_USER_FAIL("{}", status.message());
                       });
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Varchar>& s,
      const arg_type<Varchar>& format) {
    if (!isConstFormat_) {
      // Non-constant format: built per row; a bad row fails the job.
      formatter_ = buildSimpleDateTimeFormatter(
                       std::string_view(format), /*lenient=*/true)
                       .thenOrThrow(folly::identity, [](const Status& status) {
                         VELOX_USER_FAIL("{}", status.message());
                       });
    }
    auto dateTimeResult = formatter_->parse(std::string_view(s));
    // Parse failure -> NULL (Flink's JDK engine yields a garbage date).
    if (dateTimeResult.hasError()) {
      return false;
    }
    auto days = toDateDays(*dateTimeResult, *gmt_);
    if (!days.has_value()) {
      return false; // NULL
    }
    result = days.value();
    return true;
  }

 private:
  std::shared_ptr<DateTimeFormatter> formatter_;
  bool isConstFormat_ = false;
  // Flink hard-codes UTC: the fallback zone is GMT, not the session zone.
  const tz::TimeZone* gmt_{tz::locateZone(0)};
};

//===---------------------------------------------------------------------===//
// to_timestamp
//===---------------------------------------------------------------------===//

// Testable parse entries, called directly by unit tests.
// flinkParseTimestamp: hand-written mirror of Flink's
// DEFAULT_TIMESTAMP_FORMATTER (java.time SMART) — parse failure / rule
// violation -> NULL; millisecond overflow of huge years -> NULL (Flink
// silently wrapped; deliberate deviation).
// flinkParseTimestampWithFormat: LENIENT_SIMPLE engine wrapper with the three
// to_timestamp deltas (no toGMT / build failure -> NULL / full Timestamp at
// millisecond precision); every failure path -> NULL (no job failure).
std::optional<Timestamp> flinkParseTimestamp(std::string_view s);
std::optional<Timestamp> flinkParseTimestampWithFormat(
    std::string_view s,
    std::string_view format);

// Guarded parse + nanos guard + millisecond truncation over an already-built
// formatter — shared by flinkParseTimestampWithFormat and the constant-format
// fast path of the UDF below.
std::optional<Timestamp> flinkParseTimestampWithFormatter(
    const DateTimeFormatter& formatter,
    std::string_view s);

// One-argument to_timestamp(s): java.time SMART mirror of Flink's
// DEFAULT_TIMESTAMP_FORMATTER ("yyyy-[MM][M]-[dd][d]" + optional
// " [HH][H]:[mm][m]:[ss][s]" + 0-9 fraction digits). Any parse failure -> NULL;
// the function has no job-failure path (Flink's fallibleMethodGen try-catches
// every Throwable into a NULL row).
template <typename TExec>
struct ToTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& s) {
    auto parsed = flinkParseTimestamp(std::string_view(s));
    if (!parsed.has_value()) {
      return false; // NULL
    }
    result = parsed.value();
    return true;
  }
};

// Two-argument to_timestamp(s, format) over the shared LENIENT_SIMPLE engine.
// Known deviations from Flink's java.time SMART + fallback chain (T1-T13,
// user-approved as TODO-only) are pinned by unit tests.
template <typename TExec>
struct ToTimestampWithFormatFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // Constant format: compiled once per batch. A bad pattern must NOT fail the
  // job (Flink's ofPattern IAE is try-caught into a NULL row) — flag it and
  // return NULL for every row.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      auto built = buildSimpleDateTimeFormatter(
          std::string_view(format->data(), format->size()),
          /*lenient=*/true);
      if (built.hasError()) {
        invalidFormat_ = true; // delta-2: no throw, all rows NULL
        return;
      }
      formatter_ = std::move(built.value());
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& s,
      const arg_type<Varchar>& format) {
    if (isConstFormat_) {
      if (invalidFormat_) {
        return false; // constant bad format: every row NULL
      }
      auto parsed =
          flinkParseTimestampWithFormatter(*formatter_, std::string_view(s));
      if (!parsed.has_value()) {
        return false;
      }
      result = parsed.value();
      return true;
    }
    // Non-constant format: per-row build; a bad row is NULL, never a failure
    // (delta-2 — the opposite of to_date's job-failure disposition).
    auto parsed = flinkParseTimestampWithFormat(
        std::string_view(s), std::string_view(format));
    if (!parsed.has_value()) {
      return false;
    }
    result = parsed.value();
    return true;
  }

 private:
  std::shared_ptr<DateTimeFormatter> formatter_;
  bool isConstFormat_ = false;
  bool invalidFormat_ = false; // delta-2: build failure -> constant NULLs
};

//===---------------------------------------------------------------------===//
// to_timestamp_ltz
//===---------------------------------------------------------------------===//

// Flink DateTimeUtils epoch domain (MIN/MAX_EPOCH_SECONDS /
// MIN/MAX_EPOCH_MILLS): 0000-01-01 00:00:00 .. 9999-12-31 23:59:59.999 UTC;
// an out-of-range value is a NULL row (timestampDataFromEpochMills), never a
// failure.
inline constexpr int64_t kFlinkMinEpochSeconds = -62167219200LL;
inline constexpr int64_t kFlinkMaxEpochSeconds = 253402300799LL;
inline constexpr int64_t kFlinkMinEpochMillis = -62167219200000LL;
inline constexpr int64_t kFlinkMaxEpochMillis = 253402300799999LL;

// Flink's TableException text, mirrored (job failure on both stacks).
[[noreturn]] inline void failUnsupportedPrecision(int32_t precision) {
  VELOX_USER_FAIL(
      "The precision value '{}' for function TO_TIMESTAMP_LTZ(numeric, "
      "precision) is unsupported, the supported value is '0' for second or "
      "'3' for millisecond.",
      precision);
}

// timestampDataFromEpochMills mirror: out-of-range -> NULL, else fromMillis
// (negative values floor to a negative second + positive nanos).
inline std::optional<Timestamp> fromEpochMillisOrNull(int64_t millis) {
  if (millis < kFlinkMinEpochMillis || millis > kFlinkMaxEpochMillis) {
    return std::nullopt;
  }
  return Timestamp::fromMillis(millis);
}

// JLS 5.1.3 (double -> long) semantics: NaN -> 0, values at or beyond +-2^63
// saturate to Long.MIN/MAX, the rest truncate toward zero. A plain C++ cast
// is UB on the first two, hence the explicit guards.
inline int64_t javaLongCast(double v) {
  if (std::isnan(v)) {
    return 0;
  }
  if (v >= 9223372036854775808.0) {
    return INT64_MAX;
  }
  if (v <= -9223372036854775808.0) {
    return INT64_MIN;
  }
  return static_cast<int64_t>(v);
}

// Testable cores, called directly by unit tests. Precision must be 0 (seconds)
// or 3 (milliseconds); any other value throws VELOX_USER_FAIL mirroring
// Flink's TableException (a job failure on both stacks).
inline std::optional<Timestamp> flinkToTimestampLtzIntegral(
    int64_t v,
    int32_t precision) {
  switch (precision) {
    case 0:
      if (v < kFlinkMinEpochSeconds || v > kFlinkMaxEpochSeconds) {
        return std::nullopt;
      }
      // No overflow: |v| <= 2.6e11, so v * 1000 <= 2.6e14.
      return fromEpochMillisOrNull(v * 1000);
    case 3:
      return fromEpochMillisOrNull(v);
    default:
      failUnsupportedPrecision(precision);
  }
}

inline std::optional<Timestamp> flinkToTimestampLtzDouble(
    double v,
    int32_t precision) {
  switch (precision) {
    case 3:
      return fromEpochMillisOrNull(javaLongCast(v));
    case 0:
      // The second-domain check runs in double arithmetic first (NaN fails
      // every comparison -> NULL); only then does Flink cast v * 1000.
      if (!(static_cast<double>(kFlinkMinEpochSeconds) <= v &&
            v <= static_cast<double>(kFlinkMaxEpochSeconds))) {
        return std::nullopt;
      }
      return fromEpochMillisOrNull(javaLongCast(v * 1000.0));
    default:
      failUnsupportedPrecision(precision);
  }
}

inline std::optional<Timestamp> flinkToTimestampLtzDecimal(
    int128_t unscaled,
    int32_t scale,
    int32_t precision) {
  // Truncate toward zero to the integral unit (Flink setScale(0, DOWN) /
  // toMillis both round down); int128 division already truncates toward zero.
  const int128_t unit =
      scale > 0 ? unscaled / DecimalUtil::kPowersOfTen[scale] : unscaled;
  switch (precision) {
    case 0:
      if (unit < kFlinkMinEpochSeconds || unit > kFlinkMaxEpochSeconds) {
        return std::nullopt;
      }
      return fromEpochMillisOrNull(static_cast<int64_t>(unit * 1000));
    case 3:
      if (unit < kFlinkMinEpochMillis || unit > kFlinkMaxEpochMillis) {
        return std::nullopt;
      }
      return Timestamp::fromMillis(static_cast<int64_t>(unit));
    default:
      failUnsupportedPrecision(precision);
  }
}

// TO_TIMESTAMP_LTZ(numeric, precision) for the integral family
// (TINYINT/SMALLINT/INTEGER/BIGINT): the C++ type tag binds A.
// TODO(L1): SMALLINT/FLOAT args unreachable; BIGINT precision: no signature.
template <typename TExec>
struct ToTimestampLtzIntegralFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A>
  FOLLY_ALWAYS_INLINE bool
  call(out_type<Timestamp>& result, A v, int32_t precision) {
    auto ts = flinkToTimestampLtzIntegral(static_cast<int64_t>(v), precision);
    if (!ts.has_value()) {
      return false; // NULL (out-of-range epoch)
    }
    result = ts.value();
    return true;
  }
};

// TO_TIMESTAMP_LTZ(double, precision): Java (long) cast semantics (NaN -> 0,
// out-of-range saturation) are mirrored explicitly.
template <typename TExec>
struct ToTimestampLtzDoubleFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE bool
  call(out_type<Timestamp>& result, double v, int32_t precision) {
    auto ts = flinkToTimestampLtzDouble(v, precision);
    if (!ts.has_value()) {
      return false;
    }
    result = ts.value();
    return true;
  }
};

// TO_TIMESTAMP_LTZ(decimal, precision): A binds the unscaled value
// (int64_t for SHORT_DECIMAL, int128_t for LONG_DECIMAL); the scale is taken
// from the input type at initialize time.
// TODO(L2): Flink longValue() wraps huge decimals; GFV range-checks to NULL.
template <typename TExec>
struct ToTimestampLtzDecimalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A>
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      const A* /*unscaled*/,
      const int32_t* /*precision*/) {
    scale_ = getDecimalPrecisionScale(*inputTypes[0]).second;
  }

  template <typename A>
  FOLLY_ALWAYS_INLINE bool
  call(out_type<Timestamp>& result, const A& unscaled, int32_t precision) {
    auto ts = flinkToTimestampLtzDecimal(
        static_cast<int128_t>(unscaled), scale_, precision);
    if (!ts.has_value()) {
      return false;
    }
    result = ts.value();
    return true;
  }

 private:
  uint8_t scale_{0};
};

} // namespace facebook::velox::functions::flinksql
