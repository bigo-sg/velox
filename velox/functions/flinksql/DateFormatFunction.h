/*
 * Copyright (c) Contributors to the GFVBot project.
 * Licensed under the Apache License, Version 2.0.
 *
 * Flink's DATE_FORMAT on a TIMESTAMP (no timezone) renders the wall clock:
 * the wall-as-UTC storage millis are formatted in UTC, mirroring Flink's
 * DateTimeUtils.formatTimestamp(TimestampData, String) which hardcodes
 * ZoneId.of("UTC"). The session timezone does NOT apply to the no-timezone
 * type — unlike FROM_UNIXTIME whose epoch input is a real moment and is
 * rendered in the session timezone.
 *
 * DATE_FORMAT on TIMESTAMP WITH TIME ZONE renders the real moment in the
 * session timezone (a TIMESTAMP_LTZ carries a genuine instant, so
 * zone-based rendering is correct — same policy as FROM_UNIXTIME).
 */
#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "folly/CPortability.h"

#include "velox/common/base/Exceptions.h"
#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::flinksql::detail {

// Shared with FromUnixtimeFunction: reads sessionTimezone() directly,
// bypassing the adjust_timestamp_to_timezone flag (that flag governs
// TIMESTAMP storage/comparison, not rendering).
inline const tz::TimeZone* getSessionTimeZoneForRendering(
    const core::QueryConfig& config) {
  const auto sessionTzName = config.sessionTimezone();
  if (!sessionTzName.empty()) {
    return tz::locateZone(sessionTzName);
  }
  return nullptr;
}

} // namespace facebook::velox::functions::flinksql::detail

namespace facebook::velox::functions::flinksql {

// DATE_FORMAT(timestamp, format) over TIMESTAMP (no timezone):
// renders the wall clock. The session timezone does NOT apply.
template <typename T>
struct DateFormatFlinkFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    // Deliberately do NOT read the session timezone: Flink renders the
    // no-timezone TIMESTAMP as its wall clock regardless of the session zone.
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      setFormatter(formatString);
    }
    result.reserve(maxResultSize_);
    const auto resultSize = formatter_->format(
        timestamp, /*zone=*/nullptr, maxResultSize_, result.data(), true);
    result.resize(resultSize);
    return true;
  }

 private:
  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar>& formatString) {
    auto formatter = buildJodaDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()));
    VELOX_USER_CHECK(!formatter.hasError(), "Invalid date format pattern");
    formatter_ = std::move(formatter.value());
    maxResultSize_ = formatter_->maxResultSize(nullptr);
  }

  std::shared_ptr<DateTimeFormatter> formatter_;
  uint32_t maxResultSize_{0};
  bool isConstFormat_{false};
};

// DATE_FORMAT(timestamp, format) over TIMESTAMP WITH TIME ZONE:
// renders the real moment in the session timezone (a TIMESTAMP_LTZ
// carries a genuine instant — same policy as FROM_UNIXTIME). This
// diverges from the inherited sparksql implementation, which renders
// in the value's embedded zone instead of the session zone.
template <typename T>
struct DateFormatWithTzFlinkFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<TimestampWithTimezone>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = detail::getSessionTimeZoneForRendering(config);
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      setFormatter(formatString);
    }
    // The packed value is (utcMillis << shift) | zoneKey; dereference the
    // view to the underlying int64_t before unpacking.
    const auto timestamp = unpackTimestampUtc(*timestampWithTimezone);
    result.reserve(maxResultSize_);
    const auto resultSize = formatter_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data(), true);
    result.resize(resultSize);
    return true;
  }

 private:
  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar>& formatString) {
    auto formatter = buildJodaDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()));
    VELOX_USER_CHECK(!formatter.hasError(), "Invalid date format pattern");
    formatter_ = std::move(formatter.value());
    maxResultSize_ = formatter_->maxResultSize(sessionTimeZone_);
  }

  const tz::TimeZone* sessionTimeZone_{nullptr};
  std::shared_ptr<DateTimeFormatter> formatter_;
  uint32_t maxResultSize_{0};
  bool isConstFormat_{false};
};

} // namespace facebook::velox::functions::flinksql
