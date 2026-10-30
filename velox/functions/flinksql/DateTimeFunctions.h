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

#include <cctype>
#include <chrono>
#include <cstring>
#include <limits>
#include <string_view>

#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/DateTimeFormatterBuilder.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::functions::flinksql {

namespace detail {

// Flink renders an empty SimpleDateFormat pattern to an empty string; the
// Joda builder rejects an empty pattern, so build a token-less formatter
// instead.
inline std::shared_ptr<DateTimeFormatter> emptyJodaFormatter() {
  DateTimeFormatterBuilder builder(0);
  builder.setType(DateTimeFormatterType::JODA);
  return builder.build();
}

// Builds a Joda formatter, returning nullptr on an invalid pattern so the
// caller can defer the failure to call(): Flink never touches the formatter
// for all-null rows, so an invalid constant format must only fail the query
// when at least one row is actually evaluated.
inline std::shared_ptr<DateTimeFormatter> tryGetJodaFormatter(
    const std::string_view format) {
  if (format.empty()) {
    return emptyJodaFormatter();
  }
  auto formatter = buildJodaDateTimeFormatter(format);
  if (formatter.hasError()) {
    return nullptr;
  }
  return formatter.value();
}

// Flink lenient behavior for DST gap times: Java SimpleDateFormat
// interprets a non-existent local time (spring-forward gap) using the
// pre-transition UTC offset. Velox's toGMT() throws instead; this helper
// recovers by computing the epoch with the pre-gap offset, which is found
// by converting a guaranteed-to-exist time (one second before the gap).
inline void resolveDstGap(Timestamp& timestamp, const tz::TimeZone& tz) {
  const auto wall = std::chrono::seconds(timestamp.getSeconds());
  // One second before the gap is guaranteed to exist as a local time.
  const auto utcBefore = tz.to_sys(
      wall - std::chrono::seconds(1), tz::TimeZone::TChoose::kEarliest);
  // Pre-transition offset (positive for east of UTC).
  const auto offset = (wall - std::chrono::seconds(1)) - utcBefore;
  // Apply the pre-transition offset to the gap time (Java lenient).
  timestamp = Timestamp((wall - offset).count(), timestamp.getNanos());
}

// Flink renders FROM_UNIXTIME in the session timezone regardless of the
// adjust_timestamp_to_timezone flag (that flag governs TIMESTAMP
// storage/comparison, not rendering).
inline const tz::TimeZone* getSessionTimeZone(const core::QueryConfig& config) {
  const auto sessionTzName = config.sessionTimezone();
  if (!sessionTzName.empty()) {
    return tz::locateZone(sessionTzName);
  }
  return nullptr;
}

// Flink's SimpleDateFormat is lenient in two ways that velox's Joda
// formatter is not:
// 1. Calendar-invalid dates (e.g. '2025-02-30') roll to the next valid
//    date ('2025-03-02') via field overflow (Java Calendar lenient mode).
// 2. Trailing text after a valid prefix is ignored (e.g.
//    '2026-09-11 10:00:00 xyz' parses as '2026-09-11 10:00:00').
//
// Lenient calendar algorithm (mathematically equivalent to Java
// GregorianCalendar.computeTime()):
//   totalMonths = year * 12 + (month - 1)
//   normYear    = totalMonths / 12
//   normMonth   = totalMonths % 12 + 1
//   epochDays   = daysSinceEpoch(normYear, normMonth, 1) + (day - 1)
//   epochSecs   = epochDays * 86400 + hour * 3600 + minute * 60 + second
// Day/month/hour/minute/second overflow carries into the next field
// naturally (month 13 -> next year Jan, day 30 in Feb -> Mar 2, etc.).

// Raw field values extracted from the input string, without any range
// validation. Defaults match Flink's Calendar defaults (Jan 1, 00:00:00).
struct LenientDate {
  int32_t year = 1970;
  int32_t month = 1;
  int32_t day = 1;
  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t dayOfYear = -1; // -1 = not present
  bool hasYear = false;
  bool isAm = true; // AM/PM
  bool hasAmPm = false;
  bool isHourOfHalfDay = false; // hh or KK (1-12)
};

// Extracts a numeric field of at least minDigits digits from input.
// Returns the value and advances cur, or returns -1.
inline int64_t
extractNumeric(const char*& cur, const char* end, size_t minDigits) {
  const char* start = cur;
  int64_t value = 0;
  while (cur < end && std::isdigit(static_cast<unsigned char>(*cur)) &&
         static_cast<size_t>(cur - start) < minDigits + 10) {
    value = value * 10 + (*cur - '0');
    ++cur;
  }
  if (static_cast<size_t>(cur - start) < minDigits) {
    return -1;
  }
  return value;
}

// Extracts text month (Jan/Feb/... or full names) and returns 1-12, or -1.
inline int32_t extractMonthText(const char*& cur, const char* end) {
  static const std::pair<std::string_view, int32_t> months[] = {
      {"Jan", 1},
      {"Feb", 2},
      {"Mar", 3},
      {"Apr", 4},
      {"May", 5},
      {"Jun", 6},
      {"Jul", 7},
      {"Aug", 8},
      {"Sep", 9},
      {"Oct", 10},
      {"Nov", 11},
      {"Dec", 12},
  };
  static const std::pair<std::string_view, int32_t> fullMonths[] = {
      {"January", 1},
      {"February", 2},
      {"March", 3},
      {"April", 4},
      {"May", 5},
      {"June", 6},
      {"July", 7},
      {"August", 8},
      {"September", 9},
      {"October", 10},
      {"November", 11},
      {"December", 12},
  };
  if (end - cur >= 9) {
    for (const auto& [name, val] : fullMonths) {
      if (end - cur >= static_cast<int64_t>(name.size()) &&
          std::strncmp(cur, name.data(), name.size()) == 0) {
        cur += name.size();
        return val;
      }
    }
  }
  if (end - cur >= 3) {
    for (const auto& [name, val] : months) {
      if (end - cur >= 3 && std::strncmp(cur, name.data(), 3) == 0) {
        cur += 3;
        return val;
      }
    }
  }
  return -1;
}

// Extracts AM/PM and advances cur, or returns -1.
inline int32_t extractAmPm(const char*& cur, const char* end) {
  if (end - cur >= 2) {
    if (std::strncmp(cur, "AM", 2) == 0 || std::strncmp(cur, "am", 2) == 0) {
      cur += 2;
      return 1; // AM
    }
    if (std::strncmp(cur, "PM", 2) == 0 || std::strncmp(cur, "pm", 2) == 0) {
      cur += 2;
      return 0; // PM
    }
  }
  return -1;
}

// Skips a day-of-week text token (Mon/Monday, etc.) without using its value.
// Java Calendar ignores day-of-week when a full date is present.
inline bool skipDayOfWeekText(const char*& cur, const char* end) {
  static const std::string_view days[] = {
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
      "Sunday",
      "Mon",
      "Tue",
      "Wed",
      "Thu",
      "Fri",
      "Sat",
      "Sun",
  };
  for (const auto& name : days) {
    if (end - cur >= static_cast<int64_t>(name.size()) &&
        std::strncmp(cur, name.data(), name.size()) == 0) {
      cur += name.size();
      return true;
    }
  }
  return false;
}

// Walks formatter tokens and extracts raw field values from input.
// Returns true if all tokens matched (literals matched, fields extracted).
// Does NOT validate field ranges — lenient carry rules handle overflow.
inline bool extractFields(
    const DateTimeFormatter& formatter,
    const std::string_view input,
    LenientDate& date) {
  const char* cur = input.data();
  const char* end = input.data() + input.size();
  const auto& tokens = formatter.tokens();

  for (const auto& token : tokens) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      // Match literal text (must match exactly)
      const auto& lit = token.literal;
      if (end - cur < static_cast<int64_t>(lit.size()) ||
          std::strncmp(cur, lit.data(), lit.size()) != 0) {
        return false;
      }
      cur += lit.size();
      continue;
    }

    const auto& spec = token.pattern.specifier;
    const size_t minDigits = token.pattern.minRepresentDigits;

    switch (spec) {
      case DateTimeFormatSpecifier::YEAR:
      case DateTimeFormatSpecifier::YEAR_OF_ERA: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.year = static_cast<int32_t>(v);
        date.hasYear = true;
        break;
      }
      case DateTimeFormatSpecifier::CENTURY_OF_ERA: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.year = static_cast<int32_t>(v) * 100;
        date.hasYear = true;
        break;
      }
      case DateTimeFormatSpecifier::MONTH_OF_YEAR: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.month = static_cast<int32_t>(v);
        break;
      }
      case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT: {
        int32_t v = extractMonthText(cur, end);
        if (v < 0)
          return false;
        date.month = v;
        break;
      }
      case DateTimeFormatSpecifier::DAY_OF_MONTH: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.day = static_cast<int32_t>(v);
        break;
      }
      case DateTimeFormatSpecifier::DAY_OF_YEAR: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.dayOfYear = static_cast<int32_t>(v);
        break;
      }
      case DateTimeFormatSpecifier::HOUR_OF_DAY: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.hour = static_cast<int32_t>(v);
        break;
      }
      case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.hour = static_cast<int32_t>(v);
        date.isHourOfHalfDay = true;
        break;
      }
      case DateTimeFormatSpecifier::MINUTE_OF_HOUR: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.minute = static_cast<int32_t>(v);
        break;
      }
      case DateTimeFormatSpecifier::SECOND_OF_MINUTE: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        date.second = static_cast<int32_t>(v);
        break;
      }
      case DateTimeFormatSpecifier::FRACTION_OF_SECOND: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        // Fraction is truncated toward zero in unix_timestamp; ignore value.
        break;
      }
      case DateTimeFormatSpecifier::HALFDAY_OF_DAY: {
        int32_t v = extractAmPm(cur, end);
        if (v < 0)
          return false;
        date.hasAmPm = true;
        date.isAm = (v == 1);
        break;
      }
      case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT: {
        // Java ignores day-of-week when a full date is present.
        if (!skipDayOfWeekText(cur, end))
          return false;
        break;
      }
      case DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED:
      case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED: {
        int64_t v = extractNumeric(cur, end, minDigits);
        if (v < 0)
          return false;
        // Day-of-week is ignored when a full date is present.
        break;
      }
      default:
        // TIMEZONE, WEEK_YEAR, etc. — not supported in lenient parsing.
        return false;
    }
  }
  // All tokens consumed successfully.
  return cur == end || std::isspace(static_cast<unsigned char>(*cur));
}

// Computes the wall-clock epoch seconds from raw field values using
// Java Calendar's lenient carry rules. All fields participate in
// arithmetic and overflow carries into the next larger field.
inline int64_t lenientEpochSeconds(const LenientDate& date) {
  // Apply AM/PM to hour if present.
  int32_t hour = date.hour;
  if (date.hasAmPm && date.isHourOfHalfDay) {
    if (!date.isAm && hour < 12) {
      hour += 12;
    } else if (date.isAm && hour == 12) {
      hour = 0;
    }
  }

  // Handle day-of-year format (no month/day split).
  if (date.dayOfYear >= 1) {
    // Jan 1 of the year + (dayOfYear - 1) days.
    // Month is not used in this mode.
    const int64_t jan1Days =
        util::daysSinceEpochFromDate(date.year, 1, 1).value();
    return jan1Days * 86400 + (date.dayOfYear - 1) * 86400 + hour * 3600 +
        date.minute * 60 + date.second;
  }

  // Month carry: month 13 -> next year Jan; month 0 -> prev year Dec.
  const int64_t totalMonths =
      static_cast<int64_t>(date.year) * 12 + (date.month - 1);
  const int32_t normYear = static_cast<int32_t>(totalMonths / 12);
  const int32_t normMonth = static_cast<int32_t>(totalMonths % 12) + 1;

  // Day carry: day 30 in Feb -> Mar 2 (daysSinceEpoch of 1st + day-1).
  const int64_t firstOfMonth =
      util::daysSinceEpochFromDate(normYear, normMonth, 1).value();
  const int64_t epochDays = firstOfMonth + (date.day - 1);

  // Time carry: hour 25 -> next day 1:00, etc.
  return epochDays * 86400 + hour * 3600 + date.minute * 60 + date.second;
}

// Parses input with Flink lenient semantics. First tries the full
// strict Joda parse; on failure, retries with progressively shorter
// whitespace-delimited prefixes for trailing text tolerance; if that
// still fails, falls back to raw field extraction with calendar
// leniency. Returns Long.MIN_VALUE sentinel if nothing works.
inline folly::Expected<DateTimeResult, Status> parseLenient(
    const std::shared_ptr<DateTimeFormatter>& formatter,
    const std::string_view input) {
  // 1. Try strict Joda parse (handles well-formed input).
  auto result = formatter->parse(input);
  if (!result.hasError()) {
    return result;
  }

  // 2. Trailing-text tolerance: try longest whitespace-delimited prefix.
  for (size_t end = input.size(); end > 0; --end) {
    if (end < input.size() &&
        std::isspace(static_cast<unsigned char>(input[end]))) {
      auto prefixResult = formatter->parse(input.substr(0, end));
      if (!prefixResult.hasError()) {
        return prefixResult;
      }
    }
  }

  // 3. Calendar leniency: extract raw fields and apply carry rules.
  LenientDate date;
  if (extractFields(*formatter, input, date)) {
    const int64_t seconds = lenientEpochSeconds(date);
    return DateTimeResult{Timestamp(seconds, 0), nullptr};
  }

  // Nothing worked — return the original error.
  return result;
}

// Flink fails the job on an invalid SimpleDateFormat pattern
// (IllegalArgumentException from the formatter constructor).
inline std::shared_ptr<DateTimeFormatter> getJodaFormatterOrThrow(
    const std::string_view format) {
  if (format.empty()) {
    return emptyJodaFormatter();
  }
  auto formatter = buildJodaDateTimeFormatter(format);
  if (formatter.hasError()) {
    VELOX_USER_FAIL(
        "Invalid datetime format pattern '{}': {}",
        format,
        formatter.error().message());
  }
  return formatter.value();
}

// Whether the pattern parses an explicit year field.
inline bool hasYearSpecifier(const DateTimeFormatter& formatter) {
  for (const auto& token : formatter.tokens()) {
    if (token.type != DateTimeToken::Type::kPattern) {
      continue;
    }
    switch (token.pattern.specifier) {
      case DateTimeFormatSpecifier::YEAR:
      case DateTimeFormatSpecifier::YEAR_OF_ERA:
      case DateTimeFormatSpecifier::CENTURY_OF_ERA:
      case DateTimeFormatSpecifier::WEEK_YEAR:
        return true;
      default:
        break;
    }
  }
  return false;
}

// Whether the pattern parses month/day fields of the MM/dd family — the
// tokens that make velox's Joda emulation default a missing year to 2000
// instead of 1970 (DateTimeFormatter.cpp "fallsback to 2000").
inline bool hasMonthOrDaySpecifier(const DateTimeFormatter& formatter) {
  for (const auto& token : formatter.tokens()) {
    if (token.type != DateTimeToken::Type::kPattern) {
      continue;
    }
    switch (token.pattern.specifier) {
      case DateTimeFormatSpecifier::MONTH_OF_YEAR:
      case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT:
      case DateTimeFormatSpecifier::DAY_OF_MONTH:
        return true;
      default:
        break;
    }
  }
  return false;
}

// Flink/Java defaults a missing year to 1970 while velox's Joda emulation
// uses 2000 once month or day fields are present.
inline bool defaultsMissingYearTo2000(const DateTimeFormatter& formatter) {
  return !hasYearSpecifier(formatter) && hasMonthOrDaySpecifier(formatter);
}

// Replaces the wall-clock year with 1970 (the Java default for missing
// years). Feb 29 rolls to Mar 1 through the epoch recomposition, matching
// Java's lenient calendar.
inline void applyEpochDefaultYear(Timestamp& timestamp) {
  std::tm dateTime{};
  if (Timestamp::epochToCalendarUtc(timestamp.getSeconds(), dateTime)) {
    dateTime.tm_year = 70;
    timestamp = Timestamp(
        Timestamp::calendarUtcToEpoch(dateTime), timestamp.getNanos());
  }
}

} // namespace detail

// FROM_UNIXTIME(unixtime): render epoch seconds with the default pattern
// 'yyyy-MM-dd HH:mm:ss' in the session timezone.
template <typename T>
struct FromUnixtimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/) {
    sessionTimeZone_ = detail::getSessionTimeZone(config);
    formatter_ = detail::getJodaFormatterOrThrow(kDefaultFormat_);
    maxResultSize_ = formatter_->maxResultSize(sessionTimeZone_);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& second) {
    formatSeconds(result, second);
    return true;
  }

 protected:
  void formatSeconds(out_type<Varchar>& result, const int64_t second) {
    const Timestamp timestamp{second, 0};
    result.reserve(maxResultSize_);
    const int32_t resultSize = formatter_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data(), true);
    result.resize(resultSize);
  }

  constexpr static std::string_view kDefaultFormat_{"yyyy-MM-dd HH:mm:ss"};
  const tz::TimeZone* sessionTimeZone_{nullptr};
  std::shared_ptr<DateTimeFormatter> formatter_;
  uint32_t maxResultSize_{0};
};

// FROM_UNIXTIME(unixtime, format)
template <typename T>
struct FromUnixtimeWithFormatFunction : public FromUnixtimeFunction<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/,
      const arg_type<Varchar>* format) {
    this->sessionTimeZone_ = detail::getSessionTimeZone(config);
    if (format != nullptr) {
      this->formatter_ = detail::tryGetJodaFormatter(std::string_view(*format));
      isConstantFormat_ = true;
      if (this->formatter_ != nullptr) {
        this->maxResultSize_ =
            this->formatter_->maxResultSize(this->sessionTimeZone_);
      }
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& second,
      const arg_type<Varchar>& format) {
    if (!isConstantFormat_) {
      this->formatter_ =
          detail::getJodaFormatterOrThrow(std::string_view(format));
      this->maxResultSize_ =
          this->formatter_->maxResultSize(this->sessionTimeZone_);
    } else if (this->formatter_ == nullptr) {
      VELOX_USER_FAIL(
          "Invalid datetime format pattern: '{}'", std::string_view(format));
    }
    this->formatSeconds(result, second);
    return true;
  }

 private:
  bool isConstantFormat_{false};
};

// UNIX_TIMESTAMP(): current epoch seconds. Non-deterministic so velox keeps
// evaluating per row like Flink (deterministic no-arg calls are constant
// folded once per query).
template <typename T>
struct UnixTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void call(int64_t& result) {
    result = Timestamp::now().getSeconds();
  }
};

// UNIX_TIMESTAMP(s): parse 'yyyy-MM-dd HH:mm:ss' in the session timezone.
// Flink returns Long.MIN_VALUE (not NULL) when parsing fails.
template <typename T>
struct UnixTimestampParseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/) {
    setFormatter(detail::getJodaFormatterOrThrow(kDefaultFormat_));
    setTimezone(config);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input) {
    parseToSeconds(result, std::string_view(input));
    return true;
  }

 protected:
  void setFormatter(std::shared_ptr<DateTimeFormatter> formatter) {
    formatter_ = std::move(formatter);
    needsEpochDefaultYear_ = detail::defaultsMissingYearTo2000(*formatter_);
  }
  void setTimezone(const core::QueryConfig& config) {
    const auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTimeZone_ = tz::locateZone(sessionTzName);
    }
  }

  const tz::TimeZone* getTimeZone(const DateTimeResult& dateTime) const {
    // If timezone was not parsed, fallback to the session timezone.
    return dateTime.timezone ? dateTime.timezone : sessionTimeZone_;
  }
  // Flink truncates total milliseconds toward zero (ts / 1000 in Java);
  // getSeconds() alone would diverge for negative sub-second timestamps.
  void parseToSeconds(int64_t& result, const std::string_view input) {
    auto dateTimeResult = detail::parseLenient(formatter_, input);
    if (dateTimeResult.hasError()) {
      result = std::numeric_limits<int64_t>::min();
      return;
    }
    auto timestamp = dateTimeResult.value().timestamp;
    if (needsEpochDefaultYear_) {
      detail::applyEpochDefaultYear(timestamp);
    }
    const auto* tz = getTimeZone(dateTimeResult.value());
    if (tz != nullptr) {
      try {
        timestamp.toGMT(*tz);
      } catch (const VeloxUserError&) {
        // Flink lenient: a DST gap local time (e.g. 2005-03-27 02:00:00
        // in MET where 02:00 does not exist) is interpreted with the
        // pre-transition offset — the same instant as the first valid
        // time after the gap (Java SimpleDateFormat behavior).
        detail::resolveDstGap(timestamp, *tz);
      }
    }
    const int64_t millis = timestamp.getSeconds() * 1000 +
        static_cast<int64_t>(timestamp.getNanos()) / 1000000;
    result = millis / 1000;
  }

  constexpr static std::string_view kDefaultFormat_{"yyyy-MM-dd HH:mm:ss"};
  std::shared_ptr<DateTimeFormatter> formatter_;
  bool needsEpochDefaultYear_{false};
  const tz::TimeZone* sessionTimeZone_{tz::locateZone(0)};
};

// UNIX_TIMESTAMP(s, format)
template <typename T>
struct UnixTimestampParseWithFormatFunction
    : public UnixTimestampParseFunction<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    this->setTimezone(config);
    if (format != nullptr) {
      auto formatter = detail::tryGetJodaFormatter(std::string_view(*format));
      if (formatter != nullptr) {
        this->setFormatter(std::move(formatter));
      }
      isConstantFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstantFormat_) {
      this->setFormatter(
          detail::getJodaFormatterOrThrow(std::string_view(format)));
    } else if (this->formatter_ == nullptr) {
      VELOX_USER_FAIL(
          "Invalid datetime format pattern: '{}'", std::string_view(format));
    }
    this->parseToSeconds(result, std::string_view(input));
    return true;
  }

 private:
  bool isConstantFormat_{false};
};

} // namespace facebook::velox::functions::flinksql
