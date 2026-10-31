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
#include "velox/functions/flinksql/DateTimeFunctions.h"

#include <cstdint>
#include <utility>

namespace facebook::velox::functions::flinksql {

namespace {

//===---------------------------------------------------------------------===//
// to_date helpers
//===---------------------------------------------------------------------===//

// Flink String.trim(): strips code points <= U+0020 (ASCII bytes here).
std::string_view trimFlink(std::string_view s) {
  size_t b = 0;
  size_t e = s.size();
  auto isSpace = [](unsigned char c) { return c <= 0x20; };
  while (b < e && isSpace(s[b])) {
    ++b;
  }
  while (e > b && isSpace(s[e - 1])) {
    --e;
  }
  return s.substr(b, e - b);
}

// Flink isInteger: non-empty, all ASCII digits, no sign.
bool isIntegerFlink(std::string_view s) {
  if (s.empty()) {
    return false;
  }
  for (char c : s) {
    if (c < '0' || c > '9') {
      return false;
    }
  }
  return true;
}

// Flink isIllegalDate: true when the date is legal (the name is inverted);
// proleptic Gregorian 400/100 leap rules.
bool isIllegalDateFlink(int32_t y, int32_t m, int32_t d) {
  if (y < 0 || y > 9999 || m < 1 || m > 12 || d < 1 || d > 31) {
    return false;
  }
  if (m == 2 && d > 28) {
    if (!(y % 400 == 0 || (y % 4 == 0 && y % 100 != 0)) || d > 29) {
      return false;
    }
  }
  if (d == 31) {
    switch (m) {
      case 1:
      case 3:
      case 5:
      case 7:
      case 8:
      case 10:
      case 12:
        return true;
      default:
        return false;
    }
  }
  return true;
}

// parseInt: an int32 overflow fails the job (Flink: NumberFormatException).
int32_t parseInt32OrFail(std::string_view s) {
  constexpr int64_t kLimit = 2147483647;
  int64_t v = 0;
  for (char c : s) {
    v = v * 10 + (c - '0');
    if (v > kLimit) {
      VELOX_USER_FAIL("Invalid date string '{}': number out of int range", s);
    }
  }
  return static_cast<int32_t>(v);
}

// ymd -> epoch days (proleptic Gregorian), computed in int64: exact for
// the one-argument domain y in [0, 9999].
int32_t ymdToUnixDate(int32_t y, int32_t m, int32_t d) {
  constexpr int64_t kEpochJulian = 2440588;
  const int64_t a = (14 - m) / 12;
  const int64_t y2 = y + 4800 - a;
  const int64_t m2 = m + 12 * a - 3;
  const int64_t julian =
      d + (153 * m2 + 2) / 5 + 365 * y2 + y2 / 4 - y2 / 100 + y2 / 400 - 32045;
  return static_cast<int32_t>(julian - kEpochJulian);
}

//===---------------------------------------------------------------------===//
// to_timestamp helpers
//===---------------------------------------------------------------------===//

inline bool isDigit(char c) {
  return c >= '0' && c <= '9';
}

inline int32_t digitVal(char c) {
  return c - '0';
}

// y/m/d -> epoch days (proleptic Gregorian, Hinnant's closed form) in int64;
// exact for the whole supported year domain [1, 999999999].
int64_t daysFromCivil(int64_t y, int32_t m, int64_t d) {
  y -= m <= 2;
  const int64_t era = (y >= 0 ? y : y - 399) / 400;
  const int64_t yoe = y - era * 400; // [0, 399]
  const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

bool isLeapYear(int64_t y) {
  return y % 400 == 0 || (y % 4 == 0 && y % 100 != 0);
}

int64_t lastDayOfMonth(int64_t y, int32_t m) {
  static const int64_t kDays[12] = {
      31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (m == 2 && isLeapYear(y)) {
    return 29;
  }
  return kDays[m - 1];
}

// The [XX][X] optional-segment ladder shared by month/day/hour/minute/second
// (java.time optional sections stacked: fixed-width-2 first, then variable
// 1-2). A same-value second assignment consumes three digits; a conflicting
// one fails the variable segment, leaving the position after the fixed-width
// segment (the leftover digit then breaks the next literal -> NULL).
// An absent segment (no digits) leaves the position untouched.
struct LadderResult {
  bool seen{false};
  int32_t value{0};
  size_t pos{0};
};

LadderResult parseLadder(std::string_view s, size_t pos) {
  LadderResult r;
  r.pos = pos;
  if (pos + 1 < s.size() && isDigit(s[pos]) && isDigit(s[pos + 1])) {
    // Fixed-width segment [XX]: exactly two digits.
    r.value = digitVal(s[pos]) * 10 + digitVal(s[pos + 1]);
    r.pos = pos + 2;
    r.seen = true;
    // Variable-width segment [X]: 1-2 digits (greedy), from the new position.
    if (r.pos < s.size() && isDigit(s[r.pos])) {
      int32_t v2 = digitVal(s[r.pos]);
      size_t p2 = r.pos + 1;
      if (p2 < s.size() && isDigit(s[p2])) {
        v2 = v2 * 10 + digitVal(s[p2]);
        ++p2;
      }
      if (v2 == r.value) {
        r.pos = p2; // same value: no field conflict, consume
      }
      // different value: the variable segment fails, position stays put.
    }
    return r;
  }
  // Fixed-width segment absent (fewer than two digits): the variable segment
  // runs alone.
  if (pos < s.size() && isDigit(s[pos])) {
    int32_t v = digitVal(s[pos]);
    size_t p = pos + 1;
    if (p < s.size() && isDigit(s[p])) {
      v = v * 10 + digitVal(s[p]);
      ++p;
    }
    r.value = v;
    r.pos = p;
    r.seen = true;
  }
  return r;
}

} // namespace

//===---------------------------------------------------------------------===//
// to_date
//===---------------------------------------------------------------------===//

std::optional<int32_t> flinkParseDate(std::string_view s) {
  // Truncate at the first space when past position 0: timestamp strings
  // parse as dates.
  const size_t ws1 = s.find(' ');
  if (ws1 != std::string_view::npos && ws1 > 0) {
    s = s.substr(0, ws1);
  }

  int32_t y, m, d;

  const size_t hyphen1 = s.find('-');
  if (hyphen1 == std::string_view::npos) {
    const std::string_view ys = trimFlink(s);
    if (!isIntegerFlink(ys)) {
      return std::nullopt;
    }
    y = parseInt32OrFail(ys);
    m = 1;
    d = 1;
  } else {
    const std::string_view ys = trimFlink(s.substr(0, hyphen1));
    if (!isIntegerFlink(ys)) {
      return std::nullopt;
    }
    y = parseInt32OrFail(ys);
    const size_t hyphen2 = s.find('-', hyphen1 + 1);
    if (hyphen2 == std::string_view::npos) {
      const std::string_view ms = trimFlink(s.substr(hyphen1 + 1));
      if (!isIntegerFlink(ms)) {
        return std::nullopt;
      }
      m = parseInt32OrFail(ms);
      d = 1;
    } else {
      const std::string_view ms =
          trimFlink(s.substr(hyphen1 + 1, hyphen2 - hyphen1 - 1));
      if (!isIntegerFlink(ms)) {
        return std::nullopt;
      }
      m = parseInt32OrFail(ms);
      const std::string_view ds = trimFlink(s.substr(hyphen2 + 1));
      if (!isIntegerFlink(ds)) {
        return std::nullopt;
      }
      d = parseInt32OrFail(ds);
    }
  }

  if (!isIllegalDateFlink(y, m, d)) {
    return std::nullopt;
  }
  return ymdToUnixDate(y, m, d);
}

std::optional<int32_t> toDateDays(
    DateTimeResult& dateTimeResult,
    const tz::TimeZone& fallbackTimeZone) {
  // Flink hard-codes UTC: the no-timezone fallback is GMT, never the session
  // zone; a parsed timezone is honored.
  dateTimeResult.timestamp.toGMT(
      dateTimeResult.timezone ? *dateTimeResult.timezone : fallbackTimeZone);
  // floorDiv: truncating division would lose a day for BCE dates with a
  // positive time-of-day part.
  const int64_t seconds = dateTimeResult.timestamp.getSeconds();
  const int64_t days = seconds >= 0
      ? seconds / Timestamp::kSecondsInDay
      : (seconds - (Timestamp::kSecondsInDay - 1)) / Timestamp::kSecondsInDay;
  if (days < INT32_MIN || days > INT32_MAX) {
    return std::nullopt; // out of the int32 DATE range -> NULL
  }
  return static_cast<int32_t>(days);
}

std::optional<int32_t> flinkParseDateWithFormat(
    std::string_view s,
    std::string_view format) {
  // A bad pattern fails the job (Flink: IllegalArgumentException).
  auto built = buildSimpleDateTimeFormatter(format, /*lenient=*/true);
  if (built.hasError()) {
    VELOX_USER_FAIL("{}", built.error().message());
  }
  // Parse failure -> NULL (Flink's JDK engine yields a garbage date).
  auto dateTimeResult = built.value()->parse(s);
  if (dateTimeResult.hasError()) {
    return std::nullopt;
  }
  return toDateDays(*dateTimeResult, *tz::locateZone(0));
}

//===---------------------------------------------------------------------===//
// to_timestamp
//===---------------------------------------------------------------------===//

// One-argument parser: hand-written mirror of Flink's
// DateTimeUtils.parseTimestampData(String) -> DEFAULT_TIMESTAMP_FORMATTER
// (java.time SMART) -> fromTemporalAccessor(acc, 3) ->
// TimestampData.fromLocalDateTime. Rules R0-R6 are pinned by JDK17 probes;
// every failure is a NULL row — the function has no job-failure path.
std::optional<Timestamp> flinkParseTimestamp(std::string_view s) {
  const size_t n = s.size();
  size_t pos = 0;

  // --- R1: year segment (yyyy = YEAR_OF_ERA, EXCEEDS_PAD, min 4 / max 19) ---
  int64_t year = 1970;
  {
    if (pos < n && (s[pos] == '+' || s[pos] == '-')) {
      const bool negative = s[pos] == '-';
      ++pos;
      // Sign present: 5-19 digits required ('+1000-' is rejected).
      const size_t start = pos;
      int64_t v = 0;
      while (pos < n && isDigit(s[pos]) && pos - start < 19) {
        v = v * 10 + digitVal(s[pos]);
        if (v > 999999999) {
          // Year domain exceeded: bail before a 19-digit run can overflow
          // the int64 accumulator (Flink's parse rejects it too -> NULL).
          return std::nullopt;
        }
        ++pos;
      }
      const size_t digits = pos - start;
      if (digits < 5 || digits > 19) {
        return std::nullopt;
      }
      year = negative ? -v : v;
    } else {
      // No sign: exactly 4 digits (a 5th digit present means failure).
      const size_t start = pos;
      int64_t v = 0;
      while (pos < n && isDigit(s[pos]) && pos - start < 4) {
        v = v * 10 + digitVal(s[pos]);
        ++pos;
      }
      const size_t digits = pos - start;
      if (digits != 4) {
        return std::nullopt;
      }
      if (pos < n && isDigit(s[pos])) {
        return std::nullopt; // 5th digit without a sign: EXCEEDS_PAD rejects
      }
      year = v;
    }
  }

  // The '-' literal after the year is mandatory, even when the month is
  // absent ('2026--' is valid; '2026' / '2026-' are not).
  if (pos >= n || s[pos] != '-') {
    return std::nullopt;
  }
  ++pos;

  // --- R2: month ladder [MM][M] ---
  const LadderResult month = parseLadder(s, pos);
  pos = month.pos;

  // The second '-' literal is also mandatory ('2026-09-' consumes it).
  if (pos >= n || s[pos] != '-') {
    return std::nullopt;
  }
  ++pos;

  // --- R2: day ladder [dd][d] ---
  const LadderResult day = parseLadder(s, pos);
  pos = day.pos;

  // --- R3/R4: optional time section " [HH][H]:[mm][m]:[ss][s]" + fraction ---
  // The section as a whole backtracks on failure; the ladders inside are
  // optional (an absent ladder leaves the ':' literals to still match), and
  // the fraction (0-9 digits, right-padded to nanos) is only reachable
  // inside the section.
  int32_t hour = 0, minute = 0, second = 0;
  uint64_t nano = 0;
  {
    const size_t timeStart = pos;
    bool timeOk = false;
    if (pos < n && s[pos] == ' ') {
      const LadderResult h = parseLadder(s, pos + 1);
      if (h.pos < n && s[h.pos] == ':') {
        const LadderResult mi = parseLadder(s, h.pos + 1);
        if (mi.pos < n && s[mi.pos] == ':') {
          const LadderResult sec = parseLadder(s, mi.pos + 1);
          size_t p = sec.pos;
          if (p < n && s[p] == '.') {
            ++p;
            uint64_t v = 0;
            int digits = 0;
            while (p < n && isDigit(s[p]) && digits < 9) {
              v = v * 10 + digitVal(s[p]);
              ++p;
              ++digits;
            }
            for (int i = digits; i < 9; ++i) {
              v *= 10; // right-pad to nanoseconds
            }
            nano = v;
          }
          hour = h.value;
          minute = mi.value;
          second = sec.value;
          pos = p;
          timeOk = true;
        }
      }
    }
    if (!timeOk) {
      pos = timeStart; // whole-section backtrack; time fields default to 0
    }
  }

  // --- R0: full-input consumption; no trim, no trailing tolerance ---
  if (pos != n) {
    return std::nullopt;
  }

  // --- R5: field defaults, ranges, SMART clamping ---
  const int64_t y = year;
  const int32_t mo = month.seen ? month.value : 1;
  int64_t d = day.seen ? day.value : 1;
  // YearOfEra domain; a negative year reaches here only via '-' forms.
  if (y < 1 || y > 999999999) {
    return std::nullopt;
  }
  if (mo < 1 || mo > 12) {
    return std::nullopt;
  }
  // Day present without a month: java.time fails the resolve ('2026--01');
  // time fields do not trigger this rule ('2026-- 10:00:00' is fine).
  if (day.seen && !month.seen) {
    return std::nullopt;
  }
  if (d < 1 || d > 31) {
    return std::nullopt;
  }
  // SMART clamping: day beyond the month's end (but within 31) clamps to it.
  if (d > lastDayOfMonth(y, mo)) {
    d = lastDayOfMonth(y, mo);
  }
  if (hour == 24) {
    // 24:00:00.000000000 rolls to the next day's midnight; 24 with a
    // non-zero minute/second/nano (pre-truncation) is rejected.
    if (minute != 0 || second != 0 || nano != 0) {
      return std::nullopt;
    }
    hour = 0;
    ++d; // may roll over the month/year end via daysFromCivil below
  } else if (hour > 24) {
    return std::nullopt;
  }
  if (minute > 59 || second > 59) {
    return std::nullopt;
  }

  // --- R6: construct seconds/nanos; floor the fraction to milliseconds ---
  const int64_t seconds =
      daysFromCivil(y, mo, d) * 86400 + hour * 3600 + minute * 60 + second;
  const uint64_t nanos = nano / 1'000'000 * 1'000'000;
  if (seconds < Timestamp::kMinSeconds || seconds > Timestamp::kMaxSeconds) {
    // TODO(D1): Flink wraps huge-year millis into garbage; GFV returns NULL.
    return std::nullopt;
  }
  return Timestamp(seconds, nanos);
}

std::optional<Timestamp> flinkParseTimestampWithFormatter(
    const DateTimeFormatter& formatter,
    std::string_view s) {
  std::optional<DateTimeResult> parsed;
  try {
    // Only parse() is guarded: build errors must not be swallowed, and debug
    // builds throw from inside parse() on 'S'-overflow pathologies (illegal
    // Timestamp constructions) — caught here per Flink's
    // try-catch-everything-into-NULL semantics.
    auto res = formatter.parse(s);
    if (res.hasError()) {
      return std::nullopt;
    }
    parsed = std::move(res.value());
  } catch (...) {
    return std::nullopt;
  }
  // Release builds let an illegal (wrapped-negative) nanos through the
  // Timestamp constructor; guard it into a NULL row.
  if (parsed->timestamp.getNanos() > Timestamp::kMaxNanos) {
    return std::nullopt;
  }
  // delta-1: the engine result is already the wall-clock GMT frame — the
  // parsed timezone is ignored (Flink's fromTemporalAccessor reads fields
  // only and never applies OFFSET_SECONDS).
  // delta-3: keep the full Timestamp at millisecond precision (TIMESTAMP(3)).
  // TODO(T1-T13): engine deviations vs Flink SMART+fallback; approved, pinned.
  return parsed->timestamp.toPrecision(TimestampPrecision::kMilliseconds);
}

std::optional<Timestamp> flinkParseTimestampWithFormat(
    std::string_view s,
    std::string_view format) {
  // delta-2: a bad pattern is a NULL row, never a job failure (Flink's
  // ofPattern IAE happens inside the method and is try-caught per row) — the
  // opposite of to_date's disposition.
  auto built = buildSimpleDateTimeFormatter(format, /*lenient=*/true);
  if (built.hasError()) {
    return std::nullopt;
  }
  return flinkParseTimestampWithFormatter(*built.value(), s);
}

} // namespace facebook::velox::functions::flinksql
