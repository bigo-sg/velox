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
#include "velox/functions/flinksql/tests/FlinkFunctionBaseTest.h"

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::functions::flinksql::test {

namespace {
const std::string kIso{"yyyy-MM-dd"};
}

// Expected values are pinned from Flink's actual behavior where the two
// engines agree, and from this engine's semantics where they differ.
class ToDateTest : public FlinkFunctionBaseTest {
 protected:
  std::optional<int32_t> toDateOnce(const std::optional<std::string>& s) {
    return evaluateOnce<int32_t>("to_date(c0)", s);
  }

  // Non-constant format: the per-row build path.
  std::optional<int32_t> toDateOnce(
      const std::optional<std::string>& s,
      const std::optional<std::string>& f) {
    return evaluateOnce<int32_t>("to_date(c0, c1)", s, f);
  }

  // Constant format: the compile-once path.
  std::optional<int32_t> toDateConstFormatOnce(
      const std::optional<std::string>& s,
      const std::optional<std::string>& f) {
    auto formatFlat = makeNullableFlatVector<std::string>({f});
    auto data = makeRowVector(std::vector<VectorPtr>{
        makeNullableFlatVector<std::string>({s}),
        BaseVector::wrapInConstant(1, 0, formatFlat)});
    return evaluateOnce<int32_t>("to_date(c0, c1)", data);
  }
};

TEST_F(ToDateTest, OneArgBasics) {
  EXPECT_EQ(20707, toDateOnce(std::string("2026-09-11")));
  EXPECT_EQ(20697, toDateOnce(std::string("2026-9-1")));
  EXPECT_EQ(20707, toDateOnce(std::string("2026-09-11 10:00:00")));
  EXPECT_EQ(20707, toDateOnce(std::string("2026-09-11 10:00:00.123")));
  EXPECT_EQ(20707, toDateOnce(std::string(" 2026-09-11 ")));
  EXPECT_EQ(20707, toDateOnce(std::string("2026-09-11 ")));
  EXPECT_EQ(-719528, toDateOnce(std::string("0")));
  EXPECT_EQ(-719528, toDateOnce(std::string("0000-01-01")));
  EXPECT_EQ(2932532, toDateOnce(std::string("9999")));
  EXPECT_EQ(2932896, toDateOnce(std::string("9999-12-31")));
  EXPECT_EQ(19782, toDateOnce(std::string("2024-2-29")));
  EXPECT_EQ(11016, toDateOnce(std::string("2000-2-29")));
  EXPECT_EQ(-171664, toDateOnce(std::string("1500-01-01")));
}

TEST_F(ToDateTest, OneArgInvalidToNull) {
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2023-2-29")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("1900-2-29")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-2-30")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-04-31")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-13-01")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-00-10")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-09-00")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-09-32")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("10000")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2147483647-01-01")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("abc")));
  // Trailing garbage on the day is rejected (the 2-arg engine tolerates it).
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-09-11xyz")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026--09-11")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-09-")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("-2026-05-01")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026- 9- 1")));
}

TEST_F(ToDateTest, OneArgYearOutOfRangeFails) {
  VELOX_ASSERT_THROW(toDateOnce(std::string("9999999999")), "out of int range");
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("99999999999-01-01")), "out of int range");
}

TEST_F(ToDateTest, NullPropagation) {
  EXPECT_EQ(std::nullopt, toDateOnce(std::nullopt));
  EXPECT_EQ(std::nullopt, toDateOnce(std::nullopt, std::string(kIso)));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("2026-09-11"), std::nullopt));
}

TEST_F(ToDateTest, TwoArgBasics) {
  EXPECT_EQ(20707, toDateOnce(std::string("2026-09-11"), std::string(kIso)));
  EXPECT_EQ(20697, toDateOnce(std::string("2026-9-1"), std::string(kIso)));
  EXPECT_EQ(
      20707, toDateOnce(std::string("2026/09/11"), std::string("yyyy/MM/dd")));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-09-11 23:59:59.999"), std::string(kIso)));
  EXPECT_EQ(20707, toDateOnce(std::string("2026-09-11xyz"), std::string(kIso)));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-09-11 "), std::string("yyyy-MM-dd ")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-09-11"), std::string("yyyy-MM-dd ")));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026T09-11"), std::string("yyyy'T'MM-dd")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026t09-11"), std::string("yyyy'T'MM-dd")));
}

TEST_F(ToDateTest, TwoArgOutOfRangeToNull) {
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026-13-01"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026-02-30"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026-00-00"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026-13-32"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026-02-29"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2024-02-30"), std::string(kIso)));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("25"), std::string("HH")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("24"), std::string("HH")));
  // Pure-time parse lands on the epoch (1970-01-01).
  EXPECT_EQ(0, toDateOnce(std::string("23"), std::string("HH")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("90"), std::string("mm")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("61"), std::string("ss")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(
          std::string("2026-09-11 24:00:00"),
          std::string("yyyy-MM-dd HH:mm:ss")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(
          std::string("2026-09-11 23:60:00"),
          std::string("yyyy-MM-dd HH:mm:ss")));
}

TEST_F(ToDateTest, TwoArgParseFailureToNull) {
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("xyz2026-09-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026/09/11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("invalid value"), std::string(kIso)));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string(""), std::string(kIso)));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("   "), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2,026-09-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("20.26-09-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-XX-11"), std::string("yyyy-MMM-dd")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-Sept-11"), std::string("yyyy-MMM-dd")));
  // Non-ASCII digits are not recognized (only '0'..'9').
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("２０２６-09-11"), std::string(kIso)));
}

TEST_F(ToDateTest, TwoArgHistoricalDates) {
  auto d = [&](const std::string& s) {
    return toDateOnce(s, std::string(kIso));
  };
  // Proleptic Gregorian; Flink's JDK engine differs across the 1582 cutover.
  EXPECT_EQ(-141438, d("1582-10-04"));
  EXPECT_EQ(-141437, d("1582-10-05"));
  EXPECT_EQ(-141428, d("1582-10-14"));
  EXPECT_EQ(-141427, d("1582-10-15"));
  EXPECT_EQ(-141714, d("1582-01-01"));
  EXPECT_EQ(-141349, d("1583-01-01"));
  EXPECT_EQ(std::nullopt, d("1582-09-60"));
  EXPECT_EQ(std::nullopt, d("1582-10-45"));
  EXPECT_EQ(std::nullopt, d("1581-13-01"));
  EXPECT_EQ(std::nullopt, d("1582-13-01"));
  EXPECT_EQ(std::nullopt, d("1582-02-30"));
  EXPECT_EQ(std::nullopt, d("1582-00-01"));
  EXPECT_EQ(std::nullopt, d("1583-00-01"));
  EXPECT_EQ(-141380, d("1582-12-01"));
  EXPECT_EQ(-135081, d("1600-02-29"));
  EXPECT_EQ(std::nullopt, d("1700-02-29"));
  EXPECT_EQ(std::nullopt, d("1500-02-29"));
  EXPECT_EQ(std::nullopt, d("1600-01-60"));
}

TEST_F(ToDateTest, TwoArgDayOfYearAndWeekdayFields) {
  EXPECT_EQ(20707, toDateOnce(std::string("2026-254"), std::string("yyyy-D")));
  EXPECT_EQ(
      20544, toDateOnce(std::string("2026-091"), std::string("yyyy-DDD")));
  EXPECT_EQ(
      -141415, toDateOnce(std::string("1582-300"), std::string("yyyy-D")));
  EXPECT_EQ(
      -141050, toDateOnce(std::string("1583-300"), std::string("yyyy-D")));
  EXPECT_EQ(
      -141780, toDateOnce(std::string("1581-300"), std::string("yyyy-D")));
  EXPECT_EQ(
      -141435, toDateOnce(std::string("1582-280"), std::string("yyyy-D")));
  // E-only: no dow anchoring, year defaults to 2000 — all names agree.
  EXPECT_EQ(10957, toDateOnce(std::string("Fri"), std::string("E")));
  EXPECT_EQ(10957, toDateOnce(std::string("Sat"), std::string("E")));
  EXPECT_EQ(10957, toDateOnce(std::string("Sun"), std::string("E")));
  EXPECT_EQ(10957, toDateOnce(std::string("Mon"), std::string("E")));
  EXPECT_EQ(10957, toDateOnce(std::string("Thursday"), std::string("EEEE")));
  EXPECT_EQ(20454, toDateOnce(std::string("2026 Fri"), std::string("yyyy E")));
  EXPECT_EQ(
      20454, toDateOnce(std::string("2026 Thursday"), std::string("yyyy E")));
  EXPECT_EQ(
      20697, toDateOnce(std::string("2026-09 Fri"), std::string("yyyy-MM E")));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-09-11 Mon"), std::string("yyyy-MM-dd E")));
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("5"), std::string("u")), "not supported");
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("7"), std::string("u")), "not supported");
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("2026 7"), std::string("yyyy u")),
      "not supported");
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("2026-09-11 5"), std::string("yyyy-MM-dd u")),
      "not supported");
}

TEST_F(ToDateTest, TwoArgWhitespaceAndDigits) {
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string(" 2026-09-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("  2026-09-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("\t2026-09-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026- 9-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026-09- 11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026 -09-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("2026-09 -11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026- Sep-11"), std::string("yyyy-MMM-dd")));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("\n2026-09-11"), std::string(kIso)));
  EXPECT_EQ(
      20707, toDateOnce(std::string("20260911"), std::string("yyyyMMdd")));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("202691"), std::string("yyyyMMdd")));
  EXPECT_EQ(20697, toDateOnce(std::string("2026091"), std::string("yyyyMMdd")));
  // 'yyyy' reads a 2-digit year literally: year 26, not 2026.
  EXPECT_EQ(-709778, toDateOnce(std::string("26-09-11"), std::string(kIso)));
  EXPECT_EQ(-709778, toDateOnce(std::string("026-09-11"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("09-123-2026"), std::string("MM-dd-yyyy")));
  EXPECT_EQ(3789391, toDateOnce(std::string("12345-01-01"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-09-11"), std::string("dd-MM-yyyy")));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-09-1123"), std::string("yyyy-MM-ddHH")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-09-11"), std::string("yyyy-MM-ddHH")));
  // A parsed time field lazily defaults the year to 2000.
  EXPECT_EQ(10967, toDateOnce(std::string("11 05"), std::string("dd HH")));
  EXPECT_EQ(
      std::nullopt, toDateOnce(std::string("11  05"), std::string("dd HH")));
}

TEST_F(ToDateTest, TwoArgDefaultedFields) {
  EXPECT_EQ(20454, toDateOnce(std::string("2026"), std::string("yyyy")));
  EXPECT_EQ(0, toDateOnce(std::string("12:30:00"), std::string("HH:mm:ss")));
  // A parsed date field lazily defaults the year to 2000.
  EXPECT_EQ(11211, toDateOnce(std::string("09-11"), std::string("MM-dd")));
  EXPECT_EQ(10981, toDateOnce(std::string("25"), std::string("dd")));
  EXPECT_EQ(11201, toDateOnce(std::string("09"), std::string("MM")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("0"), std::string("dd")));
}

TEST_F(ToDateTest, TwoArgSignsAndYearDomain) {
  auto d = [&](const std::string& s) {
    return toDateOnce(s, std::string(kIso));
  };
  // Proleptic: -2026-09-11 = -1459256 (Flink's Julian engine differs).
  EXPECT_EQ(-1459256, d("-2026-09-11"));
  EXPECT_EQ(35804357, d("99999-01-01"));
  EXPECT_EQ(364522607, d("999999-01-01"));
  EXPECT_EQ(std::nullopt, d("1234567890-01-01"));
  EXPECT_EQ(std::nullopt, d("99999999999-01-01"));
  EXPECT_EQ(std::nullopt, d("2147483647-01-01"));
  EXPECT_EQ(std::nullopt, d("2147483648-01-01"));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("-5"), std::string("dd")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("-12"), std::string("dd")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("-25"), std::string("HH")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("-90"), std::string("mm")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("-0"), std::string("dd")));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("-9"), std::string("MM")));
  EXPECT_EQ(std::nullopt, d("2026--5-11"));
  EXPECT_EQ(std::nullopt, d("2026--9-11"));
  EXPECT_EQ(std::nullopt, d("2026-09--11"));
  EXPECT_EQ(std::nullopt, d("2026-9--1"));
  EXPECT_EQ(std::nullopt, d("2026- -9-11"));
  EXPECT_EQ(std::nullopt, toDateOnce(std::string("- 5"), std::string("dd")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-911"), std::string("yyyyMMdd")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026--911"), std::string("yyyyMMdd")));
  // The sign does not count against yyyy's digit cap: -2026-09-11 again.
  EXPECT_EQ(
      -1459256, toDateOnce(std::string("-20260911"), std::string("yyyyMMdd")));
  EXPECT_EQ(std::nullopt, d("99999999999999999999-01-01"));
  EXPECT_EQ(std::nullopt, d("9223372036854775808-01-01"));
  EXPECT_EQ(std::nullopt, d("9999999999999999999-01-01"));
  EXPECT_EQ(std::nullopt, d("-99999999999999999999-01-01"));
  EXPECT_EQ(20707, d("+2026-09-11"));
  EXPECT_EQ(2147483455, d("5881580-01-01"));
  EXPECT_EQ(std::nullopt, d("5881581-01-01"));
}

TEST_F(ToDateTest, TwoArgTextEraTimezone) {
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-Sep-11"), std::string("yyyy-MMM-dd")));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-SEP-11"), std::string("yyyy-MMM-dd")));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-sep-11"), std::string("yyyy-MMM-dd")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-jAn-11"), std::string("yyyy-MMM-dd")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-JaNuArY-11"), std::string("yyyy-MMMM-dd")));
  EXPECT_EQ(
      20707,
      toDateOnce(
          std::string("2026-September-11"), std::string("yyyy-MMMM-dd")));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-September-11"), std::string("yyyy-MMM-dd")));
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-Sep-11"), std::string("yyyy-MMMM-dd")));
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("2026-Sep-11"), std::string("yyyy-LLL-dd")),
      "not supported");
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("AD 2026-09-11"), std::string("G yyyy-MM-dd")));
  // A trailing era is decorative for 'y': the year is not negated.
  EXPECT_EQ(
      20707,
      toDateOnce(std::string("2026-09-11 BC"), std::string("yyyy-MM-dd G")));
  EXPECT_EQ(
      std::nullopt,
      toDateOnce(std::string("2026-09-11"), std::string("yyyy-MM-dd G")));
  EXPECT_EQ(
      0, toDateOnce(std::string("05:30:00 PM"), std::string("hh:mm:ss a")));
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("2026-09-11Z"), std::string("yyyy-MM-ddX")),
      "not supported");
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("2026-09-11+08:00"), std::string("yyyy-MM-ddX")),
      "not supported");
  VELOX_ASSERT_THROW(
      toDateOnce(std::string("2026-09-11+0800"), std::string("yyyy-MM-ddX")),
      "not supported");
  // A parsed timezone is honored: +0800 shifts the GMT day back.
  EXPECT_EQ(
      20706,
      toDateOnce(std::string("2026-09-11+0800"), std::string("yyyy-MM-ddZ")));
  EXPECT_EQ(
      20706,
      toDateOnce(std::string("2026-09-11+08"), std::string("yyyy-MM-ddZ")));
}

TEST_F(ToDateTest, TwoArgTwoDigitYears) {
  auto d = [&](const std::string& s) {
    return toDateOnce(s, std::string("yy-MM-dd"));
  };
  // Fixed pivot: [00,69] -> 20xx, [70,99] -> 19xx (Flink's is clock-based).
  EXPECT_EQ(27394, d("45-01-01"));
  EXPECT_EQ(27759, d("46-01-01"));
  EXPECT_EQ(28123, d("46-12-31"));
  EXPECT_EQ(10956, d("99-12-31"));
  EXPECT_EQ(10957, d("00-01-01"));
  EXPECT_EQ(20707, d("26-09-11"));
  EXPECT_EQ(20464, toDateOnce(std::string("11/26"), std::string("dd/yy")));
  EXPECT_EQ(20464, toDateOnce(std::string("11/2026"), std::string("dd/yy")));
  EXPECT_EQ(36160, d("69-01-01"));
  EXPECT_EQ(0, d("70-01-01"));
}

TEST_F(ToDateTest, TwoArgPatternLegality) {
  VELOX_ASSERT_THROW(
      toDateConstFormatOnce(std::string("2026-09-11"), std::string("QQ")),
      "not supported");
  VELOX_ASSERT_THROW(
      toDateConstFormatOnce(std::string("2026-09-11"), std::string("abc")),
      "not supported");
  VELOX_ASSERT_THROW(
      toDateConstFormatOnce(std::string("2026-09-11"), std::string("'abc")),
      "closing single quote");
  VELOX_ASSERT_THROW(
      toDateConstFormatOnce(
          std::string("2026-09-11"), std::string("yyyy-MM-dd u")),
      "not supported");
  VELOX_ASSERT_THROW(
      toDateConstFormatOnce(
          std::string("2026-09-11"), std::string("yyyy-LLL-dd")),
      "not supported");
  VELOX_ASSERT_THROW(
      toDateConstFormatOnce(
          std::string("2026-09-11"), std::string("yyyy-MM-dd XXXX")),
      "not supported");
  VELOX_ASSERT_THROW(
      toDateConstFormatOnce(
          std::string("2026-09-11"), std::string("yyyy-F-dd")),
      "not supported");
  // 'C' = century-of-era overwrites the year: 20 -> 2000.
  EXPECT_EQ(
      11211,
      toDateConstFormatOnce(
          std::string("2026 20 09 11"), std::string("yyyy CC MM dd")));
  EXPECT_EQ(
      20707,
      toDateConstFormatOnce(
          std::string("2026-09-11 3"), std::string("yyyy-MM-dd e")));
  // 'x' = week-year: 2026-W03-1 = 2026-01-12.
  EXPECT_EQ(
      20465,
      toDateConstFormatOnce(std::string("2026 3 1"), std::string("xxxx w e")));
  VELOX_ASSERT_THROW(
      toDateConstFormatOnce(std::string("2026-09-11"), std::string("")),
      "should not be empty");
}

TEST_F(ToDateTest, TwoArgColumnFormatMultiRow) {
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<std::string>(
          {"2026-09-11", "20260911", "2026-254"}),
      makeNullableFlatVector<std::string>(
          {"yyyy-MM-dd", "yyyyMMdd", "yyyy-D"})});
  auto result = evaluate("to_date(c0, c1)", data);
  ASSERT_EQ(3, result->size());
  auto flat = result->asFlatVector<int32_t>();
  EXPECT_EQ(20707, flat->valueAt(0));
  EXPECT_EQ(20707, flat->valueAt(1));
  EXPECT_EQ(20707, flat->valueAt(2));
}

TEST_F(ToDateTest, TwoArgColumnFormatBadRowFails) {
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<std::string>({"2026-09-11", "2026-09-11"}),
      makeNullableFlatVector<std::string>({"yyyy-MM-dd", "QQ"})});
  VELOX_ASSERT_THROW(evaluate("to_date(c0, c1)", data), "not supported");
}

TEST_F(ToDateTest, TwoArgAllNullInputWithBadConstantFormat) {
  // All-NULL input: Flink's null propagation skips the method call, so the
  // constant-format error must not surface — the rows come back NULL.
  EXPECT_EQ(
      std::nullopt, toDateConstFormatOnce(std::nullopt, std::string("QQ")));
  EXPECT_EQ(
      std::nullopt,
      toDateConstFormatOnce(std::nullopt, std::string("yyyy-MM-dd")));

  // A non-NULL row alongside NULLs still fails the job, like Flink.
  auto formatFlat = makeNullableFlatVector<std::string>({std::string("QQ")});
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<std::string>(
          {std::nullopt, std::string("2026-09-11")}),
      BaseVector::wrapInConstant(2, 0, formatFlat)});
  VELOX_ASSERT_THROW(evaluate("to_date(c0, c1)", data), "not supported");
}

} // namespace facebook::velox::functions::flinksql::test
