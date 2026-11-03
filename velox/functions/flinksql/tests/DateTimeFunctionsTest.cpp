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
#include <cstdint>
#include <limits>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/flinksql/DateTimeFunctions.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::flinksql::test {
namespace {

constexpr int64_t kMinBigint = std::numeric_limits<int64_t>::min();

class DateTimeFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    functions::test::FunctionBaseTest::SetUpTestCase();
    functions::flinksql::registerFunctions("");
  }

  void setSessionTimezone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  std::optional<std::string> fromUnixtime(
      const std::optional<int64_t>& unixtime) {
    return evaluateOnce<std::string>("from_unixtime(c0)", unixtime);
  }

  std::optional<std::string> fromUnixtime(
      const std::optional<int64_t>& unixtime,
      const std::optional<std::string>& format) {
    return evaluateOnce<std::string>("from_unixtime(c0, c1)", unixtime, format);
  }

  std::optional<int64_t> unixTimestamp(
      const std::optional<std::string>& input) {
    return evaluateOnce<int64_t>("unix_timestamp(c0)", input);
  }

  std::optional<int64_t> unixTimestamp(
      const std::optional<std::string>& input,
      const std::optional<std::string>& format) {
    return evaluateOnce<int64_t>("unix_timestamp(c0, c1)", input, format);
  }
};

TEST_F(DateTimeFunctionsTest, fromUnixtimeDefaultFormat) {
  setSessionTimezone("UTC");
  EXPECT_EQ("2025-10-09 08:53:20", fromUnixtime(1760000000));
  EXPECT_EQ("1970-01-01 00:00:00", fromUnixtime(0));
  EXPECT_EQ("1969-12-31 23:59:59", fromUnixtime(-1));
  EXPECT_EQ("9999-12-31 23:59:59", fromUnixtime(253402300799));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeSessionTimezone) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ("2025-10-09 16:53:20", fromUnixtime(1760000000));

  setSessionTimezone("MET");
  EXPECT_EQ("2005-03-27 03:00:00", fromUnixtime(1111885200));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeSessionTzWithoutAdjustFlag) {
  // Flink renders FROM_UNIXTIME in the session timezone regardless of the
  // adjust_timestamp_to_timezone flag (that flag governs TIMESTAMP
  // storage/comparison, not rendering).
  queryCtx_->testingOverrideConfigUnsafe({
      {core::QueryConfig::kSessionTimezone, "Asia/Shanghai"},
      {core::QueryConfig::kAdjustTimestampToTimezone, "false"},
  });
  EXPECT_EQ("1970-01-01 08:00:00", fromUnixtime(0));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeCustomFormat) {
  setSessionTimezone("UTC");
  EXPECT_EQ("2025-10-09", fromUnixtime(1760000000, "yyyy-MM-dd"));
  EXPECT_EQ("2025", fromUnixtime(1760000000, "yyyy"));
  EXPECT_EQ("", fromUnixtime(0, std::string("")));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeNull) {
  setSessionTimezone("UTC");
  EXPECT_EQ(std::nullopt, fromUnixtime(std::nullopt));
  EXPECT_EQ(std::nullopt, fromUnixtime(0, std::nullopt));
  EXPECT_EQ(std::nullopt, fromUnixtime(std::nullopt, "yyyy-MM-dd"));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeInvalidFormat) {
  setSessionTimezone("UTC");
  VELOX_ASSERT_THROW(fromUnixtime(0, "qq"), "Invalid datetime format pattern");
}

TEST_F(DateTimeFunctionsTest, unixTimestampDefaultFormat) {
  setSessionTimezone("UTC");
  EXPECT_EQ(0, unixTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(-1, unixTimestamp("1969-12-31 23:59:59"));
  EXPECT_EQ(1760000000, unixTimestamp("2025-10-09 08:53:20"));

  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(1789092000, unixTimestamp("2026-09-11 10:00:00"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampCustomFormat) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(1789056000, unixTimestamp("2026-09-11", "yyyy-MM-dd"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampDstRepeatedTime) {
  setSessionTimezone("MET");
  EXPECT_EQ(1111885200, unixTimestamp("2005-03-27 03:00:00"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampDstGapLenient) {
  setSessionTimezone("MET");
  // Flink (lenient SimpleDateFormat) maps the gap time to the pre-transition
  // offset: 2005-03-27 02:00:00 MET (nonexistent) -> 01:00 UTC = 1111885200,
  // the same instant as 03:00:00 MET after DST starts.
  EXPECT_EQ(1111885200, unixTimestamp("2005-03-27 02:00:00"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampNull) {
  setSessionTimezone("UTC");
  EXPECT_EQ(std::nullopt, unixTimestamp(std::nullopt));
  EXPECT_EQ(std::nullopt, unixTimestamp(std::nullopt, "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, unixTimestamp("2026-09-11", std::nullopt));
}

TEST_F(DateTimeFunctionsTest, unixTimestampUnparseableReturnsMin) {
  setSessionTimezone("UTC");
  EXPECT_EQ(kMinBigint, unixTimestamp("garbage"));
  EXPECT_EQ(kMinBigint, unixTimestamp(""));
  EXPECT_EQ(kMinBigint, unixTimestamp("00:00:00"));
  // Trailing text: Flink parses the prefix and ignores the rest.
  EXPECT_EQ(1789120800, unixTimestamp("2026-09-11 10:00:00 xyz"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampLenientCalendar) {
  setSessionTimezone("UTC");
  // Flink lenient: 2025-02-30 rolls to 2025-03-02.
  EXPECT_EQ(1740873600, unixTimestamp("2025-02-30", "yyyy-MM-dd"));
  // Flink lenient: month 13 rolls to next year.
  EXPECT_EQ(1767225600, unixTimestamp("2025-13-01", "yyyy-MM-dd"));
  // Flink lenient: day 32 in January rolls to February 1.
  EXPECT_EQ(1738368000, unixTimestamp("2025-01-32", "yyyy-MM-dd"));
  // Flink lenient: Feb 29 in non-leap year rolls to March 1.
  EXPECT_EQ(1740787200, unixTimestamp("2025-02-29", "yyyy-MM-dd"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampMillisecondTruncation) {
  setSessionTimezone("UTC");
  EXPECT_EQ(
      0, unixTimestamp("1970-01-01 00:00:00.900", "yyyy-MM-dd HH:mm:ss.SSS"));
  EXPECT_EQ(
      0, unixTimestamp("1969-12-31 23:59:59.900", "yyyy-MM-dd HH:mm:ss.SSS"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampMissingYearDefaultsTo1970) {
  setSessionTimezone("UTC");
  EXPECT_EQ(31449600, unixTimestamp("12-31", "MM-dd"));
  EXPECT_EQ(6307200, unixTimestamp("03-15", "MM-dd"));
  // Feb 29 rolls to Mar 1 like Java's lenient calendar.
  EXPECT_EQ(5097600, unixTimestamp("02-29", "MM-dd"));
  EXPECT_EQ(10368000, unixTimestamp("05", "MM"));
  // Time-only patterns keep the 1970 default on both sides.
  EXPECT_EQ(3599, unixTimestamp("00:59:59", "HH:mm:ss"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampInvalidFormat) {
  setSessionTimezone("UTC");
  VELOX_ASSERT_THROW(
      unixTimestamp("2026-09-11", "qq"), "Invalid datetime format pattern");
}

TEST_F(DateTimeFunctionsTest, unixTimestampCurrent) {
  auto mockRowVector =
      makeRowVector({BaseVector::createNullConstant(UNKNOWN(), 1, pool())});
  const int64_t before = Timestamp::now().getSeconds();
  const auto result = evaluateOnce<int64_t>("unix_timestamp()", mockRowVector);
  const int64_t after = Timestamp::now().getSeconds();
  ASSERT_TRUE(result.has_value());
  EXPECT_GE(*result, before);
  EXPECT_LE(*result, after);
}

} // namespace
} // namespace facebook::velox::functions::flinksql::test
