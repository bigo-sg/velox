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
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::flinksql::test {

class DateFormatFlinkTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    // FunctionBaseTest registers the prestosql baseline; the flinksql layer
    // overrides it.
    functions::test::FunctionBaseTest::SetUpTestCase();
    flinksql::registerFunctions("");
  }

  void setSessionTimezone(const std::string& tz) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSessionTimezone, tz}});
  }

  // Vector input: a "TIMESTAMP '...'" literal parses via duckdb into a cast
  // call, which is not registered in this test binary.
  std::string evalDateFormat(
      Timestamp ts,
      const std::string& pattern = "yyyy-MM-dd HH:mm:ss") {
    auto input = makeRowVector({makeFlatVector<Timestamp>({ts})});
    auto result = evaluate<SimpleVector<StringView>>(
        fmt::format("date_format(c0, '{}')", pattern), input);
    return result->valueAt(0).str();
  }

  std::string evalDateFormatLtz(
      int64_t utcMillis,
      int16_t zoneId,
      const std::string& pattern = "yyyy-MM-dd HH:mm:ss") {
    // Explicit vector: a single-element braced list resolves to the
    // (size_t, TypePtr) overload and treats the packed value as the length.
    auto input = makeFlatVector<int64_t>(
        std::vector<int64_t>({pack(utcMillis, zoneId)}),
        TIMESTAMP_WITH_TIME_ZONE());
    auto result = evaluate<SimpleVector<StringView>>(
        fmt::format("date_format(c0, '{}')", pattern), makeRowVector({input}));
    return result->valueAt(0).str();
  }

  // 2024-03-15 10:15:30 UTC in epoch millis.
  static constexpr int64_t kUtcMillis = 1710497730000;
};

// Under a non-UTC session timezone the no-timezone TIMESTAMP renders its
// wall clock; the sparksql semantics would shift by +8 (10:15:30 → 18:15:30).
TEST_F(DateFormatFlinkTest, WallClockUnderNonUtcSessionTimezone) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(
      "2024-03-15 10:15:30",
      evalDateFormat(Timestamp{1710497730, 0} /* 2024-03-15 10:15:30 */));
}

TEST_F(DateFormatFlinkTest, WallClockUnderUtcSessionTimezone) {
  setSessionTimezone("UTC");
  EXPECT_EQ(
      "2024-03-15 10:15:30",
      evalDateFormat(Timestamp{1710497730, 0} /* 2024-03-15 10:15:30 */));
}

TEST_F(DateFormatFlinkTest, WallClockDifferentValues) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(
      "2024-01-15 03:04:05",
      evalDateFormat(Timestamp{1705287845, 0} /* 2024-01-15 03:04:05 */));
  EXPECT_EQ(
      "2023-12-31 23:59:59",
      evalDateFormat(Timestamp{1704067199, 0} /* 2023-12-31 23:59:59 */));
  EXPECT_EQ(
      "2024-06-01 00:00:00",
      evalDateFormat(Timestamp{1717200000, 0} /* 2024-06-01 00:00:00 */));
}

// UTC instant 2024-03-15 10:15:30 → Shanghai wall clock 18:15:30.
TEST_F(DateFormatFlinkTest, LtzRendersInSessionTimezone) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(
      "2024-03-15 18:15:30",
      evalDateFormatLtz(kUtcMillis, tz::getTimeZoneID("UTC")));
}

// The session zone wins over the value's embedded zone (Los_Angeles):
// embedded-zone rendering (presto format_datetime semantics) would
// render 02:15:30 instead.
TEST_F(DateFormatFlinkTest, LtzSessionZoneWinsOverEmbeddedZone) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(
      "2024-03-15 18:15:30",
      evalDateFormatLtz(kUtcMillis, tz::getTimeZoneID("America/Los_Angeles")));
}

TEST_F(DateFormatFlinkTest, LtzUnderUtcSessionTimezone) {
  setSessionTimezone("UTC");
  EXPECT_EQ(
      "2024-03-15 10:15:30",
      evalDateFormatLtz(kUtcMillis, tz::getTimeZoneID("America/Los_Angeles")));
}

// The pattern dialect is velox's Joda-style builder (the same dialect as
// FROM_UNIXTIME), not java.time's DateTimeFormatter: 'n' and 'u' are valid
// Flink letters but rejected here at bind time.
TEST_F(DateFormatFlinkTest, UnsupportedPatternLettersError) {
  VELOX_ASSERT_THROW(
      evalDateFormat(Timestamp{1710497730, 0}, "n"), "Invalid date format");
  VELOX_ASSERT_THROW(
      evalDateFormat(Timestamp{1710497730, 0}, "u"), "Invalid date format");
}

// 'z' parses but errors at format time on the no-timezone overload: no zone
// is rendered for the wall-clock type. On the LTZ overload it renders the
// session zone's short name.
TEST_F(DateFormatFlinkTest, PatternLetterZ) {
  VELOX_ASSERT_THROW(
      evalDateFormat(Timestamp{1710497730, 0}, "z"), "cannot be formatted");
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(
      "CST", evalDateFormatLtz(kUtcMillis, tz::getTimeZoneID("UTC"), "z"));
}

// 'e' renders the Joda day-of-week number (Mon=1..Sun=7); 2024-03-15 is a
// Friday. java.time would render a locale-based number instead.
TEST_F(DateFormatFlinkTest, PatternLetterEJodaDayOfWeek) {
  EXPECT_EQ("5", evalDateFormat(Timestamp{1710497730, 0}, "e"));
}

// NULL inputs propagate to a NULL result.
TEST_F(DateFormatFlinkTest, NullTimestampYieldsNull) {
  auto input = makeRowVector({makeNullableFlatVector<Timestamp>(
      {std::optional<Timestamp>(Timestamp{1710497730, 0}),
       std::optional<Timestamp>()})});
  auto result =
      evaluate<SimpleVector<StringView>>("date_format(c0, 'yyyy')", input);
  ASSERT_FALSE(result->isNullAt(0));
  ASSERT_TRUE(result->isNullAt(1));
}

TEST_F(DateFormatFlinkTest, NullFormatYieldsNull) {
  auto input = makeRowVector(
      {makeFlatVector<Timestamp>(
           {Timestamp{1710497730, 0}, Timestamp{1710497730, 0}}),
       makeNullableFlatVector<std::string>(
           {std::optional<std::string>("yyyy"), std::nullopt})});
  auto result =
      evaluate<SimpleVector<StringView>>("date_format(c0, c1)", input);
  ASSERT_FALSE(result->isNullAt(0));
  ASSERT_TRUE(result->isNullAt(1));
}

// A per-row format column exercises the rebuild path in call().
TEST_F(DateFormatFlinkTest, NonConstFormatColumn) {
  auto input = makeRowVector(
      {makeFlatVector<Timestamp>(
           {Timestamp{1710497730, 0}, Timestamp{1705287845, 0}}),
       makeFlatVector<std::string>({"yyyy-MM-dd", "HH:mm:ss"})});
  auto result =
      evaluate<SimpleVector<StringView>>("date_format(c0, c1)", input);
  EXPECT_EQ("2024-03-15", result->valueAt(0).str());
  EXPECT_EQ("03:04:05", result->valueAt(1).str());
}

TEST_F(DateFormatFlinkTest, LtzNullTimestampYieldsNull) {
  auto input = makeNullableFlatVector<int64_t>(
      std::vector<std::optional<int64_t>>(
          {std::optional<int64_t>(pack(kUtcMillis, tz::getTimeZoneID("UTC"))),
           std::nullopt}),
      TIMESTAMP_WITH_TIME_ZONE());
  auto result = evaluate<SimpleVector<StringView>>(
      "date_format(c0, 'yyyy-MM-dd HH:mm:ss')", makeRowVector({input}));
  ASSERT_FALSE(result->isNullAt(0));
  ASSERT_TRUE(result->isNullAt(1));
}

TEST_F(DateFormatFlinkTest, LtzNonConstFormatColumn) {
  setSessionTimezone("Asia/Shanghai");
  auto input = makeRowVector(
      {makeFlatVector<int64_t>(
           std::vector<int64_t>(
               {pack(kUtcMillis, tz::getTimeZoneID("UTC")),
                pack(kUtcMillis, tz::getTimeZoneID("UTC"))}),
           TIMESTAMP_WITH_TIME_ZONE()),
       makeFlatVector<std::string>({"yyyy-MM-dd", "HH:mm:ss"})});
  auto result =
      evaluate<SimpleVector<StringView>>("date_format(c0, c1)", input);
  EXPECT_EQ("2024-03-15", result->valueAt(0).str());
  EXPECT_EQ("18:15:30", result->valueAt(1).str());
}

} // namespace facebook::velox::functions::flinksql::test
