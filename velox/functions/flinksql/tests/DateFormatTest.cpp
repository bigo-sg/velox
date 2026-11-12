/*
 * Copyright (c) Contributors to the GFVBot project.
 * Licensed under the Apache License, Version 2.0.
 */
#include <gtest/gtest.h>
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
  std::string evalDateFormat(Timestamp ts) {
    auto input = makeRowVector({makeFlatVector<Timestamp>({ts})});
    auto result = evaluate<SimpleVector<StringView>>(
        "date_format(c0, 'yyyy-MM-dd HH:mm:ss')", input);
    return result->valueAt(0).str();
  }

  std::string evalDateFormatLtz(int64_t utcMillis, int16_t zoneId) {
    // Explicit vector: a single-element braced list resolves to the
    // (size_t, TypePtr) overload and treats the packed value as the length.
    auto input = makeFlatVector<int64_t>(
        std::vector<int64_t>({pack(utcMillis, zoneId)}),
        TIMESTAMP_WITH_TIME_ZONE());
    auto result = evaluate<SimpleVector<StringView>>(
        "date_format(c0, 'yyyy-MM-dd HH:mm:ss')", makeRowVector({input}));
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

// The session zone wins over the value's embedded zone (Los_Angeles): the
// sparksql implementation would render 02:15:30 instead.
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

} // namespace facebook::velox::functions::flinksql::test
