/*
 * Copyright (c) Contributors to the GFVBot project.
 * Licensed under the Apache License, Version 2.0.
 *
 * Tests for flinksql date_format:
 * - TIMESTAMP (no timezone) renders its wall clock — the session timezone
 *   does NOT apply (the inherited sparksql implementation applies the
 *   session zone when it reaches the function, producing a +offset shift).
 * - TIMESTAMP WITH TIME ZONE renders the real moment in the session
 *   timezone (the inherited sparksql implementation renders in the value's
 *   embedded zone instead).
 * Both verified under a non-UTC session timezone (Asia/Shanghai) to prevent
 * the timezone-offset regression class.
 */
#include <gtest/gtest.h>
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::flinksql::test {

class DateFormatFlinkTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    // Same setup pattern as DateTimeFunctionsTest: the prestosql baseline
    // registered by FunctionBaseTest is overridden by the flinksql layer.
    functions::test::FunctionBaseTest::SetUpTestCase();
    flinksql::registerFunctions("");
  }

  void setSessionTimezone(const std::string& tz) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSessionTimezone, tz}});
  }

  // Timestamps are passed as vector inputs (the established flinksql test
  // idiom): a "TIMESTAMP '...'" literal in the expression string parses via
  // duckdb into a cast call, and the cast special form is not registered in
  // this test binary.
  std::string evalDateFormat(Timestamp ts) {
    auto input = makeRowVector({makeFlatVector<Timestamp>({ts})});
    auto result = evaluate<SimpleVector<StringView>>(
        "date_format(c0, 'yyyy-MM-dd HH:mm:ss')", input);
    return result->valueAt(0).str();
  }

  std::string evalDateFormatLtz(int64_t utcMillis, int16_t zoneId) {
    // Explicit std::vector: a single-element braced list would resolve to the
    // makeFlatVector<T>(size_t, TypePtr) overload and treat the packed value
    // as the vector length.
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

// Core regression test: under a non-UTC session timezone, DATE_FORMAT on a
// no-timezone TIMESTAMP renders the wall clock. The inherited sparksql
// implementation would produce '+8' shift (e.g., '18:15:30' for an input
// of '10:15:30') if the session timezone reached the function.
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

// DATE_FORMAT on TIMESTAMP WITH TIME ZONE renders the real moment in the
// session timezone: UTC instant 2024-03-15 10:15:30 → Shanghai wall clock
// 18:15:30 regardless of the value's embedded zone.
TEST_F(DateFormatFlinkTest, LtzRendersInSessionTimezone) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(
      "2024-03-15 18:15:30",
      evalDateFormatLtz(kUtcMillis, tz::getTimeZoneID("UTC")));
}

// The session zone wins over the value's embedded zone: the embedded zone
// is America/Los_Angeles (-8) here, yet the rendering follows Shanghai
// (+8). The inherited sparksql implementation would render 02:15:30 (the
// embedded zone's wall clock) instead.
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
