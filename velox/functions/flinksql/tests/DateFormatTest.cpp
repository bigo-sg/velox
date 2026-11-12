/*
 * Copyright (c) Contributors to the GFVBot project.
 * Licensed under the Apache License, Version 2.0.
 *
 * Tests for flinksql date_format: the no-timezone TIMESTAMP renders its
 * wall clock — the session timezone does NOT apply. Verified under a
 * non-UTC session timezone (Asia/Shanghai) to prevent the timezone-offset
 * regression class (the inherited sparksql implementation applies the
 * session zone when it reaches the function, producing a +offset shift).
 */
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include <gtest/gtest.h>

namespace facebook::velox::functions::flinksql::test {

class DateFormatFlinkTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    flinksql::registerFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }

  void setSessionTimezone(const std::string& tz) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSessionTimezone, tz}});
  }

  std::string evalDateFormat(const std::string& timestampLiteral) {
    auto result = evaluate<SimpleVector<StringView>>(
        fmt::format(
            "date_format(TIMESTAMP '{}', 'yyyy-MM-dd HH:mm:ss')",
            timestampLiteral),
        makeRowVector(ROW({}, {}), 1));
    return result->valueAt(0).str();
  }
};

// Core regression test: under a non-UTC session timezone, DATE_FORMAT on a
// no-timezone TIMESTAMP renders the wall clock. The inherited sparksql
// implementation would produce '+8' shift (e.g., '18:15:30' for an input
// of '10:15:30') if the session timezone reached the function.
TEST_F(DateFormatFlinkTest, WallClockUnderNonUtcSessionTimezone) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(
      "2024-03-15 10:15:30",
      evalDateFormat("2024-03-15 10:15:30"));
}

TEST_F(DateFormatFlinkTest, WallClockUnderUtcSessionTimezone) {
  setSessionTimezone("UTC");
  EXPECT_EQ(
      "2024-03-15 10:15:30",
      evalDateFormat("2024-03-15 10:15:30"));
}

TEST_F(DateFormatFlinkTest, WallClockDifferentValues) {
  setSessionTimezone("Asia/Shanghai");
  EXPECT_EQ(
      "2024-01-15 03:04:05",
      evalDateFormat("2024-01-15 03:04:05"));
  EXPECT_EQ(
      "2023-12-31 23:59:59",
      evalDateFormat("2023-12-31 23:59:59"));
  EXPECT_EQ(
      "2024-06-01 00:00:00",
      evalDateFormat("2024-06-01 00:00:00"));
}

} // namespace facebook::velox::functions::flinksql::test
