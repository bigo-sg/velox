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
#include <chrono>
#include <cmath>
#include <date/date.h>
#include <string>

#include "velox/functions/flinksql/TemporalFunctions.h"
#include "velox/functions/flinksql/Register.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::functions::flinksql::test {

class FlinkTemporalTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    flinksql::registerTemporalFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }

  // Wall-clock date of "now" in the zone, in days since epoch.
  int32_t evaluateCurrentDate() {
    auto expr = std::make_shared<core::CallTypedExpr>(
        DATE(), std::vector<core::TypedExprPtr>{}, "current_date");
    auto result = evaluate(expr, makeRowVector(ROW({}), 1));
    return result->as<SimpleVector<int32_t>>()->valueAt(0);
  }

  static int32_t expectedDateInZone(const tz::TimeZone& zone) {
    auto now = Timestamp::now();
    now.toTimezone(zone);
    const std::chrono::
        time_point<std::chrono::system_clock, std::chrono::milliseconds>
            localTimepoint(std::chrono::milliseconds(now.toMillis()));
    const auto daysSinceEpoch = std::chrono::floor<date::days>(
        localTimepoint.time_since_epoch()).count();
    return static_cast<int32_t>(daysSinceEpoch);
  }
};

TEST_F(FlinkTemporalTest, currentDateDefaultZone) {
  // Default zone: any valid date near today (±1 day for midnight races).
  const auto result = evaluateCurrentDate();
  const auto today = expectedDateInZone(*tz::locateZone("UTC"));
  ASSERT_GE(result, today - 1);
  ASSERT_LE(result, today + 1);
}

TEST_F(FlinkTemporalTest, currentDateSessionTimezone) {
  setTimezone("Asia/Shanghai");
  const auto shanghai = evaluateCurrentDate();
  const auto expected = expectedDateInZone(*tz::locateZone("Asia/Shanghai"));
  ASSERT_EQ(expected, shanghai);
}

TEST_F(FlinkTemporalTest, currentDateAcrossZonesSameRun) {
  // Zones on different calendar dates at the same instant: the function
  // must follow the session zone.
  setTimezone("Asia/Tokyo");
  const auto tokyo = evaluateCurrentDate();
  setTimezone("America/Los_Angeles");
  const auto la = evaluateCurrentDate();
  ASSERT_EQ(
      expectedDateInZone(*tz::locateZone("Asia/Tokyo")), tokyo);
  ASSERT_EQ(
      expectedDateInZone(*tz::locateZone("America/Los_Angeles")), la);
}

// CURRENT_TIME: the session-timezone wall clock as millisecond-of-day INT.
class FlinkCurrentTimeTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    flinksql::registerTemporalFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }

  int32_t evaluateCurrentTime() {
    auto expr = std::make_shared<core::CallTypedExpr>(
        INTEGER(), std::vector<core::TypedExprPtr>{}, "current_time");
    auto result = evaluate(expr, makeRowVector(ROW({}), 1));
    return result->as<SimpleVector<int32_t>>()->valueAt(0);
  }

  static int32_t millisOfDayOfNowInZone(const tz::TimeZone& zone) {
    auto now = Timestamp::now();
    now.toTimezone(zone);
    return static_cast<int32_t>(now.toMillis() % 86'400'000);
  }

  // Distance on the millisecond-of-day circle (midnight-safe).
  static int32_t dayDistance(int32_t a, int32_t b) {
    const auto delta = std::abs(a - b);
    return std::min(delta, 86'400'000 - delta);
  }
};

TEST_F(FlinkCurrentTimeTest, millisOfDayRange) {
  setTimezone("Asia/Shanghai");
  const auto value = evaluateCurrentTime();
  ASSERT_GE(value, 0);
  ASSERT_LT(value, 86'400'000);
}

TEST_F(FlinkCurrentTimeTest, sessionTimezoneWallClock) {
  setTimezone("Asia/Shanghai");
  const auto value = evaluateCurrentTime();
  const auto expected =
      millisOfDayOfNowInZone(*tz::locateZone("Asia/Shanghai"));
  ASSERT_LE(dayDistance(value, expected), 120'000)
      << value << " vs " << expected;
}

TEST_F(FlinkCurrentTimeTest, followsSessionZone) {
  setTimezone("Asia/Tokyo");
  const auto tokyo = evaluateCurrentTime();
  setTimezone("America/Los_Angeles");
  const auto la = evaluateCurrentTime();
  const auto tokyoMillis =
      millisOfDayOfNowInZone(*tz::locateZone("Asia/Tokyo"));
  const auto laMillis =
      millisOfDayOfNowInZone(*tz::locateZone("America/Los_Angeles"));
  // Each result within two minutes of its zone's wall clock; the two zones
  // are hours apart, so the results must differ by hours too.
  ASSERT_LE(dayDistance(tokyo, tokyoMillis), 120'000);
  ASSERT_LE(dayDistance(la, laMillis), 120'000);
  ASSERT_GE(dayDistance(tokyo, la), 60'000);
}

} // namespace facebook::velox::functions::flinksql::test
