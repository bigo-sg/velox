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
#include <date/date.h>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "velox/core/Expressions.h"
#include "velox/functions/flinksql/RecordClock.h"
#include "velox/functions/flinksql/TemporalFunctions.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
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
    const auto daysSinceEpoch =
        std::chrono::floor<date::days>(localTimepoint.time_since_epoch())
            .count();
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
  ASSERT_EQ(expectedDateInZone(*tz::locateZone("Asia/Tokyo")), tokyo);
  ASSERT_EQ(expectedDateInZone(*tz::locateZone("America/Los_Angeles")), la);
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

// Tests for the temporal function group (CURRENT_TIMESTAMP / NOW /
// LOCALTIMESTAMP / LOCALTIME). Unpinned assertions are range assertions: on
// a thread with no emission-boundary refresh the functions fall back to a
// fresh wall-clock reading per record, so rows may legitimately differ from
// each other by milliseconds. Pinned assertions (RecordClock refreshed by
// the test) are exact: every occurrence in the same evaluation shares one
// instant, and LOCALTIMESTAMP / LOCALTIME derive from it.
class TemporalFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    // This test process registers no prestosql/sparksql/stateful baselines,
    // so the flinksql registrations are free of overwrite interference.
    memory::MemoryManager::testingSetInstance({});
  }

  // Pins the record clock for the test body and guarantees the pin is
  // dropped on exit: zero residue for later tests, independent of test
  // order.
  struct RecordClockPin {
    RecordClockPin() {
      RecordClock::refresh();
    }
    ~RecordClockPin() {
      RecordClock::reset();
    }
  };

  // Builds a no-argument call ("current_timestamp" and friends are SQL
  // keywords, so the expression tree is constructed directly).
  static core::TypedExprPtr makeNoArgCall(const std::string& name) {
    return std::make_shared<core::CallTypedExpr>(
        TIMESTAMP(), std::vector<core::TypedExprPtr>{}, name);
  }

  // Overload with an explicit result type: localtime returns INTEGER (the
  // TIME(0) milliseconds-of-day carrier), not TIMESTAMP.
  static core::TypedExprPtr makeNoArgCall(
      const std::string& name,
      const TypePtr& resultType) {
    return std::make_shared<core::CallTypedExpr>(
        resultType, std::vector<core::TypedExprPtr>{}, name);
  }

  // Evaluates a no-argument temporal function over `size` rows and returns
  // the per-row values (never NULL).
  std::vector<Timestamp> evaluateTemporal(
      const std::string& name,
      vector_size_t size) {
    auto rowVector =
        makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>(size, 0))});
    // Keep the result vector alive in a named owner; the raw pointer taken
    // below must not outlive it.
    auto resultVector = evaluate(makeNoArgCall(name), rowVector);
    auto* result = resultVector->asFlatVector<Timestamp>();
    EXPECT_EQ(result->size(), size);
    std::vector<Timestamp> values;
    values.reserve(size);
    for (auto i = 0; i < size; ++i) {
      EXPECT_FALSE(result->isNullAt(i)) << "row: " << i;
      values.push_back(result->valueAt(i));
    }
    return values;
  }

  // Same as evaluateTemporal, for the INTEGER-carrier localtime.
  std::vector<int32_t> evaluateLocalTime(
      const std::string& name,
      vector_size_t size) {
    auto rowVector =
        makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>(size, 0))});
    auto resultVector = evaluate(makeNoArgCall(name, INTEGER()), rowVector);
    auto* result = resultVector->asFlatVector<int32_t>();
    EXPECT_EQ(result->size(), size);
    std::vector<int32_t> values;
    values.reserve(size);
    for (auto i = 0; i < size; ++i) {
      EXPECT_FALSE(result->isNullAt(i)) << "row: " << i;
      values.push_back(result->valueAt(i));
    }
    return values;
  }

  // Circular difference modulo the day (86'400'000 ms), folded to
  // (-43'200'000, 43'200'000]: a linear difference breaks when the two
  // clocks straddle midnight (U7/U8/U9 all judge via this fold).
  static int64_t circularDiffMillis(int64_t a, int64_t b) {
    constexpr int64_t kDay = 86'400'000;
    int64_t d = (a - b) % kDay;
    if (d < -kDay / 2) {
      d += kDay;
    } else if (d > kDay / 2) {
      d -= kDay;
    }
    return d;
  }

  static Timestamp toTimestamp(std::chrono::system_clock::time_point tp) {
    return Timestamp::fromMillis(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            tp.time_since_epoch())
            .count());
  }

  static Timestamp minusOneSecond(const Timestamp& ts) {
    return Timestamp(ts.getSeconds() - 1, 0);
  }

  static Timestamp plusOneSecond(const Timestamp& ts) {
    return Timestamp(ts.getSeconds() + 1, 999'999'999);
  }

  static Timestamp minusTwoSeconds(const Timestamp& ts) {
    return Timestamp(ts.getSeconds() - 2, 0);
  }

  static Timestamp plusTwoSeconds(const Timestamp& ts) {
    return Timestamp(ts.getSeconds() + 2, 999'999'999);
  }
};

// U1: current_timestamp returns a timestamp within the test wall-clock
// window and has millisecond granularity.
TEST_F(TemporalFunctionsTest, currentTimestampBasic) {
  flinksql::registerFunctions("");
  auto t0 = toTimestamp(std::chrono::system_clock::now());
  auto values = evaluateTemporal("current_timestamp", 1);
  auto t1 = toTimestamp(std::chrono::system_clock::now());

  ASSERT_EQ(values.size(), 1);
  EXPECT_GE(values[0], minusOneSecond(t0));
  EXPECT_LE(values[0], plusOneSecond(t1));
  EXPECT_EQ(values[0].getNanos() % 1'000'000, 0);
}

// U2: current_timestamp is evaluated per record — every row falls in the
// wall-clock window, but rows are not required to be equal.
TEST_F(TemporalFunctionsTest, currentTimestampPerRecordRange) {
  flinksql::registerFunctions("");
  constexpr vector_size_t kSize = 100;
  auto t0 = toTimestamp(std::chrono::system_clock::now());
  auto values = evaluateTemporal("current_timestamp", kSize);
  auto t1 = toTimestamp(std::chrono::system_clock::now());

  ASSERT_EQ(values.size(), kSize);
  for (const auto& value : values) {
    EXPECT_GE(value, minusOneSecond(t0));
    EXPECT_LE(value, plusOneSecond(t1));
  }
}

// U3: now() is an alias of current_timestamp (same struct, dual registration).
TEST_F(TemporalFunctionsTest, nowAlias) {
  flinksql::registerFunctions("");
  constexpr vector_size_t kSize = 100;
  auto t0 = toTimestamp(std::chrono::system_clock::now());
  auto values = evaluateTemporal("now", kSize);
  auto t1 = toTimestamp(std::chrono::system_clock::now());

  ASSERT_EQ(values.size(), kSize);
  for (const auto& value : values) {
    EXPECT_GE(value, minusOneSecond(t0));
    EXPECT_LE(value, plusOneSecond(t1));
  }
}

// U4: localtimestamp with a UTC session timezone is the un-shifted epoch.
TEST_F(TemporalFunctionsTest, localTimestampUtc) {
  flinksql::registerFunctions("");
  setTimezone("UTC");
  constexpr vector_size_t kSize = 100;
  auto t0 = toTimestamp(std::chrono::system_clock::now());
  auto values = evaluateTemporal("localtimestamp", kSize);
  auto t1 = toTimestamp(std::chrono::system_clock::now());

  ASSERT_EQ(values.size(), kSize);
  for (const auto& value : values) {
    EXPECT_GE(value, minusTwoSeconds(t0));
    EXPECT_LE(value, plusTwoSeconds(t1));
  }
}

// U5: localtimestamp with an Asia/Shanghai session timezone is the epoch
// shifted by +8h (toTimezone conversion).
TEST_F(TemporalFunctionsTest, localTimestampShanghai) {
  flinksql::registerFunctions("");
  setTimezone("Asia/Shanghai");
  constexpr vector_size_t kSize = 100;
  constexpr int64_t kOffsetSeconds = 8 * 3600;
  auto t0 = toTimestamp(std::chrono::system_clock::now());
  auto values = evaluateTemporal("localtimestamp", kSize);
  auto t1 = toTimestamp(std::chrono::system_clock::now());

  ASSERT_EQ(values.size(), kSize);
  for (const auto& value : values) {
    EXPECT_GE(
        value, Timestamp(minusTwoSeconds(t0).getSeconds() + kOffsetSeconds, 0));
    EXPECT_LE(
        value,
        Timestamp(
            plusTwoSeconds(t1).getSeconds() + kOffsetSeconds, 999'999'999));
  }
}

// U6: in the same projection (same ExprSet pass) with the record clock
// pinned (emission-boundary shape), localtimestamp - current_timestamp is
// exactly +8h for every row in Asia/Shanghai: both derive from the same
// shared instant.
TEST_F(TemporalFunctionsTest, localTimestampDerivedFromCurrentTimestamp) {
  flinksql::registerFunctions("");
  setTimezone("Asia/Shanghai");
  RecordClockPin pin;
  constexpr vector_size_t kSize = 100;
  constexpr int64_t kOffsetMillis = 8LL * 3600 * 1000;

  auto rowVector =
      makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>(kSize, 0))});
  exec::ExprSet exprSet(
      {makeNoArgCall("current_timestamp"), makeNoArgCall("localtimestamp")},
      &execCtx_);
  exec::EvalCtx context(&execCtx_, &exprSet, rowVector.get());
  std::vector<VectorPtr> results(2);
  SelectivityVector rows(kSize);
  exprSet.eval(rows, context, results);

  auto* currentTimestamp = results[0]->asFlatVector<Timestamp>();
  auto* localTimestamp = results[1]->asFlatVector<Timestamp>();
  ASSERT_EQ(currentTimestamp->size(), kSize);
  ASSERT_EQ(localTimestamp->size(), kSize);
  for (auto i = 0; i < kSize; ++i) {
    const auto diffMillis = localTimestamp->valueAt(i).toMillis() -
        currentTimestamp->valueAt(i).toMillis();
    EXPECT_EQ(diffMillis, kOffsetMillis) << "row: " << i;
  }
}

// U7: localtime at UTC with the record clock pinned — every row lands in
// [0, 86'400'000), never NULL, carries the INTEGER type, and equals the
// same row's localtimestamp milliseconds-of-day exactly.
TEST_F(TemporalFunctionsTest, localTimeUtcRangeAndDerivation) {
  flinksql::registerFunctions("");
  setTimezone("UTC");
  RecordClockPin pin;
  constexpr vector_size_t kSize = 100;

  auto rowVector =
      makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>(kSize, 0))});
  exec::ExprSet exprSet(
      {makeNoArgCall("localtime", INTEGER()), makeNoArgCall("localtimestamp")},
      &execCtx_);
  exec::EvalCtx context(&execCtx_, &exprSet, rowVector.get());
  std::vector<VectorPtr> results(2);
  SelectivityVector rows(kSize);
  exprSet.eval(rows, context, results);

  auto* localTime = results[0]->asFlatVector<int32_t>();
  auto* localTimestamp = results[1]->asFlatVector<Timestamp>();
  ASSERT_EQ(localTime->size(), kSize);
  ASSERT_EQ(localTimestamp->size(), kSize);
  EXPECT_EQ(results[0]->type()->kind(), TypeKind::INTEGER);
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_FALSE(localTime->isNullAt(i)) << "row: " << i;
    const auto v = localTime->valueAt(i);
    EXPECT_GE(v, 0) << "row: " << i;
    EXPECT_LT(v, 86'400'000) << "row: " << i;
    const int64_t derived =
        millisOfDay(localTimestamp->valueAt(i).toMillis());
    EXPECT_EQ(v, static_cast<int32_t>(derived)) << "row: " << i;
  }
}

// U8: localtime at Asia/Shanghai with the record clock pinned — same
// assertions as U7 against the Shanghai-shifted localtimestamp (the +8h
// offset must survive the floor-mod day wrap).
TEST_F(TemporalFunctionsTest, localTimeShanghaiRangeAndDerivation) {
  flinksql::registerFunctions("");
  setTimezone("Asia/Shanghai");
  RecordClockPin pin;
  constexpr vector_size_t kSize = 100;

  auto rowVector =
      makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>(kSize, 0))});
  exec::ExprSet exprSet(
      {makeNoArgCall("localtime", INTEGER()), makeNoArgCall("localtimestamp")},
      &execCtx_);
  exec::EvalCtx context(&execCtx_, &exprSet, rowVector.get());
  std::vector<VectorPtr> results(2);
  SelectivityVector rows(kSize);
  exprSet.eval(rows, context, results);

  auto* localTime = results[0]->asFlatVector<int32_t>();
  auto* localTimestamp = results[1]->asFlatVector<Timestamp>();
  ASSERT_EQ(localTime->size(), kSize);
  ASSERT_EQ(localTimestamp->size(), kSize);
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_FALSE(localTime->isNullAt(i)) << "row: " << i;
    const auto v = localTime->valueAt(i);
    EXPECT_GE(v, 0) << "row: " << i;
    EXPECT_LT(v, 86'400'000) << "row: " << i;
    const int64_t derived =
        millisOfDay(localTimestamp->valueAt(i).toMillis());
    EXPECT_EQ(v, static_cast<int32_t>(derived)) << "row: " << i;
  }
}

// U9: localtime wall-clock cross-check without localtimestamp — every row
// falls in the circular window [t0 - 1s, t1 + 1s] around the test's own
// wall clock (t0 before, t1 after the evaluation).
TEST_F(TemporalFunctionsTest, localTimeWallClockWindow) {
  flinksql::registerFunctions("");
  setTimezone("UTC");
  constexpr vector_size_t kSize = 100;
  auto t0 = toTimestamp(std::chrono::system_clock::now());
  auto values = evaluateLocalTime("localtime", kSize);
  auto t1 = toTimestamp(std::chrono::system_clock::now());

  ASSERT_EQ(values.size(), kSize);
  const int64_t t0Millis = t0.toMillis();
  const int64_t t1Millis = t1.toMillis();
  for (const auto v : values) {
    // Not before t0 - 1s ...
    EXPECT_GE(circularDiffMillis(v, t0Millis % 86'400'000), -1'000);
    // ... and not after t1 + 1s, both judged on the circular day clock.
    EXPECT_LE(circularDiffMillis(v, t1Millis % 86'400'000), 1'000);
  }
}

// U10: with the record clock pinned, two independent current_timestamp
// call sites in the same ExprSet evaluate equal on every row — the same-row
// multi-occurrence consistency Flink guarantees per record.
TEST_F(TemporalFunctionsTest, currentTimestampSameRowEqual) {
  flinksql::registerFunctions("");
  setTimezone("UTC");
  RecordClockPin pin;
  constexpr vector_size_t kSize = 100;

  auto rowVector =
      makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>(kSize, 0))});
  exec::ExprSet exprSet(
      {makeNoArgCall("current_timestamp"), makeNoArgCall("current_timestamp")},
      &execCtx_);
  exec::EvalCtx context(&execCtx_, &exprSet, rowVector.get());
  std::vector<VectorPtr> results(2);
  SelectivityVector rows(kSize);
  exprSet.eval(rows, context, results);

  auto* first = results[0]->asFlatVector<Timestamp>();
  auto* second = results[1]->asFlatVector<Timestamp>();
  ASSERT_EQ(first->size(), kSize);
  ASSERT_EQ(second->size(), kSize);
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_EQ(first->valueAt(i), second->valueAt(i)) << "row: " << i;
  }
}

// U11: with the record clock pinned, now() agrees with current_timestamp on
// every row — the dual registration names share one instant per record.
TEST_F(TemporalFunctionsTest, nowAliasSameRowEqual) {
  flinksql::registerFunctions("");
  setTimezone("UTC");
  RecordClockPin pin;
  constexpr vector_size_t kSize = 100;

  auto rowVector =
      makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>(kSize, 0))});
  exec::ExprSet exprSet(
      {makeNoArgCall("current_timestamp"), makeNoArgCall("now")},
      &execCtx_);
  exec::EvalCtx context(&execCtx_, &exprSet, rowVector.get());
  std::vector<VectorPtr> results(2);
  SelectivityVector rows(kSize);
  exprSet.eval(rows, context, results);

  auto* currentTimestamp = results[0]->asFlatVector<Timestamp>();
  auto* now = results[1]->asFlatVector<Timestamp>();
  ASSERT_EQ(currentTimestamp->size(), kSize);
  ASSERT_EQ(now->size(), kSize);
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_EQ(currentTimestamp->valueAt(i), now->valueAt(i)) << "row: " << i;
  }
}

// U12: each refresh re-pins the instant — after sleeping past the clock
// granularity, a refreshed evaluation strictly exceeds the previous one,
// and both readings stay inside the test wall-clock window.
TEST_F(TemporalFunctionsTest, refreshAdvancesInstantPerRecord) {
  flinksql::registerFunctions("");
  RecordClockPin pin;
  auto t0 = toTimestamp(std::chrono::system_clock::now());
  RecordClock::refresh();
  auto first = evaluateTemporal("current_timestamp", 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(3));
  RecordClock::refresh();
  auto second = evaluateTemporal("current_timestamp", 1);
  auto t1 = toTimestamp(std::chrono::system_clock::now());

  ASSERT_EQ(first.size(), 1);
  ASSERT_EQ(second.size(), 1);
  EXPECT_GT(second[0], first[0]);
  EXPECT_GE(first[0], minusOneSecond(t0));
  EXPECT_LE(first[0], plusOneSecond(t1));
  EXPECT_GE(second[0], minusOneSecond(t0));
  EXPECT_LE(second[0], plusOneSecond(t1));
}

// U13: unpinned fallback — after a reset, two evaluations separated by a
// sleep read the wall clock independently (per-call values); the later one
// is not before the earlier one.
TEST_F(TemporalFunctionsTest, unpinnedFallbackReadsPerCall) {
  flinksql::registerFunctions("");
  RecordClock::reset();
  auto first = evaluateTemporal("current_timestamp", 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(3));
  auto second = evaluateTemporal("current_timestamp", 1);

  ASSERT_EQ(first.size(), 1);
  ASSERT_EQ(second.size(), 1);
  EXPECT_GE(second[0], first[0]);
}

// U14: millisOfDay is a floor-mod — table-driven over the day boundary and
// pre-1970 (negative) local millis, all results land in [0, 86'400'000).
TEST_F(TemporalFunctionsTest, millisOfDayFloorMod) {
  constexpr int64_t kDay = 86'400'000;
  const std::vector<std::pair<int64_t, int64_t>> cases = {
      {0, 0},
      {kDay - 1, kDay - 1},
      {kDay, 0},
      {-1, kDay - 1},
      {-kDay, 0},
      {-kDay - 1, kDay - 1},
      // Pre-1970 local midnight minus 7h: folds back to 17:00 of the
      // previous local day.
      {-(kDay + 7LL * 3600 * 1000), kDay - 7LL * 3600 * 1000}};
  for (const auto& [input, expected] : cases) {
    const auto actual = millisOfDay(input);
    EXPECT_GE(actual, 0) << "input: " << input;
    EXPECT_LT(actual, kDay) << "input: " << input;
    EXPECT_EQ(actual, expected) << "input: " << input;
  }
}

} // namespace facebook::velox::functions::flinksql::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}
