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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/flinksql/IntervalArithmetic.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::flinksql::test {
namespace {

// 2017-11-29 10:58:58.998 UTC in micros.
constexpr int64_t kTsMicros = 1511947138998000;
constexpr int64_t kHourMillis = 3'600'000;

class IntervalArithmeticTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    functions::test::FunctionBaseTest::SetUpTestCase();
    functions::flinksql::registerFunctions("");
  }

  std::optional<Timestamp> plusTs(
      const std::optional<int64_t>& tsMicros,
      int64_t millis) {
    return evaluateOnce<Timestamp>(
        "plus(c0, c1)",
        makeRowVector({
            makeNullableFlatVector<Timestamp>(
                std::vector<std::optional<Timestamp>>{
                    tsMicros.has_value() ? Timestamp::fromMicros(*tsMicros)
                                         : std::optional<Timestamp>{}},
                TIMESTAMP()),
            makeNullableFlatVector<int64_t>({millis}, INTERVAL_DAY_TIME()),
        }));
  }

  std::optional<Timestamp> plusTsMonths(
      const std::optional<int64_t>& tsMicros,
      std::optional<int32_t> months) {
    return evaluateOnce<Timestamp>(
        "plus(c0, c1)",
        makeRowVector({
            makeNullableFlatVector<Timestamp>(
                std::vector<std::optional<Timestamp>>{
                    tsMicros.has_value() ? Timestamp::fromMicros(*tsMicros)
                                         : std::optional<Timestamp>{}},
                TIMESTAMP()),
            makeNullableFlatVector<int32_t>({months}, INTERVAL_YEAR_MONTH()),
        }));
  }

  std::optional<int32_t> plusDate(
      const std::optional<int32_t>& date,
      int64_t millis) {
    auto data = makeRowVector({
        makeNullableFlatVector<int32_t>({date}, DATE()),
        makeNullableFlatVector<int64_t>({millis}, INTERVAL_DAY_TIME()),
    });
    auto result =
        evaluate(makeTypedExpr("plus(c0, c1)", asRowType(data->type())), data);
    auto flat = std::reinterpret_pointer_cast<FlatVector<int32_t>>(result);
    return flat->isNullAt(0) ? std::nullopt
                             : std::optional<int32_t>(flat->valueAt(0));
  }

  std::optional<int32_t> plusDateMonths(
      const std::optional<int32_t>& date,
      std::optional<int32_t> months) {
    auto data = makeRowVector({
        makeNullableFlatVector<int32_t>({date}, DATE()),
        makeNullableFlatVector<int32_t>({months}, INTERVAL_YEAR_MONTH()),
    });
    auto result =
        evaluate(makeTypedExpr("plus(c0, c1)", asRowType(data->type())), data);
    auto flat = std::reinterpret_pointer_cast<FlatVector<int32_t>>(result);
    return flat->isNullAt(0) ? std::nullopt
                             : std::optional<int32_t>(flat->valueAt(0));
  }

  std::optional<int64_t> multiplyInterval(
      int64_t millis,
      std::optional<int64_t> n) {
    auto data = makeRowVector({
        makeNullableFlatVector<int64_t>({millis}, INTERVAL_DAY_TIME()),
        makeNullableFlatVector<int64_t>({n}, BIGINT()),
    });
    auto result = evaluate(
        makeTypedExpr("multiply(c0, c1)", asRowType(data->type())), data);
    auto flat = std::reinterpret_pointer_cast<FlatVector<int64_t>>(result);
    return flat->isNullAt(0) ? std::nullopt
                             : std::optional<int64_t>(flat->valueAt(0));
  }

  std::optional<int32_t> multiplyMonths(
      int32_t months,
      std::optional<int64_t> n) {
    auto data = makeRowVector({
        makeNullableFlatVector<int32_t>({months}, INTERVAL_YEAR_MONTH()),
        makeNullableFlatVector<int64_t>({n}, BIGINT()),
    });
    auto result = evaluate(
        makeTypedExpr("multiply(c0, c1)", asRowType(data->type())), data);
    auto flat = std::reinterpret_pointer_cast<FlatVector<int32_t>>(result);
    return flat->isNullAt(0) ? std::nullopt
                             : std::optional<int32_t>(flat->valueAt(0));
  }
};

TEST_F(IntervalArithmeticTest, timestampPlusIntervalDayTime) {
  // 2017-11-29 10:58:58.998 + 8h = 18:58:58.998 (card anchor).
  EXPECT_EQ(
      Timestamp::fromMicros(kTsMicros + 8 * kHourMillis * 1000),
      plusTs(kTsMicros, 8 * kHourMillis));
  // Sub-millisecond part survives.
  EXPECT_EQ(
      Timestamp::fromMicros(kTsMicros + 288000000LL * 1000),
      plusTs(kTsMicros, 288000000));
  // Negative interval via minus.
  EXPECT_EQ(
      Timestamp::fromMicros(kTsMicros - 5000LL * 1000),
      evaluateOnce<Timestamp>(
          "minus(c0, c1)",
          makeRowVector({
              makeNullableFlatVector<Timestamp>(
                  {Timestamp::fromMicros(kTsMicros)}, TIMESTAMP()),
              makeNullableFlatVector<int64_t>({5000}, INTERVAL_DAY_TIME()),
          })));
  // NULL propagates.
  EXPECT_EQ(std::nullopt, plusTs(std::nullopt, kHourMillis));
  // Interval on the left.
  EXPECT_EQ(
      Timestamp::fromMicros(kTsMicros + kHourMillis * 1000),
      evaluateOnce<Timestamp>(
          "plus(c0, c1)",
          makeRowVector({
              makeNullableFlatVector<int64_t>(
                  {kHourMillis}, INTERVAL_DAY_TIME()),
              makeNullableFlatVector<Timestamp>(
                  {Timestamp::fromMicros(kTsMicros)}, TIMESTAMP()),
          })));
}

TEST_F(IntervalArithmeticTest, timestampPlusIntervalYearMonth) {
  // 2017-11-29 + 1 month = 2017-12-29 (card anchor).
  EXPECT_EQ(
      Timestamp::fromMicros(kTsMicros + 30 * 86'400'000'000LL),
      plusTsMonths(kTsMicros, 1));
  // Month-end clamping: 2025-01-31 + 1 month = 2025-02-28.
  const int64_t jan31 = 1738367999999000; // 2025-01-31 23:59:59.999
  EXPECT_EQ(
      Timestamp::fromMicros(1740787199999000), // 2025-02-28 23:59:59.999
      plusTsMonths(jan31, 1));
  // Timezone-naive: same wall-clock result regardless of session tz.
  queryCtx_->testingOverrideConfigUnsafe({
      {core::QueryConfig::kSessionTimezone, "Asia/Shanghai"},
      {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
  });
  EXPECT_EQ(Timestamp::fromMicros(1740787199999000), plusTsMonths(jan31, 1));
  // NULL propagates.
  EXPECT_EQ(std::nullopt, plusTsMonths(kTsMicros, std::nullopt));
}

TEST_F(IntervalArithmeticTest, datePlusInterval) {
  // 2017-11-29 (day 17499) + 25h = 2017-11-30 (day 17500, truncation anchor).
  EXPECT_EQ(17500, plusDate(17499, 25 * kHourMillis));
  // Negative day-time interval truncates toward zero (Calcite:
  // date + intervalMillis / 86400000, Java truncation).
  EXPECT_EQ(17497, plusDate(17498, -25 * kHourMillis));
  // Date + month clamping: 2025-01-31 (day 20119) + 1M = 2025-02-28 (day
  // 20147).
  EXPECT_EQ(20147, plusDateMonths(20119, 1));
  // Date - month: 2025-01-31 - 1M = 2024-12-31 (day 20088).
  EXPECT_EQ(20088, plusDateMonths(20119, -1));
  // NULL propagates.
  EXPECT_EQ(std::nullopt, plusDate(std::nullopt, kHourMillis));
}

TEST_F(IntervalArithmeticTest, intervalMultiply) {
  EXPECT_EQ(3 * kHourMillis, multiplyInterval(kHourMillis, 3));
  EXPECT_EQ(-2 * kHourMillis, multiplyInterval(kHourMillis, -2));
  EXPECT_EQ(std::nullopt, multiplyInterval(kHourMillis, std::nullopt));
  EXPECT_EQ(6, multiplyMonths(3, 2));
}

TEST_F(IntervalArithmeticTest, timestampPlusMinusIntervalPreservesNanos) {
  // Sub-microsecond nanoseconds must survive interval arithmetic:
  // .123456789 + 1ms = .124456789, .123456789 - 1ms = .122456789.
  const int64_t sec = 1511947138;
  const uint64_t nanos = 123456789;
  Timestamp ts(sec, nanos);

  // IntervalDayTime is in milliseconds: 1ms = 1000 micros.
  auto plus = evaluateOnce<Timestamp>(
      "plus(c0, c1)",
      makeRowVector({
          makeNullableFlatVector<Timestamp>({ts}, TIMESTAMP()),
          makeNullableFlatVector<int64_t>({1}, INTERVAL_DAY_TIME()),
      }));
  ASSERT_TRUE(plus.has_value());
  EXPECT_EQ(sec, plus->getSeconds());
  EXPECT_EQ(124456789, plus->getNanos());

  auto minus = evaluateOnce<Timestamp>(
      "minus(c0, c1)",
      makeRowVector({
          makeNullableFlatVector<Timestamp>({ts}, TIMESTAMP()),
          makeNullableFlatVector<int64_t>({1}, INTERVAL_DAY_TIME()),
      }));
  ASSERT_TRUE(minus.has_value());
  EXPECT_EQ(sec, minus->getSeconds());
  EXPECT_EQ(122456789, minus->getNanos());
}

} // namespace
} // namespace facebook::velox::functions::flinksql::test
