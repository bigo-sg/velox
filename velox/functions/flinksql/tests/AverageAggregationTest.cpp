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
#include <limits>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/Aggregate.h"
#include "velox/functions/flinksql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::flinksql::test {
namespace {

class AverageAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    aggregate::prestosql::registerAllAggregateFunctions();
    // Flink's avg semantics replace the presto baseline (overwrite), the same
    // layering initForFlink applies in velox4j.
    aggregate::flinksql::registerAggregateFunctions();
  }

  // Runs a single-step aggregation (the only step the factory supports) and
  // verifies the result. Comparison is order-insensitive because grouped
  // aggregation output order is not deterministic.
  void assertSingleAggregation(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const RowVectorPtr& expectedResult) {
    auto plan = PlanBuilder()
                    .values(input)
                    .singleAggregation(groupingKeys, aggregates)
                    .planNode();
    AssertQueryBuilder(plan).assertResults(expectedResult);
  }
};

// Flink's avg keeps the integral input type: the sum accumulates in BIGINT
// and the final value is a division truncating toward zero, e.g. avg(-7, 2)
// is -2, not the -3 a flooring division would give.
TEST_F(AverageAggregationTest, avgIntegralTruncatesTowardZero) {
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int8_t>({1, 2}, TINYINT())})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int8_t>({1}, TINYINT())}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int16_t>({-7, 2}, SMALLINT())})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int16_t>({-2}, SMALLINT())}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int32_t>({7, 2})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int32_t>({4})}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>({-7, 2})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int64_t>({-2})}));
}

// The BIGINT sum wraps around like Java's long addition instead of failing,
// so averages of extreme values may be surprising but stay deterministic.
TEST_F(AverageAggregationTest, avgBigIntSumWrapsLikeJavaLong) {
  const auto kMax = std::numeric_limits<int64_t>::max();
  const auto kMin = std::numeric_limits<int64_t>::min();

  // 2 * kMax + 3 wraps to 1; 1 / 3 truncates to 0.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>({kMax, kMax, 3})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int64_t>({0})}));

  // 3 * kMin wraps to kMin; kMin / 3 truncates toward zero.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>({kMin, kMin, kMin})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int64_t>({-3074457345618258602})}));

  // kMax + 1 wraps to kMin, so the average lands far in the negatives.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>({kMax, 1})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int64_t>({-4611686018427387904})}));
}

// A constant input takes the multiply-once fast path, which must wrap the
// same way as the per-row addition: 3 * kMax wraps to kMax - 2.
TEST_F(AverageAggregationTest, avgConstantBigIntWrapsLikeJavaLong) {
  const auto kMax = std::numeric_limits<int64_t>::max();
  assertSingleAggregation(
      {makeRowVector({makeConstant<int64_t>(kMax, 3)})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int64_t>({3074457345618258601})}));
}

// Null inputs are skipped; a group with only nulls (or no input at all)
// produces null.
TEST_F(AverageAggregationTest, avgIntegralNullsAndEmpty) {
  assertSingleAggregation(
      {makeRowVector(
          {makeNullableFlatVector<int64_t>({1, std::optional<int64_t>(), 3})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int64_t>({2})}));

  assertSingleAggregation(
      {makeRowVector({makeAllNullFlatVector<int64_t>(10)})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeNullableFlatVector<int64_t>({std::optional<int64_t>()})}));

  // Global aggregation over zero rows still emits one null row.
  assertSingleAggregation(
      {makeRowVector({makeFlatVector<int64_t>(std::vector<int64_t>{})})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeNullableFlatVector<int64_t>({std::optional<int64_t>()})}));

  // Grouped: group 0 holds one value, group 1 is all null.
  assertSingleAggregation(
      {makeRowVector(
          {makeNullableFlatVector<int64_t>({0, 0, 1, 1}),
           makeNullableFlatVector<int64_t>(
               {std::optional<int64_t>(),
                5,
                std::optional<int64_t>(),
                std::optional<int64_t>()})})},
      {"c0"},
      {"avg(c1)"},
      makeRowVector(
          {makeNullableFlatVector<int64_t>({0, 1}),
           makeNullableFlatVector<int64_t>({5, std::optional<int64_t>()})}));
}

// Grouped aggregation with per-group accumulators: group 0's sum wraps,
// group 1's does not (31 / 2 truncates to 15).
TEST_F(AverageAggregationTest, avgGroupedBigInt) {
  const auto kMax = std::numeric_limits<int64_t>::max();
  assertSingleAggregation(
      {makeRowVector(
          {makeNullableFlatVector<int64_t>({0, 0, 0, 1, 1}),
           makeNullableFlatVector<int64_t>({kMax, kMax, 3, 10, 21})})},
      {"c0"},
      {"avg(c1)"},
      makeRowVector(
          {makeNullableFlatVector<int64_t>({0, 1}),
           makeNullableFlatVector<int64_t>({0, 15})}));
}

// REAL accumulates the sum in DOUBLE and narrows the quotient to REAL;
// DOUBLE keeps the double quotient as is.
TEST_F(AverageAggregationTest, avgFloatingPoint) {
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<double>({1.5, 2.5, 3.25})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<double>({7.25 / 3.0})}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<double>(
          {1.5, std::optional<double>(), 3.25})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<double>({4.75 / 2.0})}));

  assertSingleAggregation(
      {makeRowVector({makeAllNullFlatVector<double>(7)})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeNullableFlatVector<double>({std::optional<double>()})}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<float>({1.0, 2.0, 2.0})})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeNullableFlatVector<float>({static_cast<float>(5.0 / 3.0)})}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<float>(
          {1.0, std::optional<float>(), std::optional<float>()})})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<float>({1.0})}));
}

// Flink's decimal avg produces DECIMAL(38, max(6, s)): the sum stays at the
// input scale and the final division rescales by 10^k, k = max(6, s) - s,
// with HALF_UP rounding (ties away from zero), matching Flink's decimal
// division.
TEST_F(AverageAggregationTest, avgShortDecimal) {
  // Exact quotient: (1.00 + 2.00) / 2 = 1.500000.
  assertSingleAggregation(
      {makeRowVector(
          {makeNullableFlatVector<int64_t>({100, 200}, DECIMAL(10, 2))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{1500000}, DECIMAL(38, 6))}));

  // 3.02 / 3 = 1.006666... rounds up to 1.006667.
  assertSingleAggregation(
      {makeRowVector(
          {makeNullableFlatVector<int64_t>({100, 101, 101}, DECIMAL(10, 2))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{1006667}, DECIMAL(38, 6))}));

  // 3.01 / 3 = 1.003333... rounds down to 1.003333.
  assertSingleAggregation(
      {makeRowVector(
          {makeNullableFlatVector<int64_t>({100, 100, 101}, DECIMAL(10, 2))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{1003333}, DECIMAL(38, 6))}));

  // -3.02 / 3 = -1.006666... rounds away from zero to -1.006667.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>(
          {-100, -101, -101}, DECIMAL(10, 2))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{-1006667}, DECIMAL(38, 6))}));

  // Input scale 8 keeps the result at scale 8 (max(6, 8)); the quotient
  // 2.00000003 / 2 = 1.000000015 is an exact HALF_UP tie and rounds away
  // from zero to 1.00000002.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>(
          {100000001, 100000002}, DECIMAL(10, 8))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{100000002}, DECIMAL(38, 8))}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>(
          {-100000001, -100000002}, DECIMAL(10, 8))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{-100000002}, DECIMAL(38, 8))}));

  // Nulls are skipped; an all-null input produces null.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>(
          {100, std::optional<int64_t>(), 200}, DECIMAL(10, 2))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{1500000}, DECIMAL(38, 6))}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int64_t>(
          {std::optional<int64_t>()}, DECIMAL(10, 2))})},
      {},
      {"avg(c0)"},
      makeRowVector({makeNullableFlatVector<int128_t>(
          {std::optional<int128_t>()}, DECIMAL(38, 6))}));
}

// Long decimal inputs take the int128 accumulator with the same rescaling
// rules as short decimals.
TEST_F(AverageAggregationTest, avgLongDecimal) {
  // (1000000.0001 + 1000000.0003) / 2 = 1000000.000200.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int128_t>(
          {HugeInt::build(0, 10000000001), HugeInt::build(0, 10000000003)},
          DECIMAL(23, 4))})},
      {},
      {"avg(c0)"},
      makeRowVector({makeFlatVector(
          std::vector<int128_t>{1000000000200}, DECIMAL(38, 6))}));

  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int128_t>(
          {std::optional<int128_t>(), HugeInt::build(0, 20000000000)},
          DECIMAL(23, 4))})},
      {},
      {"avg(c0)"},
      makeRowVector({makeFlatVector(
          std::vector<int128_t>{2000000000000}, DECIMAL(38, 6))}));
}

// The sum can exceed the int128 range; the overflow counter carries the
// 2^127 digits and the average stays exact, including a HALF_UP remainder
// in the overflow branch.
TEST_F(AverageAggregationTest, avgDecimalSumOverflow) {
  // 2 * 9e37 exceeds int128 max; the average is 9e37 at scale 8.
  const int128_t k90E37 =
      HugeInt::build(4878909776184769953LL, 618659596260605952ULL);
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int128_t>(
          {k90E37, k90E37}, DECIMAL(38, 8))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{k90E37}, DECIMAL(38, 8))}));

  // (2 * 9e37 + 1) / 2 = 9e37 + 0.5 rounds up to 9e37 + 1.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int128_t>(
          {k90E37, k90E37 + 1}, DECIMAL(38, 8))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{k90E37 + 1}, DECIMAL(38, 8))}));

  // Underflow mirrors the overflow direction.
  assertSingleAggregation(
      {makeRowVector({makeNullableFlatVector<int128_t>(
          {-k90E37, -k90E37}, DECIMAL(38, 8))})},
      {},
      {"avg(c0)"},
      makeRowVector(
          {makeFlatVector(std::vector<int128_t>{-k90E37}, DECIMAL(38, 8))}));
}

// The streaming pattern GroupAggregator uses: accumulate raw input, extract
// the accumulator, feed it to a fresh aggregate, add more raw input, and
// produce the final value. The integral accumulator is a BIGINT column, so
// this also covers the bit-preserving uint64_t-to-int64_t extraction.
TEST_F(AverageAggregationTest, avgStreamingAccumulatorRoundTrip) {
  const auto kMax = std::numeric_limits<int64_t>::max();
  // (kMax + kMax) wraps to -2; adding 5 gives 3 over 3 rows, average 1.
  auto result = testStreaming(
      "avg",
      true,
      {makeNullableFlatVector<int64_t>({kMax, kMax})},
      {makeNullableFlatVector<int64_t>({5})});
  velox::test::assertEqualVectors(makeNullableFlatVector<int64_t>({1}), result);

  // Decimal accumulator round trip: (1.00 + 2.00 + 3.01) / 3 = 2.003333...
  // rounds up to 2.003333.
  result = testStreaming(
      "avg",
      true,
      {makeNullableFlatVector<int64_t>({100, 200}, DECIMAL(10, 2))},
      {makeNullableFlatVector<int64_t>({301}, DECIMAL(10, 2))});
  velox::test::assertEqualVectors(
      makeFlatVector(std::vector<int128_t>{2003333}, DECIMAL(38, 6)), result);
}

// Partial-to-final aggregation is not implemented yet and the factory must
// fail loudly when asked for a non-raw-input step instead of silently mixing
// incompatible accumulator semantics. HashAggregation plans never request
// such steps (it maps every node to kPartial/kSingle at AggregateInfo.cpp and
// feeds final nodes through the accumulator merge path), so the guard is
// verified at the factory level the way direct callers (e.g. the companion
// extract adapter) would hit it.
TEST_F(AverageAggregationTest, avgNonRawInputStepsUnsupported) {
  const core::QueryConfig config({});
  for (auto step :
       {core::AggregationNode::Step::kFinal,
        core::AggregationNode::Step::kIntermediate}) {
    VELOX_ASSERT_THROW(
        exec::Aggregate::create("avg", step, {BIGINT()}, BIGINT(), config),
        "avg does not support partial-to-final aggregation steps yet");
  }
}

} // namespace
} // namespace facebook::velox::aggregate::flinksql::test
