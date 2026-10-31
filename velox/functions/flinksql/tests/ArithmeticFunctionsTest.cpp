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
#include <optional>
#include <string>
#include <vector>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/flinksql/ArithmeticFunctions.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::functions::flinksql::test {

using facebook::velox::test::assertEqualVectors;

class FlinkArithmeticTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    flinksql::registerArithmeticFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }

  void testUnary(
      const std::string& functionName,
      const VectorPtr& expected,
      const VectorPtr& input) {
    auto inputExpr =
        std::make_shared<core::FieldAccessTypedExpr>(input->type(), "c0");
    std::vector<core::TypedExprPtr> args{inputExpr};
    auto expr = std::make_shared<core::CallTypedExpr>(
        expected->type(), args, functionName);
    assertEqualVectors(expected, evaluate(expr, makeRowVector({input})));
  }

  void testBinary(
      const std::string& functionName,
      const VectorPtr& expected,
      const VectorPtr& a,
      const VectorPtr& b) {
    auto aExpr = std::make_shared<core::FieldAccessTypedExpr>(a->type(), "c0");
    auto bExpr = std::make_shared<core::FieldAccessTypedExpr>(b->type(), "c1");
    std::vector<core::TypedExprPtr> args{aExpr, bExpr};
    auto expr = std::make_shared<core::CallTypedExpr>(
        expected->type(), args, functionName);
    assertEqualVectors(expected, evaluate(expr, makeRowVector({a, b})));
  }
};

// The decimal-round regression anchors from the task design: Flink rounds
// through the decimal representation, so half-way and drift-prone values
// must land on the decimal answer, not the binary one.
TEST_F(FlinkArithmeticTest, roundDoubleHalfUp) {
  auto input = makeNullableFlatVector<double>(
      {28.05, 0.15, 99.99, 2.5, -2.5, 12.5, -12.5, 28.5, 123456.789, 1.0e-5});
  auto d = makeNullableFlatVector<int32_t>({1, 1, 1, 0, 0, 0, 0, 0, 2, 4});
  auto expected = makeNullableFlatVector<double>(
      {28.1, 0.2, 100.0, 3.0, -3.0, 13.0, -13.0, 29.0, 123456.79, 0.0});
  testBinary("round", expected, input, d);
}

// Decimal HALF_UP ties where the binary double sits below the decimal
// value: separators against scale-multiply and binary-domain rounding,
// which both give ...81 / ...20 / ...59 / 2.67 here.
TEST_F(FlinkArithmeticTest, roundDoubleLargeMagnitudeTie) {
  auto input = makeNullableFlatVector<double>(
      {647568.815, 581245.205, 284978.595, 2.675});
  auto d = makeNullableFlatVector<int32_t>({2, 2, 2, 2});
  auto expected =
      makeNullableFlatVector<double>({647568.82, 581245.21, 284978.6, 2.68});
  testBinary("round", expected, input, d);
}

// -251 at tens: the dropped fraction is 0.1 (< 0.5), so HALF_UP keeps
// the magnitude: -250 (matches BigDecimal setScale(-1, HALF_UP)).
TEST_F(FlinkArithmeticTest, roundDoubleNegativeScale) {
  auto input =
      makeNullableFlatVector<double>({25.0, -25.0, 249.0, -251.0, 28.05});
  auto d = makeNullableFlatVector<int32_t>({-1, -1, -1, -1, -2});
  auto expected =
      makeNullableFlatVector<double>({30.0, -30.0, 250.0, -250.0, 0.0});
  testBinary("round", expected, input, d);
}

TEST_F(FlinkArithmeticTest, roundDoubleOneArg) {
  auto input = makeNullableFlatVector<double>(
      {28.05, 2.5, -2.5, std::optional<double>(std::nullopt)});
  auto expected = makeNullableFlatVector<double>(
      {28.0, 3.0, -3.0, std::optional<double>(std::nullopt)});
  testUnary("round", expected, input);
}

// shift >= 0: more requested decimals than present, value unchanged.
TEST_F(FlinkArithmeticTest, roundDoubleNoRoundingNeeded) {
  auto input = makeNullableFlatVector<double>({28.05, -7.125, 1.0e300});
  auto d = makeNullableFlatVector<int32_t>({5, 10, 5});
  auto expected = makeNullableFlatVector<double>({28.05, -7.125, 1.0e300});
  testBinary("round", expected, input, d);
}

// Flink's sround(float, int) widens to double first, so 28.05f becomes
// 28.049999237060547 and rounds to 28.0f — matching Java exactly.
TEST_F(FlinkArithmeticTest, roundFloat) {
  auto input = makeNullableFlatVector<float>(
      {28.05f, 2.5f, -2.5f, 0.15f, std::optional<float>(std::nullopt)});
  auto d = makeNullableFlatVector<int32_t>({1, 0, 0, 1, 0});
  auto expected = makeNullableFlatVector<float>(
      {28.0f, 3.0f, -3.0f, 0.2f, std::optional<float>(std::nullopt)});
  testBinary("round", expected, input, d);
}

// -251 at tens: dropped fraction 0.1 < 0.5, HALF_UP keeps -250.
TEST_F(FlinkArithmeticTest, roundIntegerNegativeScale) {
  auto input = makeNullableFlatVector<int32_t>({25, -25, 249, -251, 5, -5});
  auto d = makeNullableFlatVector<int32_t>({-1, -1, -1, -1, -1, -1});
  auto expected =
      makeNullableFlatVector<int32_t>({30, -30, 250, -250, 10, -10});
  testBinary("round", expected, input, d);
}

TEST_F(FlinkArithmeticTest, roundIntegerNonNegativeScale) {
  auto input = makeNullableFlatVector<int64_t>(
      {7, -7, std::optional<int64_t>(std::nullopt)});
  auto d = makeNullableFlatVector<int32_t>({0, 3, 2});
  auto expected = makeNullableFlatVector<int64_t>(
      {7, -7, std::optional<int64_t>(std::nullopt)});
  testBinary("round", expected, input, d);
}

// Unscaled values at scale 2: 55.67, 28.55, 28.05, 45.00.
TEST_F(FlinkArithmeticTest, roundShortDecimal) {
  auto input = makeNullableFlatVector<int64_t>(
      {5567, 2855, 2805, 4500, std::optional<int64_t>(std::nullopt)},
      DECIMAL(10, 2));
  auto d = makeNullableFlatVector<int32_t>({1, 1, 1, 1, 1});
  // 55.70, 28.60, 28.10, 45.00 (scale stays 2; digits below d become 0).
  auto expected = makeNullableFlatVector<int64_t>(
      {5570, 2860, 2810, 4500, std::optional<int64_t>(std::nullopt)},
      DECIMAL(10, 2));
  testBinary("round", expected, input, d);
}

// 28.55 rounded at d = -1: 30.00 at scale 2.
TEST_F(FlinkArithmeticTest, roundShortDecimalNegativeScale) {
  auto input =
      makeNullableFlatVector<int64_t>({2855, -2855, 1234}, DECIMAL(10, 2));
  auto d = makeNullableFlatVector<int32_t>({-1, -1, -2});
  auto expected =
      makeNullableFlatVector<int64_t>({3000, -3000, 0}, DECIMAL(10, 2));
  testBinary("round", expected, input, d);
}

// 123456789012345678.99 at scale 2 (long decimal), round at d = 1.
TEST_F(FlinkArithmeticTest, roundLongDecimal) {
  auto input = makeNullableFlatVector<int128_t>(
      {HugeInt::parse("12345678901234567899"),
       HugeInt::parse("-12345678901234567899")},
      DECIMAL(20, 2));
  auto d = makeNullableFlatVector<int32_t>({1, 1});
  auto expected = makeNullableFlatVector<int128_t>(
      {HugeInt::parse("12345678901234567900"),
       HugeInt::parse("-12345678901234567900")},
      DECIMAL(20, 2));
  testBinary("round", expected, input, d);
}

TEST_F(FlinkArithmeticTest, floorCeilNumeric) {
  auto input = makeNullableFlatVector<double>(
      {55.67, -55.01, 45.00, 0.5, -0.5, std::optional<double>(std::nullopt)});
  auto floorExpected = makeNullableFlatVector<double>(
      {55.0, -56.0, 45.0, 0.0, -1.0, std::optional<double>(std::nullopt)});
  auto ceilExpected = makeNullableFlatVector<double>(
      {56.0, -55.0, 45.0, 1.0, -0.0, std::optional<double>(std::nullopt)});
  testUnary("floor", floorExpected, input);
  testUnary("ceil", ceilExpected, input);
}

TEST_F(FlinkArithmeticTest, floorCeilIntegralIdentity) {
  auto input = makeNullableFlatVector<int64_t>(
      {7, -7, std::optional<int64_t>(std::nullopt)});
  auto expected = makeNullableFlatVector<int64_t>(
      {7, -7, std::optional<int64_t>(std::nullopt)});
  testUnary("floor", expected, input);
  testUnary("ceil", expected, input);
}

// decimal(10, 2) -> decimal(10, 0): unscaled 5567 (55.67) floors to 55,
// ceils to 56; -5501 (-55.01) floors to -56, ceils to -55.
TEST_F(FlinkArithmeticTest, floorCeilShortDecimal) {
  auto input = makeNullableFlatVector<int64_t>(
      {5567, -5501, 4500, std::optional<int64_t>(std::nullopt)},
      DECIMAL(10, 2));
  auto floorExpected = makeNullableFlatVector<int64_t>(
      {55, -56, 45, std::optional<int64_t>(std::nullopt)}, DECIMAL(10, 0));
  auto ceilExpected = makeNullableFlatVector<int64_t>(
      {56, -55, 45, std::optional<int64_t>(std::nullopt)}, DECIMAL(10, 0));
  testUnary("floor", floorExpected, input);
  testUnary("ceil", ceilExpected, input);
  // CEILING is a Flink alias of CEIL.
  testUnary("ceiling", ceilExpected, input);
}

TEST_F(FlinkArithmeticTest, floorCeilLongDecimal) {
  auto input = makeNullableFlatVector<int128_t>(
      {HugeInt::parse("12345678901234567899"),
       HugeInt::parse("-12345678901234567899")},
      DECIMAL(20, 2));
  auto floorExpected = makeNullableFlatVector<int128_t>(
      {HugeInt::parse("123456789012345678"),
       HugeInt::parse("-123456789012345679")},
      DECIMAL(20, 0));
  auto ceilExpected = makeNullableFlatVector<int128_t>(
      {HugeInt::parse("123456789012345679"),
       HugeInt::parse("-123456789012345678")},
      DECIMAL(20, 0));
  testUnary("floor", floorExpected, input);
  testUnary("ceil", ceilExpected, input);
}

TEST_F(FlinkArithmeticTest, absNumeric) {
  auto input = makeNullableFlatVector<int32_t>(
      {-27, 27, 0, std::optional<int32_t>(std::nullopt)});
  auto expected = makeNullableFlatVector<int32_t>(
      {27, 27, 0, std::optional<int32_t>(std::nullopt)});
  testUnary("abs", expected, input);

  auto dbl = makeNullableFlatVector<double>({-2.5, 2.5, -0.0, 0.0});
  auto dblExpected = makeNullableFlatVector<double>({2.5, 2.5, 0.0, 0.0});
  testUnary("abs", dblExpected, dbl);
}

// -12.34 -> 12.34 at decimal(10, 2).
TEST_F(FlinkArithmeticTest, absDecimal) {
  auto input = makeNullableFlatVector<int64_t>(
      {-1234, 1234, std::optional<int64_t>(std::nullopt)}, DECIMAL(10, 2));
  auto expected = makeNullableFlatVector<int64_t>(
      {1234, 1234, std::optional<int64_t>(std::nullopt)}, DECIMAL(10, 2));
  testUnary("abs", expected, input);
}

// INT_MIN and the int64 most-negative value wrap to themselves under
// abs/negate (Math.abs semantics), and same-typed int64 keeps exact
// |x| >= 2^53 values where Flink's
// double-widening fallback path silently corrupts them (e.g.
// abs(-2^53 - 1) evaluates as (long)abs((double)x) in Flink); the
// most negative integer wraps to itself, mirroring Java Math.abs.
TEST_F(FlinkArithmeticTest, absNegateIntegerEdges) {
  auto intInput = makeNullableFlatVector<int32_t>(
      {std::numeric_limits<int32_t>::min(), -7, 7});
  auto intAbsExpected = makeNullableFlatVector<int32_t>(
      {std::numeric_limits<int32_t>::min(), 7, 7});
  testUnary("abs", intAbsExpected, intInput);
  auto intNegExpected = makeNullableFlatVector<int32_t>(
      {std::numeric_limits<int32_t>::min(), 7, -7});
  testUnary("negate", intNegExpected, intInput);

  auto input = makeNullableFlatVector<int64_t>(
      {-9007199254740993LL,
       9007199254740993LL,
       std::numeric_limits<int64_t>::min()});
  auto absExpected = makeNullableFlatVector<int64_t>(
      {9007199254740993LL,
       9007199254740993LL,
       std::numeric_limits<int64_t>::min()});
  testUnary("abs", absExpected, input);

  auto negExpected = makeNullableFlatVector<int64_t>(
      {9007199254740993LL,
       -9007199254740993LL,
       std::numeric_limits<int64_t>::min()});
  testUnary("negate", negExpected, input);
}

// Integral floor/ceil are identities in same-typed int64; Flink's
// double-widening path loses 2^53 + 1 entirely (rounds to 2^53).
TEST_F(FlinkArithmeticTest, floorCeilInt64Beyond2Pow53) {
  auto input = makeNullableFlatVector<int64_t>(
      {9007199254740993LL, -9007199254740993LL});
  auto expected = makeNullableFlatVector<int64_t>(
      {9007199254740993LL, -9007199254740993LL});
  testUnary("floor", expected, input);
  testUnary("ceil", expected, input);
}

// Flink's ROUND(double) evaluates through BigDecimal.valueOf, which
// throws NumberFormatException for NaN/Inf and fails the job; the
// velox-side ROUND fails loudly with a user error the same way.
TEST_F(FlinkArithmeticTest, roundDoubleNanInfFailsLikeFlink) {
  auto input = makeNullableFlatVector<double>(
      {std::numeric_limits<double>::quiet_NaN(),
       std::numeric_limits<double>::infinity()});
  auto expected = makeNullableFlatVector<double>({0.0, 0.0});
  VELOX_ASSERT_THROW(
      testUnary("round", expected, input), "ROUND is not defined");
}

TEST_F(FlinkArithmeticTest, negate) {
  auto input = makeNullableFlatVector<int64_t>(
      {3, -3, 0, std::optional<int64_t>(std::nullopt)});
  auto expected = makeNullableFlatVector<int64_t>(
      {-3, 3, 0, std::optional<int64_t>(std::nullopt)});
  testUnary("negate", expected, input);

  auto dec = makeNullableFlatVector<int64_t>({-1234, 1234}, DECIMAL(10, 2));
  auto decExpected =
      makeNullableFlatVector<int64_t>({1234, -1234}, DECIMAL(10, 2));
  testUnary("negate", decExpected, dec);
}

} // namespace facebook::velox::functions::flinksql::test
