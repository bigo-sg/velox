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
#include "velox/functions/flinksql/tests/FlinkFunctionBaseTest.h"

#include <limits>

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::functions::flinksql::test {

// Expected values are pinned from Flink 1.19.2 behavior (epoch domain
// 0000-01-01 .. 9999-12-31 23:59:59.999, NULL out of range, TableException
// on any precision other than 0/3).
class ToTimestampLtzTest : public FlinkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<Timestamp> ltzOnce(
      std::optional<T> v,
      std::optional<int32_t> p) {
    return evaluateOnce<Timestamp>("to_timestamp_ltz(c0, c1)", v, p);
  }
};

// J. Precision 0/3 domain matrix (the card's example table).
TEST_F(ToTimestampLtzTest, PrecisionDomain) {
  EXPECT_EQ(Timestamp(0, 0), ltzOnce<int64_t>(0, 3));
  EXPECT_EQ(Timestamp(-1, 999000000), ltzOnce<int64_t>(-1, 3));
  EXPECT_EQ(Timestamp(1760000, 0), ltzOnce<int64_t>(1760000000, 3));
  EXPECT_EQ(Timestamp(1760000000, 0), ltzOnce<int64_t>(1760000000, 0));
  EXPECT_EQ(Timestamp(1666605683, 0), ltzOnce<int64_t>(1666605683, 0));
  EXPECT_EQ(Timestamp(1709165040, 0), ltzOnce<int64_t>(1709165040, 0));
  EXPECT_EQ(
      Timestamp(253402300799, 999000000), ltzOnce<int64_t>(253402300799999, 3));
  EXPECT_EQ(std::nullopt, ltzOnce<int64_t>(253402300800000, 3));
  EXPECT_EQ(Timestamp(-62167219200, 0), ltzOnce<int64_t>(-62167219200000, 3));
  EXPECT_EQ(std::nullopt, ltzOnce<int64_t>(-62167219200001, 3));
  EXPECT_EQ(Timestamp(-62167219200, 0), ltzOnce<int64_t>(-62167219200, 0));
  EXPECT_EQ(std::nullopt, ltzOnce<int64_t>(-62167219201, 0));
  EXPECT_EQ(Timestamp(253402300799, 0), ltzOnce<int64_t>(253402300799, 0));
  EXPECT_EQ(std::nullopt, ltzOnce<int64_t>(253402300800, 0));
}

// J13-J18: the double family with Java (long) cast semantics.
TEST_F(ToTimestampLtzTest, DoubleSemantics) {
  EXPECT_EQ(Timestamp(123456, 789000000), ltzOnce<double>(123456789.123, 3));
  EXPECT_EQ(Timestamp(0, 0), ltzOnce<double>(-0.9, 3));
  EXPECT_EQ(Timestamp(1666605683, 900000000), ltzOnce<double>(1666605683.9, 0));
  EXPECT_EQ(std::nullopt, ltzOnce<double>(1e19, 3));
  EXPECT_EQ(
      Timestamp(0, 0),
      ltzOnce<double>(std::numeric_limits<double>::quiet_NaN(), 3));
  // p=0 checks the second domain in double arithmetic first: every NaN
  // comparison is false -> NULL (the J17/J18 asymmetry is the tested
  // semantics).
  EXPECT_EQ(
      std::nullopt,
      ltzOnce<double>(std::numeric_limits<double>::quiet_NaN(), 0));
  EXPECT_EQ(std::nullopt, ltzOnce<double>(-1e19, 3));
}

// K. Illegal precision -> VELOX_USER_FAIL (Flink TableException; a job
// failure on both stacks). Column values and a constant INT both throw. (A
// bare literal 6 is typed BIGINT by the expression parser and fails at
// signature matching instead — the L1 known limitation.)
TEST_F(ToTimestampLtzTest, IllegalPrecisionFails) {
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<int64_t>({1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
      makeNullableFlatVector<int32_t>({1, 2, 4, 5, 6, 7, 8, 9, -1, 10})});
  VELOX_ASSERT_THROW(evaluate("to_timestamp_ltz(c0, c1)", data), "unsupported");
  // Constant INT precision column: the compile-once constant path throws too.
  auto constData = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<int64_t>({1}),
      BaseVector::wrapInConstant(1, 0, makeNullableFlatVector<int32_t>({6}))});
  VELOX_ASSERT_THROW(
      evaluate("to_timestamp_ltz(c0, c1)", constData), "unsupported");
}

// L. NULL propagation: the first-argument NULL short-circuits before the
// precision check ((NULL, 6) is a NULL row, not a failure).
TEST_F(ToTimestampLtzTest, NullPropagation) {
  EXPECT_EQ(std::nullopt, ltzOnce<int64_t>(std::nullopt, 3));
  EXPECT_EQ(std::nullopt, ltzOnce<int64_t>(std::nullopt, 6));
  EXPECT_EQ(std::nullopt, ltzOnce<double>(std::nullopt, 3));
}

// M. Integral type family: each registered signature evaluates. Negative
// millisecond values floor into a negative second + positive nanos (J2 form).
TEST_F(ToTimestampLtzTest, IntegralFamily) {
  EXPECT_EQ(Timestamp(127, 0), ltzOnce<int8_t>(127, 0));
  EXPECT_EQ(Timestamp(-1, 873000000), ltzOnce<int8_t>(-127, 3));
  EXPECT_EQ(std::nullopt, ltzOnce<int8_t>(std::nullopt, 3));
  EXPECT_EQ(Timestamp(32767, 0), ltzOnce<int16_t>(32767, 0));
  EXPECT_EQ(Timestamp(-33, 233000000), ltzOnce<int16_t>(-32767, 3));
  EXPECT_EQ(Timestamp(1666605683, 0), ltzOnce<int32_t>(1666605683, 0));
  EXPECT_EQ(Timestamp(1760000, 0), ltzOnce<int32_t>(1760000000, 3));
  EXPECT_EQ(Timestamp(1760000000, 0), ltzOnce<int64_t>(1760000000, 0));
  EXPECT_EQ(Timestamp(-62167219200, 0), ltzOnce<int64_t>(-62167219200, 0));
  EXPECT_EQ(std::nullopt, ltzOnce<int64_t>(-62167219201, 0));
}

// N. Decimal family: unscaled value + scale from the input type; the
// fraction truncates toward zero (Flink setScale(0, DOWN)).
TEST_F(ToTimestampLtzTest, DecimalFamily) {
  // DECIMAL(11,1): 1760000000.5 -> unscaled 17600000005. p=3 reads it as
  // milliseconds (J3 form); p=0 as seconds — the fromMillis forms.
  {
    auto data = makeRowVector(std::vector<VectorPtr>{
        makeFlatVector<int64_t>(
            {17600000005LL, 17600000005LL, -55}, DECIMAL(11, 1)),
        makeNullableFlatVector<int32_t>({3, 0, 0})});
    auto result = evaluate("to_timestamp_ltz(c0, c1)", data);
    ASSERT_EQ(3, result->size());
    auto flat = result->asFlatVector<Timestamp>();
    // N1: 1760000000.5 read as milliseconds, fraction dropped.
    EXPECT_EQ(Timestamp(1760000, 0), flat->valueAt(0));
    // N2: the same value read as seconds.
    EXPECT_EQ(Timestamp(1760000000, 0), flat->valueAt(1));
    // N4: -5.5 -> -5 seconds (toward zero).
    EXPECT_EQ(Timestamp(-5, 0), flat->valueAt(2));
  }
  // LONG_DECIMAL(20,2): an out-of-range universe-scale value is NULL (the
  // int128 range check; Flink's longValue() would wrap).
  {
    const int128_t hugeUnscaled =
        static_cast<int128_t>(9999999999LL) * 10000000000LL + 9999999999LL;
    auto data = makeRowVector(std::vector<VectorPtr>{
        makeFlatVector<int128_t>(
            {hugeUnscaled, 25340230079999LL}, DECIMAL(20, 2)),
        makeNullableFlatVector<int32_t>({3, 0})});
    auto result = evaluate("to_timestamp_ltz(c0, c1)", data);
    ASSERT_EQ(2, result->size());
    // N3: 999999999999999999.99 -> out of range -> NULL.
    EXPECT_TRUE(result->isNullAt(0));
    auto flat = result->asFlatVector<Timestamp>();
    // 253402300799.99 -> 253402300799 seconds (fraction dropped), top of range.
    EXPECT_EQ(Timestamp(253402300799, 0), flat->valueAt(1));
  }
}

} // namespace facebook::velox::functions::flinksql::test
