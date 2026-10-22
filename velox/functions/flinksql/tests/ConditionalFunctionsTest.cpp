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
#include <optional>
#include <vector>

#include "velox/functions/flinksql/ConditionalFunctions.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::functions::flinksql::test {

using facebook::velox::test::assertEqualVectors;

class FlinkConditionalTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    flinksql::registerConditionalFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }


  void testIfNull(
      const VectorPtr& expected,
      const VectorPtr& a,
      const VectorPtr& b) {
    auto expr = std::make_shared<core::CallTypedExpr>(
        expected->type(),
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(a->type(), "c0"),
            std::make_shared<core::FieldAccessTypedExpr>(b->type(), "c1")},
        "ifnull");
    assertEqualVectors(expected, evaluate(expr, makeRowVector({a, b})));
  }
};

TEST_F(FlinkConditionalTest, ifNullBigint) {
  // a = {null, 1, null, 2}, b = {10, null, null, 20}
  // -> {10, 1, null, 2}: first non-null wins; both null -> null.
  auto a = makeNullableFlatVector<int64_t>(
      {std::optional<int64_t>(std::nullopt),
       1,
       std::optional<int64_t>(std::nullopt),
       2});
  auto b = makeNullableFlatVector<int64_t>(
      {10, std::optional<int64_t>(std::nullopt),
      std::optional<int64_t>(std::nullopt), 20});
  auto expected = makeNullableFlatVector<int64_t>(
      {10, 1, std::optional<int64_t>(std::nullopt), 2});
  testIfNull(expected, a, b);
}

TEST_F(FlinkConditionalTest, ifNullVarchar) {
  auto a = makeNullableFlatVector<std::string>(
      {std::optional<std::string>(std::nullopt), "first",
      std::optional<std::string>(std::nullopt)});
  auto b = makeNullableFlatVector<std::string>(
      {"fallback", std::optional<std::string>(std::nullopt),
      std::optional<std::string>(std::nullopt)});
  auto expected = makeNullableFlatVector<std::string>(
      {"fallback", "first", std::optional<std::string>(std::nullopt)});
  testIfNull(expected, a, b);
}

TEST_F(FlinkConditionalTest, ifNullDouble) {
  auto a = makeNullableFlatVector<double>(
      {std::optional<double>(std::nullopt), 1.5,
      std::optional<double>(std::nullopt)});
  auto b = makeNullableFlatVector<double>({0.25, 9.0,
      std::optional<double>(std::nullopt)});
  auto expected = makeNullableFlatVector<double>(
      {0.25, 1.5, std::optional<double>(std::nullopt)});
  testIfNull(expected, a, b);
}

TEST_F(FlinkConditionalTest, ifNullShortDecimal) {
  // 12.34, null | 56.78, 99.99 -> 12.34, 99.99 (scale 2).
  auto a = makeNullableFlatVector<int64_t>(
      {1234, std::optional<int64_t>(std::nullopt)}, DECIMAL(10, 2));
  auto b = makeNullableFlatVector<int64_t>({5678, 9999}, DECIMAL(10, 2));
  auto expected =
      makeNullableFlatVector<int64_t>({1234, 9999}, DECIMAL(10, 2));
  testIfNull(expected, a, b);
}

TEST_F(FlinkConditionalTest, ifNullLongDecimal) {
  auto a = makeNullableFlatVector<int128_t>(
      {HugeInt::parse("12345678901234567899"),
       std::optional<int128_t>(std::nullopt)},
      DECIMAL(20, 2));
  auto b = makeNullableFlatVector<int128_t>(
      {HugeInt::parse("1"), HugeInt::parse("-1")}, DECIMAL(20, 2));
  auto expected = makeNullableFlatVector<int128_t>(
      {HugeInt::parse("12345678901234567899"), HugeInt::parse("-1")},
      DECIMAL(20, 2));
  testIfNull(expected, a, b);
}

TEST_F(FlinkConditionalTest, ifNullBooleanAndDate) {
  auto boolA = makeNullableFlatVector<bool>(
      {std::optional<bool>(std::nullopt), true});
  auto boolB = makeNullableFlatVector<bool>({false,
      std::optional<bool>(std::nullopt)});
  auto boolExpected = makeNullableFlatVector<bool>({false, true});
  testIfNull(boolExpected, boolA, boolB);

  // Date values are day counts since epoch at the exec layer.
  auto dateA = makeNullableFlatVector<int32_t>(
      {std::optional<int32_t>(std::nullopt), 20250}, DATE());
  auto dateB = makeNullableFlatVector<int32_t>({19700, 20251}, DATE());
  auto dateExpected = makeNullableFlatVector<int32_t>({19700, 20250}, DATE());
  testIfNull(dateExpected, dateA, dateB);
}

} // namespace facebook::velox::functions::flinksql::test
