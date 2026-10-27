/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

class FlinkStringTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    functions::test::FunctionBaseTest::SetUpTestCase();
    functions::flinksql::registerStringFunctions();
  }

  VectorPtr eval(const std::string& expr, const VectorPtr& input) {
    return evaluate(expr, makeRowVector({input}));
  }
};

// from_base64

TEST_F(FlinkStringTest, fromBase64) {
  auto result = eval("from_base64(c0)", makeNullableFlatVector<std::string>(
      {"QTNGMTlDMjdCNEUw"}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"A3F19C27B4E0"}), result);
}

TEST_F(FlinkStringTest, fromBase64LongInput) {
  std::string raw(100, 'a');
  auto result = eval(
      "from_base64(c0)",
      makeNullableFlatVector<std::string>(
          {"YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYQ=="}));
  test::assertEqualVectors(makeNullableFlatVector<std::string>({raw}), result);
}

TEST_F(FlinkStringTest, fromBase64Batch) {
  auto result = evaluate(
      "from_base64(c0)",
      makeRowVector({makeNullableFlatVector<std::string>(
          {"dGVzdA==", "", std::nullopt, "QQ=="})}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"test", "", std::nullopt, "A"}),
      result);
}

// replace

TEST_F(FlinkStringTest, replaceBasic) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"hello"}),
          makeNullableFlatVector<std::string>({"l"}),
          makeNullableFlatVector<std::string>({"x"}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"hexxo"}), result);
}

TEST_F(FlinkStringTest, replaceEmptyOldStr) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abc"}),
          makeNullableFlatVector<std::string>({""}),
          makeNullableFlatVector<std::string>({"x"}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"xaxbxcx"}), result);
}

TEST_F(FlinkStringTest, replaceNullInput) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({std::nullopt}),
          makeNullableFlatVector<std::string>({"a"}),
          makeNullableFlatVector<std::string>({"b"}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({std::nullopt}), result);
}

TEST_F(FlinkStringTest, replaceEmptyInput) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({""}),
          makeNullableFlatVector<std::string>({"a"}),
          makeNullableFlatVector<std::string>({"b"}),
      }));
  test::assertEqualVectors(makeNullableFlatVector<std::string>({""}), result);
}

TEST_F(FlinkStringTest, replaceEmptyReplacement) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abc"}),
          makeNullableFlatVector<std::string>({"b"}),
          makeNullableFlatVector<std::string>({""}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"ac"}), result);
}

TEST_F(FlinkStringTest, replaceNoMatch) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abc"}),
          makeNullableFlatVector<std::string>({"d"}),
          makeNullableFlatVector<std::string>({"x"}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"abc"}), result);
}

TEST_F(FlinkStringTest, replaceLargeInputEmptyOld) {
  std::string input(1024 * 1024, 'a');
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({input}),
          makeNullableFlatVector<std::string>({""}),
          makeNullableFlatVector<std::string>({"xx"}),
      }));
  auto sv = result->as<SimpleVector<StringView>>()->valueAt(0);
  EXPECT_EQ((1024 * 1024 + 1) * 2 + 1024 * 1024, sv.size());
  EXPECT_EQ("xxaxx", std::string(sv).substr(0, 5));
}

TEST_F(FlinkStringTest, replaceOverlappingOld) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abbbbc"}),
          makeNullableFlatVector<std::string>({"bb"}),
          makeNullableFlatVector<std::string>({"d"}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"addc"}), result);
}

TEST_F(FlinkStringTest, replaceOverlappingOldOdd) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abbbbbc"}),
          makeNullableFlatVector<std::string>({"bb"}),
          makeNullableFlatVector<std::string>({"d"}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"addbc"}), result);
}

TEST_F(FlinkStringTest, replaceOverlappingDelete) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abbbbc"}),
          makeNullableFlatVector<std::string>({"bb"}),
          makeNullableFlatVector<std::string>({""}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"ac"}), result);
}

TEST_F(FlinkStringTest, replaceGrowingReplacement) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abbbbc"}),
          makeNullableFlatVector<std::string>({"b"}),
          makeNullableFlatVector<std::string>({"bb"}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"abbbbbbbbc"}), result);
}

TEST_F(FlinkStringTest, replaceBatch) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>(
              {"hello", "abc", std::nullopt, "aXbXc"}),
          makeNullableFlatVector<std::string>({"l", "", "b", "X"}),
          makeNullableFlatVector<std::string>({"x", "x", "y", std::nullopt}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"hexxo", "xaxbxcx", std::nullopt, std::nullopt}),
      result);
}

// right

TEST_F(FlinkStringTest, rightBasic) {
  auto result = evaluate(
      "right(c0, c1)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abc"}),
          makeNullableFlatVector<int32_t>({2}),
      }));
  test::assertEqualVectors(makeNullableFlatVector<std::string>({"bc"}), result);
}

TEST_F(FlinkStringTest, rightZeroAndNegative) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({"abc", "abc"}),
      makeNullableFlatVector<int32_t>({0, -5}),
  });
  auto result = evaluate("right(c0, c1)", data);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"", ""}), result);
}

TEST_F(FlinkStringTest, rightWholeString) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({"abc", "abc"}),
      makeNullableFlatVector<int32_t>({3, 5}),
  });
  auto result = evaluate("right(c0, c1)", data);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"abc", "abc"}), result);
}

TEST_F(FlinkStringTest, rightMultibyte) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({"abc你好", "abc你好"}),
      makeNullableFlatVector<int32_t>({2, 5}),
  });
  auto result = evaluate("right(c0, c1)", data);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"你好", "abc你好"}), result);
}

TEST_F(FlinkStringTest, rightNullAndEmpty) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({std::nullopt, "abc", ""}),
      makeNullableFlatVector<int32_t>({2, std::nullopt, 1}),
  });
  auto result = evaluate("right(c0, c1)", data);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({std::nullopt, std::nullopt, ""}),
      result);
}

TEST_F(FlinkStringTest, rightExtremeN) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({"abc", "abc"}),
      makeNullableFlatVector<int32_t>({2147483647, -2147483647 - 1}),
  });
  auto result = evaluate("right(c0, c1)", data);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"abc", ""}), result);
}

TEST_F(FlinkStringTest, rightBatch) {
  auto result = evaluate(
      "right(c0, c1)",
      makeRowVector({
          makeNullableFlatVector<std::string>(
              {"abc", "hello", std::nullopt, "abc你好"}),
          makeNullableFlatVector<int32_t>({2, 1, 2, 2}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"bc", "o", std::nullopt, "你好"}),
      result);
}