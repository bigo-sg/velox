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
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

class FlinkJsonTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    functions::test::FunctionBaseTest::SetUpTestCase();
    functions::flinksql::registerJsonFunctions();
  }
};

// -------------------- json_value --------------------
// Signature: json_value(json, path, onEmpty, onEmptyDefault, onError,
// onErrorDefault) handler encoding (flinksql/JsonFunctions.h): 0=NULL, 1=ERROR,
// 2=DEFAULT

TEST_F(FlinkJsonTest, jsonValueScalar) {
  auto result = evaluate(
      "json_value(c0, c1, c2, c3, c4, c5)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"{\"a\":1}"}),
          makeNullableFlatVector<std::string>({"$.a"}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
      }));
  test::assertEqualVectors(makeNullableFlatVector<std::string>({"1"}), result);
}

TEST_F(FlinkJsonTest, jsonValueString) {
  auto result = evaluate(
      "json_value(c0, c1, c2, c3, c4, c5)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"{\"name\":\"alice\"}"}),
          makeNullableFlatVector<std::string>({"$.name"}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"alice"}), result);
}

TEST_F(FlinkJsonTest, jsonValueMissingNull) {
  auto result = evaluate(
      "json_value(c0, c1, c2, c3, c4, c5)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"{}"}),
          makeNullableFlatVector<std::string>({"$.missing"}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({std::nullopt}), result);
}

TEST_F(FlinkJsonTest, jsonValueMissingDefault) {
  auto result = evaluate(
      "json_value(c0, c1, c2, c3, c4, c5)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"{}"}),
          makeNullableFlatVector<std::string>({"strict $.missing"}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
          makeNullableFlatVector<int32_t>({2}),
          makeNullableFlatVector<std::string>({"d"}),
      }));
  test::assertEqualVectors(makeNullableFlatVector<std::string>({"d"}), result);
}

TEST_F(FlinkJsonTest, jsonValueLaxEmptyDefault) {
  auto result = evaluate(
      "json_value(c0, c1, c2, c3, c4, c5)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"{}"}),
          makeNullableFlatVector<std::string>({"lax $.missing"}),
          makeNullableFlatVector<int32_t>({2}),
          makeNullableFlatVector<std::string>({"e"}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
      }));
  test::assertEqualVectors(makeNullableFlatVector<std::string>({"e"}), result);
}

TEST_F(FlinkJsonTest, jsonValueLaxObjectEmpty) {
  auto result = evaluate(
      "json_value(c0, c1, c2, c3, c4, c5)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"{\"obj\":{}}"}),
          makeNullableFlatVector<std::string>({"lax $.obj"}),
          makeNullableFlatVector<int32_t>({2}),
          makeNullableFlatVector<std::string>({"E"}),
          makeNullableFlatVector<int32_t>({0}),
          makeNullableFlatVector<std::string>({""}),
      }));
  test::assertEqualVectors(makeNullableFlatVector<std::string>({"E"}), result);
}

TEST_F(FlinkJsonTest, jsonValueErrorOnError) {
  EXPECT_THROW(
      evaluate(
          "json_value(c0, c1, c2, c3, c4, c5)",
          makeRowVector({
              makeNullableFlatVector<std::string>({"{}"}),
              makeNullableFlatVector<std::string>({"strict $.missing"}),
              makeNullableFlatVector<int32_t>({0}),
              makeNullableFlatVector<std::string>({""}),
              makeNullableFlatVector<int32_t>({1}),
              makeNullableFlatVector<std::string>({""}),
          })),
      facebook::velox::VeloxUserError);
}

TEST_F(FlinkJsonTest, jsonValueBatch) {
  auto result = evaluate(
      "json_value(c0, c1, c2, c3, c4, c5)",
      makeRowVector({
          makeNullableFlatVector<std::string>(
              {"{\"a\":1}", "{}", std::nullopt, "{\"a\":2}"}),
          makeNullableFlatVector<std::string>({"$.a", "$.a", "$.a", "$.a"}),
          makeNullableFlatVector<int32_t>({0, 0, 0, 0}),
          makeNullableFlatVector<std::string>({"", "", "", ""}),
          makeNullableFlatVector<int32_t>({0, 0, 0, 0}),
          makeNullableFlatVector<std::string>({"", "", "", ""}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"1", std::nullopt, std::nullopt, "2"}),
      result);
}

// -------------------- RETURNING (non-VARCHAR) --------------------

TEST_F(FlinkJsonTest, jsonValueReturningInt) {
  auto result = evaluateOnce<int32_t>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"n\":42}"),
      std::optional<std::string>("$.n"),
      std::optional<int32_t>(0),
      std::optional<int32_t>(0),
      std::optional<int32_t>(0),
      std::optional<int32_t>(0));
  ASSERT_EQ(std::optional<int32_t>(42), result);
}

TEST_F(FlinkJsonTest, jsonValueReturningDouble) {
  auto result = evaluateOnce<double>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"n\":1.5}"),
      std::optional<std::string>("$.n"),
      std::optional<int32_t>(0),
      std::optional<double>(0.0),
      std::optional<int32_t>(0),
      std::optional<double>(0.0));
  ASSERT_EQ(std::optional<double>(1.5), result);
}

TEST_F(FlinkJsonTest, jsonValueReturningBoolean) {
  auto result = evaluateOnce<bool>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"b\":true}"),
      std::optional<std::string>("$.b"),
      std::optional<int32_t>(0),
      std::optional<bool>(false),
      std::optional<int32_t>(0),
      std::optional<bool>(false));
  ASSERT_EQ(std::optional<bool>(true), result);
}

TEST_F(FlinkJsonTest, jsonValueIntDefaultOnEmpty) {
  auto result = evaluateOnce<int32_t>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"x\":1}"),
      std::optional<std::string>("lax $.missing"),
      std::optional<int32_t>(2),
      std::optional<int32_t>(42),
      std::optional<int32_t>(0),
      std::optional<int32_t>(0));
  ASSERT_EQ(std::optional<int32_t>(42), result);
}

TEST_F(FlinkJsonTest, jsonValueReturningTypeMismatch) {
  // A RETURNING-type mismatch fails the query in Flink (an uncaught
  // ClassCastException outside the extraction try/catch), regardless of the
  // ON ERROR clause, so fail here too.
  EXPECT_THROW(
      evaluateOnce<int32_t>(
          "json_value(c0, c1, c2, c3, c4, c5)",
          std::optional<std::string>("{\"s\":\"x\"}"),
          std::optional<std::string>("$.s"),
          std::optional<int32_t>(0),
          std::optional<int32_t>(0),
          std::optional<int32_t>(0),
          std::optional<int32_t>(0)),
      facebook::velox::VeloxUserError);
  // Float scalar with RETURNING INT is likewise a mismatch, regardless of the
  // ON ERROR DEFAULT NULL clause.
  EXPECT_THROW(
      evaluateOnce<int32_t>(
          "json_value(c0, c1, c2, c3, c4, c5)",
          std::optional<std::string>("{\"n\":1.5}"),
          std::optional<std::string>("$.n"),
          std::optional<int32_t>(0),
          std::optional<int32_t>(std::nullopt),
          std::optional<int32_t>(2),
          std::optional<int32_t>(std::nullopt)),
      facebook::velox::VeloxUserError);
}

TEST_F(FlinkJsonTest, jsonValueBooleanToString) {
  auto result = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"b\":true}"),
      std::optional<std::string>("$.b"),
      std::optional<int32_t>(0),
      std::optional<std::string>(""),
      std::optional<int32_t>(0),
      std::optional<std::string>(""));
  ASSERT_EQ(std::optional<std::string>("true"), result);
}

TEST_F(FlinkJsonTest, jsonValueMalformedJsonStrictDefault) {
  // Malformed JSON in strict mode is routed to ON ERROR, not ON EMPTY.
  auto result = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("not-json"),
      std::optional<std::string>("$.a"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("error"), result);
}

TEST_F(FlinkJsonTest, jsonValueMalformedJsonLaxEmpty) {
  // Malformed JSON in lax mode routes to ON EMPTY (aligned with Flink).
  auto result = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("not-json"),
      std::optional<std::string>("lax $.a"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("empty"), result);
}

TEST_F(FlinkJsonTest, jsonValueModePrefixCaseInsensitive) {
  // 'LAX' (case-insensitive) is recognized as lax mode.
  auto result = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"a\":1}"),
      std::optional<std::string>("LAX $.missing"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("empty"), result);
}

TEST_F(FlinkJsonTest, jsonValueNumberNormalization) {
  // Non-canonical number spellings are normalized the way Flink's Jackson does.
  auto evalOne = [&](const char* json) {
    return evaluateOnce<std::string>(
        "json_value(c0, c1, c2, c3, c4, c5)",
        std::optional<std::string>(json),
        std::optional<std::string>("$.n"),
        std::optional<int32_t>(0),
        std::optional<std::string>(""),
        std::optional<int32_t>(0),
        std::optional<std::string>(""));
  };
  ASSERT_EQ(std::optional<std::string>("1.5"), evalOne("{\"n\":1.50}"));
  ASSERT_EQ(std::optional<std::string>("1000.0"), evalOne("{\"n\":1e3}"));
  ASSERT_EQ(std::optional<std::string>("42"), evalOne("{\"n\":42}"));
  ASSERT_EQ(
      std::optional<std::string>("123456789012345678901234567890"),
      evalOne("{\"n\":123456789012345678901234567890}"));
  // [1e5, 1e7) renders in fixed notation, not scientific.
  ASSERT_EQ(
      std::optional<std::string>("100000.0"), evalOne("{\"n\":100000.0}"));
  ASSERT_EQ(std::optional<std::string>("1000000.0"), evalOne("{\"n\":1e6}"));
  ASSERT_EQ(std::optional<std::string>("9000000.0"), evalOne("{\"n\":9e6}"));
}

TEST_F(FlinkJsonTest, jsonValueMultipleResults) {
  // A path resolving to more than one scalar is an empty sequence under lax
  // (ON EMPTY) and an error under strict (ON ERROR).
  const std::string json = "{\"a\":[1,2]}";
  auto laxResult = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>(json),
      std::optional<std::string>("lax $.a[*]"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("empty"), laxResult);

  auto strictResult = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>(json),
      std::optional<std::string>("strict $.a[*]"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("error"), strictResult);
}

TEST_F(FlinkJsonTest, jsonValueLeadingWhitespace) {
  // Leading whitespace before the (optional) mode prefix must be ignored.
  auto result = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"a\":1}"),
      std::optional<std::string>("   lax $.missing"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("empty"), result);
}

TEST_F(FlinkJsonTest, jsonValueStrictObject) {
  // An object/array match is an empty sequence: under strict it is an error.
  auto result = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"obj\":{}}"),
      std::optional<std::string>("strict $.obj"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("error"), result);
}

TEST_F(FlinkJsonTest, jsonValueLaxArray) {
  // An object/array match is an empty sequence: under lax it is ON EMPTY.
  auto result = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"a\":[1,2]}"),
      std::optional<std::string>("lax $.a"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("empty"), result);
}

TEST_F(FlinkJsonTest, jsonValueStrictArray) {
  // An object/array match is an empty sequence: under strict it is ON ERROR.
  auto result = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"a\":[1,2]}"),
      std::optional<std::string>("strict $.a"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("error"), result);
}

TEST_F(FlinkJsonTest, jsonValueInvalidPath) {
  // A malformed path cannot be tokenized and must raise, not silently empty.
  EXPECT_THROW(
      evaluateOnce<std::string>(
          "json_value(c0, c1, c2, c3, c4, c5)",
          std::optional<std::string>("{\"a\":1}"),
          std::optional<std::string>("$.store.book["),
          std::optional<int32_t>(2),
          std::optional<std::string>("empty"),
          std::optional<int32_t>(2),
          std::optional<std::string>("error")),
      facebook::velox::VeloxUserError);
}

TEST_F(FlinkJsonTest, jsonValueJsonNull) {
  // JSON null is not a scalar: lax routes to ON EMPTY, strict to ON ERROR.
  auto laxResult = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"x\":null}"),
      std::optional<std::string>("lax $.x"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("empty"), laxResult);

  auto strictResult = evaluateOnce<std::string>(
      "json_value(c0, c1, c2, c3, c4, c5)",
      std::optional<std::string>("{\"x\":null}"),
      std::optional<std::string>("strict $.x"),
      std::optional<int32_t>(2),
      std::optional<std::string>("empty"),
      std::optional<int32_t>(2),
      std::optional<std::string>("error"));
  ASSERT_EQ(std::optional<std::string>("error"), strictResult);
}

TEST_F(FlinkJsonTest, jsonValueUnknownHandler) {
  // An unknown handler encoding is an ABI error and must raise, not DEFAULT.
  EXPECT_THROW(
      evaluateOnce<std::string>(
          "json_value(c0, c1, c2, c3, c4, c5)",
          std::optional<std::string>("{}"),
          std::optional<std::string>("strict $.missing"),
          std::optional<int32_t>(2),
          std::optional<std::string>("empty"),
          std::optional<int32_t>(3),
          std::optional<std::string>("error")),
      facebook::velox::VeloxUserError);
}
