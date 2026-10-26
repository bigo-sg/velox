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
// Signature: json_value(json, path, onEmpty, onEmptyDefault, onError, onErrorDefault)
// handler encoding (flinksql/JsonFunctions.h): 0=NULL, 1=ERROR, 2=DEFAULT

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
  test::assertEqualVectors(makeNullableFlatVector<std::string>({"alice"}), result);
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
      makeNullableFlatVector<std::string>({"1", std::nullopt, std::nullopt, "2"}),
      result);
}
