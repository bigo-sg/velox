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

class FlinkLikeTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    functions::test::FunctionBaseTest::SetUpTestCase();
    functions::flinksql::registerStringFunctions();
  }
};

// -------------------- like --------------------

TEST_F(FlinkLikeTest, likeEmptyPattern) {
  // A constant empty pattern matches every input (vacuous truth): Flink's
  // LikeCallGen routes a literal '' into SqlLikeChainChecker, whose
  // zero-checker chain matches everything, unlike the SQL-standard '^$'
  // (empty-string-only).
  auto result = evaluate(
      "like(c0, '')",
      makeRowVector({makeNullableFlatVector<std::string>({"abc", "", "a\nb"})}));
  test::assertEqualVectors(
      makeNullableFlatVector<bool>({true, true, true}), result);
}

TEST_F(FlinkLikeTest, likeBatch) {
  auto result = evaluate(
      "like(c0, c1)",
      makeRowVector({
          makeNullableFlatVector<std::string>(
              {"abc", "abc", std::nullopt, "你好", ""}),
          makeNullableFlatVector<std::string>({"a%", "_bc", "a%", "__", ""}),
      }));
  test::assertEqualVectors(
      makeNullableFlatVector<bool>({true, true, std::nullopt, true, true}),
      result);
}

TEST_F(FlinkLikeTest, likeLineTerminator) {
  auto underscore = makeRowVector({
      makeNullableFlatVector<std::string>(
          {"\n",
           "\r",
           "\xC2\x85",
           "\xE2\x80\xA8",
           "\xE2\x80\xA9",
           "a\nb",
           "aXb"}),
      makeNullableFlatVector<std::string>(
          {"_", "_", "_", "_", "_", "a_b", "a_b"}),
  });
  auto result1 = evaluate("like(c0, c1)", underscore);
  test::assertEqualVectors(
      makeNullableFlatVector<bool>(
          {false, false, false, false, false, false, true}),
      result1);

  auto percent = makeRowVector({
      makeNullableFlatVector<std::string>({"\n", "a\nb"}),
      makeNullableFlatVector<std::string>({"%", "a%b"}),
  });
  auto result2 = evaluate("like(c0, c1)", percent);
  test::assertEqualVectors(makeNullableFlatVector<bool>({true, true}), result2);
}