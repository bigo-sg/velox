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

class FlinkHashTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    functions::test::FunctionBaseTest::SetUpTestCase();
    functions::flinksql::registerHashFunctions();
  }

  VectorPtr eval(const std::string& expr, const VectorPtr& input) {
    return evaluate(expr, makeRowVector({input}));
  }
};

// -------------------- md5 --------------------

TEST_F(FlinkHashTest, md5Basic) {
  auto result = eval("md5(c0)", makeNullableFlatVector<std::string>({"test"}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"098f6bcd4621d373cade4e832627b4f6"}),
      result);
}

TEST_F(FlinkHashTest, md5Empty) {
  auto result = eval("md5(c0)", makeNullableFlatVector<std::string>({""}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"d41d8cd98f00b204e9800998ecf8427e"}),
      result);
}

TEST_F(FlinkHashTest, md5Multibyte) {
  auto result = eval("md5(c0)", makeNullableFlatVector<std::string>({"你好"}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"7eca689f0d3389d9dea66ae112e5cfd7"}),
      result);
}

TEST_F(FlinkHashTest, md5Null) {
  auto result =
      eval("md5(c0)", makeNullableFlatVector<std::string>({std::nullopt}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({std::nullopt}), result);
}

TEST_F(FlinkHashTest, md5LongInput) {
  std::string input(10000, 'a');
  auto result = eval("md5(c0)", makeNullableFlatVector<std::string>({input}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"0d0c9c4db6953fee9e03f528cafd7d3e"}),
      result);
}

TEST_F(FlinkHashTest, md5Batch) {
  auto result = evaluate(
      "md5(c0)",
      makeRowVector({makeNullableFlatVector<std::string>(
          {"test", "", std::nullopt, "abc"})}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"098f6bcd4621d373cade4e832627b4f6",
           "d41d8cd98f00b204e9800998ecf8427e",
           std::nullopt,
           "900150983cd24fb0d6963f7d28e17f72"}),
      result);
}

// -------------------- sha1 --------------------

TEST_F(FlinkHashTest, sha1Basic) {
  auto result = eval("sha1(c0)", makeNullableFlatVector<std::string>({"test"}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"}),
      result);
}

TEST_F(FlinkHashTest, sha1Empty) {
  auto result = eval("sha1(c0)", makeNullableFlatVector<std::string>({""}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"da39a3ee5e6b4b0d3255bfef95601890afd80709"}),
      result);
}

TEST_F(FlinkHashTest, sha1Multibyte) {
  auto result = eval("sha1(c0)", makeNullableFlatVector<std::string>({"你好"}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"440ee0853ad1e99f962b63e459ef992d7c211722"}),
      result);
}

TEST_F(FlinkHashTest, sha1Null) {
  auto result =
      eval("sha1(c0)", makeNullableFlatVector<std::string>({std::nullopt}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({std::nullopt}), result);
}

TEST_F(FlinkHashTest, sha1LongInput) {
  std::string input(10000, 'a');
  auto result = eval("sha1(c0)", makeNullableFlatVector<std::string>({input}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"a080cbda64850abb7b7f67ee875ba068074ff6fe"}),
      result);
}

TEST_F(FlinkHashTest, sha1Batch) {
  auto result = evaluate(
      "sha1(c0)",
      makeRowVector({makeNullableFlatVector<std::string>(
          {"test", "", std::nullopt, "abc"})}));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
           "da39a3ee5e6b4b0d3255bfef95601890afd80709",
           std::nullopt,
           "a9993e364706816aba3e25717850c26c9cd0d89d"}),
      result);
}