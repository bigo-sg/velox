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
#include "velox/experimental/stateful/udf/Register.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::udf::test {

class UDFTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    stateful::udf::registerFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(UDFTest, splitIndex) {
  const auto splitIndex = [&](const std::optional<std::string>& a,
                              const std::optional<std::string>& d,
                              const std::optional<int64_t>& i) {
    return evaluateOnce<std::string>("split_index(c0, c1, c2)", a, d, i);
  };
  EXPECT_EQ(splitIndex("a/b/c", "/", 1), "a");
  EXPECT_EQ(splitIndex("a/b/c", "/", 2), "b");
  const std::optional<std::string> res0 = splitIndex("a/b/c", "/", 0);
  const std::optional<std::string> res1 = splitIndex("a/b/c", "/", -1);
  const std::optional<std::string> res2 = splitIndex("a/b/c", "/", 4);
  EXPECT_EQ(res0.has_value(), false);
  EXPECT_EQ(res1.has_value(), false);
  EXPECT_EQ(res2.has_value(), false);
}

} // namespace facebook::velox::udf::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}
