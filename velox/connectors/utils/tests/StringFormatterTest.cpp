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
#include "velox/connectors/utils/tests/StringFormatterTest.h"
#include "velox/connectors/utils/StringFormatter.h"
#include "velox/type/Type.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::connector::test {

class StringFormatterTest : public StringFormatterTestBase {};

TEST_F(StringFormatterTest, testNormalize) {
  std::string s = "1, 2, 3";
  std::string s0 = "+I[1, 2, 3]";
  std::string s1 = "+I[1, +I[2, 3]]";
  std::string s2 = "[2, 3, 4]";
  std::string s3 = "{2=3, 3=4}";
  std::string s4 = "[2, [3, 4]]";
  StringFormatter::normalize(s, TypeKind::VARCHAR);
  StringFormatter::normalize(s1, TypeKind::ROW);
  StringFormatter::normalize(s0, TypeKind::ROW);
  StringFormatter::normalize(s2, TypeKind::ARRAY);
  StringFormatter::normalize(s3, TypeKind::MAP);
  StringFormatter::normalize(s4, TypeKind::ARRAY);
  ASSERT_TRUE(s == "1, 2, 3");
  ASSERT_TRUE(s0 == "1,2,3");
  ASSERT_TRUE(s1 == "1,+I[2,3]");
  ASSERT_TRUE(s2 == "2,3,4");
  ASSERT_TRUE(s3 == "2=3,3=4");
  ASSERT_TRUE(s4 == "2,[3,4]");
}

TEST_F(StringFormatterTest, testSplit) {
  std::string s0 = "1,2,3";
  std::string s1 = "1,+I[2,3],[3,4,5],{2=3,3=4}";
  std::string s2 = "1, +I[2, 3], [3, 4, 5], {2=3, 3=4}";
  std::string s3 = "1,[+I[1,2],+I[3,4]]";
  std::string s4 = "1,[{a=1},{b=2}]";
  std::vector<std::string> ss0 = StringFormatter::split(s0, ",");
  ASSERT_TRUE(ss0.size() == 3);
  ASSERT_TRUE(ss0[0] == "1");
  ASSERT_TRUE(ss0[1] == "2");
  ASSERT_TRUE(ss0[2] == "3");
  std::vector<std::string> ss1 = StringFormatter::split(s1, ",");
  ASSERT_TRUE(ss1.size() == 4);
  ASSERT_TRUE(ss1[0] == "1");
  ASSERT_TRUE(ss1[1] == "+I[2,3]");
  ASSERT_TRUE(ss1[2] == "[3,4,5]");
  ASSERT_TRUE(ss1[3] == "{2=3,3=4}");
  std::vector<std::string> ss2 = StringFormatter::split(s2, ", ");
  ASSERT_TRUE(ss2.size() == 4);
  ASSERT_TRUE(ss2[0] == "1");
  ASSERT_TRUE(ss2[1] == "+I[2, 3]");
  ASSERT_TRUE(ss2[2] == "[3, 4, 5]");
  ASSERT_TRUE(ss2[3] == "{2=3, 3=4}");
  std::vector<std::string> ss3 = StringFormatter::split(s3, ",");
  ASSERT_TRUE(ss3.size() == 2);
  ASSERT_TRUE(ss3[0] == "1");
  ASSERT_TRUE(ss3[1] == "[+I[1,2],+I[3,4]]");
  std::vector<std::string> ss4 = StringFormatter::split(s4, ",");
  ASSERT_TRUE(ss4.size() == 2);
}

} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}