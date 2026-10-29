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

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::flinksql::test {

namespace {
// Digests pinned from Flink's ScalarFunctionsTest / SqlExpressionTest and
// probed independently; lowercase hex over the UTF-8 bytes of the input.
const std::string kMd5Empty{"d41d8cd98f00b204e9800998ecf8427e"};
const std::string kMd5Test{"098f6bcd4621d373cade4e832627b4f6"};
const std::string kMd5Abc{"900150983cd24fb0d6963f7d28e17f72"};
const std::string kMd5Utf8{"65396ee4aad0b4f17aacd1c6112ee364"};
const std::string kMd5Long{"e9b1713db620f1e3a14b6812de523f4b"};
const std::string kSha1Empty{"da39a3ee5e6b4b0d3255bfef95601890afd80709"};
const std::string kSha1Test{"a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"};
const std::string kSha1Abc{"a9993e364706816aba3e25717850c26c9cd0d89d"};
const std::string kSha1Utf8{"dabaa5fe7c47fb21be902480a13013f16a1ab6eb"};
const std::string kSha1Long{"a26704c04fc5f10db5aab58468035531cc542485"};
const std::string kEmpty;
const std::string kTest{"test"};
const std::string kAbc{"abc"};
const std::string kUtf8{"你好世界"};
const std::string kLong{"0123456789abcdefghijklmnopqrstuvwxyz"};
} // namespace

class FlinkHashTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    functions::test::FunctionBaseTest::SetUpTestCase();
    registerHashFunctions("");
  }

  std::optional<std::string> md5Once(const std::optional<std::string>& s) {
    return evaluateOnce<std::string>("md5(c0)", s);
  }

  std::optional<std::string> sha1Once(const std::optional<std::string>& s) {
    return evaluateOnce<std::string>("sha1(c0)", s);
  }
};

// A. md5 over empty / plain / case-variant / UTF-8 / long inputs.
TEST_F(FlinkHashTest, Md5Basics) {
  EXPECT_EQ(kMd5Empty, md5Once(kEmpty));
  EXPECT_EQ(kMd5Test, md5Once(kTest));
  EXPECT_EQ(kMd5Abc, md5Once(kAbc));
  EXPECT_EQ(kMd5Utf8, md5Once(kUtf8));
  EXPECT_EQ(kMd5Long, md5Once(kLong));
}

// B. sha1 over the same inputs.
TEST_F(FlinkHashTest, Sha1Basics) {
  EXPECT_EQ(kSha1Empty, sha1Once(kEmpty));
  EXPECT_EQ(kSha1Test, sha1Once(kTest));
  EXPECT_EQ(kSha1Abc, sha1Once(kAbc));
  EXPECT_EQ(kSha1Utf8, sha1Once(kUtf8));
  EXPECT_EQ(kSha1Long, sha1Once(kLong));
}

// C. NULL in -> NULL out.
TEST_F(FlinkHashTest, NullInputs) {
  EXPECT_EQ(std::nullopt, md5Once(std::nullopt));
  EXPECT_EQ(std::nullopt, sha1Once(std::nullopt));
}

// D. Multi-row evaluation: distinct rows get distinct digests of the expected
// length (32 / 40 lowercase hex chars).
TEST_F(FlinkHashTest, MultiRow) {
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<std::string>({kEmpty, kTest, kUtf8, kLong})});

  auto md5 = evaluate("md5(c0)", data);
  auto md5Flat = md5->asFlatVector<StringView>();
  ASSERT_EQ(4, md5Flat->size());
  EXPECT_EQ(kMd5Empty, md5Flat->valueAt(0).str());
  EXPECT_EQ(kMd5Test, md5Flat->valueAt(1).str());
  EXPECT_EQ(kMd5Utf8, md5Flat->valueAt(2).str());
  EXPECT_EQ(kMd5Long, md5Flat->valueAt(3).str());
  EXPECT_EQ(32, md5Flat->valueAt(0).size());

  auto sha1 = evaluate("sha1(c0)", data);
  auto sha1Flat = sha1->asFlatVector<StringView>();
  ASSERT_EQ(4, sha1Flat->size());
  EXPECT_EQ(kSha1Empty, sha1Flat->valueAt(0).str());
  EXPECT_EQ(kSha1Test, sha1Flat->valueAt(1).str());
  EXPECT_EQ(kSha1Utf8, sha1Flat->valueAt(2).str());
  EXPECT_EQ(kSha1Long, sha1Flat->valueAt(3).str());
  EXPECT_EQ(40, sha1Flat->valueAt(0).size());
}

} // namespace facebook::velox::functions::flinksql::test
