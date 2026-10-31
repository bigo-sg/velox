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

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::functions::flinksql::test {

namespace {
// Digests pinned from Flink's SqlExpressionTest/ScalarFunctionsTest
// ("", "test") and probed independently for the supplements.
const std::string kSha224Empty{
    "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"};
const std::string kSha224Test{
    "90a3ed9e32b2aaf4c61c410eb925426119e1a9dc53d4286ade99a809"};
const std::string kSha256Empty{
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"};
const std::string kSha256Test{
    "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"};
const std::string kSha384Empty{
    "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc"
    "7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b"};
const std::string kSha384Test{
    "768412320f7b0aa5812fce428dc4706b3cae50e02a64caa16a782249bfe8efc"
    "4b7ef1ccb126255d196047dfedf17a0a9"};
const std::string kSha512Empty{
    "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d"
    "0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"};
const std::string kSha512Test{
    "ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db"
    "27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff"};
const std::string kSha224Utf8{
    "b3f5c93d7b531d7e29266412f2c842f8a4b7286871f87d259c6eaf07"};
const std::string kSha256Utf8{
    "beca6335b20ff57ccc47403ef4d9e0b8fccb4442b3151c2e7d50050673d43172"};
const std::string kSha384Utf8{
    "5621250177cc297c02d4c7c2a950d541a52b5c478e1fa16ca5022316de898d7be5c"
    "66b16ad735295b48b84a47e986144"};
const std::string kSha512Utf8{
    "4b28a152c8e203ebb52e099301041e3cf704a56190d3097ec8b086a0f9bfb4b9d533"
    "ce71fc3bcf374359e506dc5f17322ec3911eac8dd8f5b35308d938ba0c26"};
const std::string kSha224Long{
    "e6e4a6be069cc9bead8b6050856d2b26da6b3f7efa0951e5fb3a54dd"};
const std::string kSha256Long{
    "74e7e5bb9d22d6db26bf76946d40fff3ea9f0346b884fd0694920fccfad15e33"};
const std::string kSha384Long{
    "ce6d4ea5442bc6c830bea1942d4860db9f7b96f0e9d2c3073ffe47a0e1166d95612d"
    "840ff15e5efdd23c1f273096da32"};
const std::string kSha512Long{
    "95cadc34aa46b9fdef432f62fe5bad8d9f475bfbecf797d5802bb5f2937a85d93ce48"
    "57a6262b03834c01c610d74cd1215f9a466dc6ad3dd15078e3309a03a6d"};
const std::string kEmpty;
const std::string kTest{"test"};
const std::string kUtf8{"你好世界"};
const std::string kLong{"0123456789abcdefghijklmnopqrstuvwxyz"};
} // namespace

// Expected values are pinned from Flink's actual behavior (lowercase hex,
// UTF-8 bytes of the string); the rejection of unsupported hash lengths,
// 0 included (Flink 1.19.2 rejects it too), follows the expression card.
class FlinkHashTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    functions::test::FunctionBaseTest::SetUpTestCase();
    registerHashFunctions("");
  }

  std::optional<std::string> sha2Once(
      const std::optional<std::string>& s,
      std::optional<int32_t> hashLength) {
    return evaluateOnce<std::string>("sha2(c0, c1)", s, hashLength);
  }

  // BIGINT hashLength: the NUMERIC_INTEGER family may hand velox any width.
  std::optional<std::string> sha2BigIntOnce(
      const std::optional<std::string>& s,
      std::optional<int64_t> hashLength) {
    return evaluateOnce<std::string>("sha2(c0, c1)", s, hashLength);
  }

  std::optional<std::string> sha224Once(const std::optional<std::string>& s) {
    return evaluateOnce<std::string>("sha224(c0)", s);
  }

  std::optional<std::string> sha256Once(const std::optional<std::string>& s) {
    return evaluateOnce<std::string>("sha256(c0)", s);
  }

  std::optional<std::string> sha384Once(const std::optional<std::string>& s) {
    return evaluateOnce<std::string>("sha384(c0)", s);
  }

  std::optional<std::string> sha512Once(const std::optional<std::string>& s) {
    return evaluateOnce<std::string>("sha512(c0)", s);
  }
};

// A. sha2 basics: each supported length over empty, plain, UTF-8, and long
// inputs; expected digests are Flink-pinned or probed independently.
TEST_F(FlinkHashTest, Sha2SupportedLengths) {
  EXPECT_EQ(kSha224Empty, sha2Once(kEmpty, 224));
  EXPECT_EQ(kSha256Empty, sha2Once(kEmpty, 256));
  EXPECT_EQ(kSha384Empty, sha2Once(kEmpty, 384));
  EXPECT_EQ(kSha512Empty, sha2Once(kEmpty, 512));

  EXPECT_EQ(kSha224Test, sha2Once(kTest, 224));
  EXPECT_EQ(kSha256Test, sha2Once(kTest, 256));
  EXPECT_EQ(kSha384Test, sha2Once(kTest, 384));
  EXPECT_EQ(kSha512Test, sha2Once(kTest, 512));

  EXPECT_EQ(kSha224Utf8, sha2Once(kUtf8, 224));
  EXPECT_EQ(kSha256Utf8, sha2Once(kUtf8, 256));
  EXPECT_EQ(kSha384Utf8, sha2Once(kUtf8, 384));
  EXPECT_EQ(kSha512Utf8, sha2Once(kUtf8, 512));

  EXPECT_EQ(kSha224Long, sha2Once(kLong, 224));
  EXPECT_EQ(kSha256Long, sha2Once(kLong, 256));
  EXPECT_EQ(kSha384Long, sha2Once(kLong, 384));
  EXPECT_EQ(kSha512Long, sha2Once(kLong, 512));
}

// B. hashLength 0 is rejected like any other unsupported length — Flink
// 1.19.2 rejects 0 outright, and the offloaded path must stay consistent with
// native execution; BIGINT hashLength follows the int32 path.
TEST_F(FlinkHashTest, Sha2ZeroAndBigIntLength) {
  VELOX_ASSERT_THROW(sha2Once(kEmpty, 0), "Unsupported SHA-2 hash length: 0");
  VELOX_ASSERT_THROW(sha2Once(kTest, 0), "Unsupported SHA-2 hash length: 0");

  EXPECT_EQ(kSha256Test, sha2BigIntOnce(kTest, 256));
  EXPECT_EQ(kSha512Test, sha2BigIntOnce(kTest, 512));
}

// C. Unsupported hash lengths fail the job, mirroring Flink's rejection.
TEST_F(FlinkHashTest, Sha2UnsupportedLengthFails) {
  VELOX_ASSERT_THROW(sha2Once(kTest, 1), "Unsupported SHA-2 hash length: 1");
  VELOX_ASSERT_THROW(sha2Once(kTest, -1), "Unsupported SHA-2 hash length: -1");
  VELOX_ASSERT_THROW(
      sha2Once(kTest, 385), "Unsupported SHA-2 hash length: 385");
}

// D. Fixed-length spellings agree with sha2 at the matching length.
TEST_F(FlinkHashTest, FixedLengthSpellings) {
  EXPECT_EQ(kSha224Empty, sha224Once(kEmpty));
  EXPECT_EQ(kSha224Test, sha224Once(kTest));
  EXPECT_EQ(kSha224Utf8, sha224Once(kUtf8));
  EXPECT_EQ(kSha224Long, sha224Once(kLong));

  EXPECT_EQ(kSha256Empty, sha256Once(kEmpty));
  EXPECT_EQ(kSha256Test, sha256Once(kTest));
  EXPECT_EQ(kSha256Utf8, sha256Once(kUtf8));
  EXPECT_EQ(kSha256Long, sha256Once(kLong));

  EXPECT_EQ(kSha384Empty, sha384Once(kEmpty));
  EXPECT_EQ(kSha384Test, sha384Once(kTest));
  EXPECT_EQ(kSha384Utf8, sha384Once(kUtf8));
  EXPECT_EQ(kSha384Long, sha384Once(kLong));

  EXPECT_EQ(kSha512Empty, sha512Once(kEmpty));
  EXPECT_EQ(kSha512Test, sha512Once(kTest));
  EXPECT_EQ(kSha512Utf8, sha512Once(kUtf8));
  EXPECT_EQ(kSha512Long, sha512Once(kLong));
}

// E. NULL in -> NULL out for every spelling, including a NULL hashLength.
TEST_F(FlinkHashTest, NullInputs) {
  EXPECT_EQ(std::nullopt, sha2Once(std::nullopt, 256));
  EXPECT_EQ(std::nullopt, sha2Once(kTest, std::nullopt));
  EXPECT_EQ(std::nullopt, sha2BigIntOnce(kTest, std::nullopt));
  EXPECT_EQ(std::nullopt, sha224Once(std::nullopt));
  EXPECT_EQ(std::nullopt, sha256Once(std::nullopt));
  EXPECT_EQ(std::nullopt, sha384Once(std::nullopt));
  EXPECT_EQ(std::nullopt, sha512Once(std::nullopt));
}

// F. Multi-row evaluation: distinct rows get distinct digests of the
// expected length (56/64/96/128 lowercase hex chars).
TEST_F(FlinkHashTest, MultiRow) {
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<std::string>({kEmpty, kTest, kUtf8, kLong})});
  auto result = evaluate("sha256(c0)", data);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_EQ(4, flat->size());
  EXPECT_EQ(kSha256Empty, flat->valueAt(0).str());
  EXPECT_EQ(kSha256Test, flat->valueAt(1).str());
  EXPECT_EQ(kSha256Utf8, flat->valueAt(2).str());
  EXPECT_EQ(kSha256Long, flat->valueAt(3).str());

  auto result224 = evaluate("sha224(c0)", data);
  EXPECT_EQ(56, result224->asFlatVector<StringView>()->valueAt(0).size());
  auto result384 = evaluate("sha384(c0)", data);
  EXPECT_EQ(96, result384->asFlatVector<StringView>()->valueAt(0).size());
  auto result512 = evaluate("sha512(c0)", data);
  EXPECT_EQ(128, result512->asFlatVector<StringView>()->valueAt(0).size());
}

} // namespace facebook::velox::functions::flinksql::test
