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
#include <gtest/gtest.h>
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"

#include <cstdint>
#include <optional>
#include <string>

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
namespace facebook::velox::functions::flinksql::test {
namespace {

// Flink-semantics string function tests. Expected values follow Flink's
// runtime (BinaryStringDataUtil / SqlFunctionUtils), not presto/spark:
// - 1-arg trim removes only 0x20 spaces; tabs/newlines are preserved.
// - 2-arg trim treats the second argument as a character set.
// - lpad/rpad return NULL for a negative length or an empty pad string.
class FlinkStringTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    // FunctionBaseTest registers the prestosql baseline first; the flinksql
    // functions layer on top under the same names (overwrite semantics), so
    // name lookups below resolve to the Flink implementations.
    FunctionBaseTest::SetUpTestCase();
    registerStringFunctions();
  }

  VectorPtr eval(const std::string& expr, const VectorPtr& input) {
    return evaluate(expr, makeRowVector({input}));
  }
};

// basic one-argument trim for all three ends.
// all-spaces / empty / NULL inputs.
TEST_F(FlinkStringTest, trimSpaces) {
  const auto trim = [&](const std::optional<std::string>& str) {
    return evaluateOnce<std::string>("trim(c0)", str);
  };
  const auto ltrim = [&](const std::optional<std::string>& str) {
    return evaluateOnce<std::string>("ltrim(c0)", str);
  };
  const auto rtrim = [&](const std::optional<std::string>& str) {
    return evaluateOnce<std::string>("rtrim(c0)", str);
  };

  EXPECT_EQ(trim("  ab  "), "ab");
  EXPECT_EQ(ltrim("  ab  "), "ab  ");
  EXPECT_EQ(rtrim("  ab  "), "  ab");

  EXPECT_EQ(trim("  hello world  "), "hello world");
  EXPECT_EQ(trim("  \u6570\u636E  "), "\u6570\u636E");

  EXPECT_EQ(trim("   "), "");
  EXPECT_EQ(ltrim("   "), "");
  EXPECT_EQ(rtrim("   "), "");
  EXPECT_EQ(trim(""), "");
  EXPECT_EQ(ltrim(""), "");
  EXPECT_EQ(rtrim(""), "");

  EXPECT_EQ(trim(std::nullopt), std::nullopt);
  EXPECT_EQ(ltrim(std::nullopt), std::nullopt);
  EXPECT_EQ(rtrim(std::nullopt), std::nullopt);
}

// only 0x20 is removed - tabs and newlines are preserved. This is
// the key difference from prestosql, which trims all Unicode whitespace.
TEST_F(FlinkStringTest, trimSpacesWhitespaceSemantics) {
  const auto trim = [&](const std::optional<std::string>& str) {
    return evaluateOnce<std::string>("trim(c0)", str);
  };
  const auto ltrim = [&](const std::optional<std::string>& str) {
    return evaluateOnce<std::string>("ltrim(c0)", str);
  };
  const auto rtrim = [&](const std::optional<std::string>& str) {
    return evaluateOnce<std::string>("rtrim(c0)", str);
  };

  EXPECT_EQ(trim("\t ab \t"), "\t ab \t");
  EXPECT_EQ(ltrim("\t ab \t"), "\t ab \t");
  EXPECT_EQ(rtrim("\t ab \t"), "\t ab \t");

  // A leading space is removed, the tab behind it stops the walk.
  EXPECT_EQ(ltrim(" \t ab \t "), "\t ab \t ");
  EXPECT_EQ(rtrim(" \t ab \t "), " \t ab \t");
  EXPECT_EQ(trim(" \t ab \t "), "\t ab \t");

  EXPECT_EQ(trim("\n ab \n"), "\n ab \n");
}

// two-argument trim removes characters of a character set,
// not a literal prefix.
TEST_F(FlinkStringTest, trimChars) {
  const auto trim = [&](const std::optional<std::string>& str,
                        const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("trim(c0, c1)", str, seek);
  };
  const auto ltrim = [&](const std::optional<std::string>& str,
                         const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("ltrim(c0, c1)", str, seek);
  };
  const auto rtrim = [&](const std::optional<std::string>& str,
                         const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("rtrim(c0, c1)", str, seek);
  };

  EXPECT_EQ(trim("xxabx", "x"), "ab");
  EXPECT_EQ(ltrim("xxabx", "x"), "abx");
  EXPECT_EQ(rtrim("xxabx", "x"), "xxab");

  // The seek string is a set: every character is matched anywhere at the
  // ends, and the whole string can disappear.
  EXPECT_EQ(trim("ba", "ab"), "");
  EXPECT_EQ(trim("abba", "ab"), "");

  // An empty seek returns the original string; a NULL seek
  // propagates NULL.
  EXPECT_EQ(trim("abc", ""), "abc");
  EXPECT_EQ(ltrim("abc", ""), "abc");
  EXPECT_EQ(rtrim("abc", ""), "abc");
  EXPECT_EQ(trim("abc", std::nullopt), std::nullopt);
  EXPECT_EQ(ltrim("abc", std::nullopt), std::nullopt);
  EXPECT_EQ(rtrim("abc", std::nullopt), std::nullopt);

  // A NULL str propagates NULL.
  EXPECT_EQ(trim(std::nullopt, "x"), std::nullopt);
}

// non-ASCII seek sets walk by UTF-8 character.
// seek containing a space (unified character-set walk, no
// isSpaceString special case).
TEST_F(FlinkStringTest, trimCharsNonAscii) {
  const auto trim = [&](const std::optional<std::string>& str,
                        const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("trim(c0, c1)", str, seek);
  };
  const auto ltrim = [&](const std::optional<std::string>& str,
                         const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("ltrim(c0, c1)", str, seek);
  };

  EXPECT_EQ(trim("\u3002\u6D4B\u8BD5\u3002", "\u3002"), "\u6D4B\u8BD5");
  EXPECT_EQ(ltrim("\u3002\u6D4B\u8BD5\u3002", "\u3002"), "\u6D4B\u8BD5\u3002");

  EXPECT_EQ(trim(" \u6D4B\u8BD5\u3002 ", "\u3002 "), "\u6D4B\u8BD5");
  EXPECT_EQ(trim(" a b ", " x"), "a b");
  EXPECT_EQ(ltrim(" a b ", " x"), "a b ");
}

// trim(s, ' ') is equivalent to the one-argument form.
TEST_F(FlinkStringTest, trimCharsSpaceEquivalentToSingleArg) {
  const auto trimSpace = [&](const std::optional<std::string>& str) {
    return evaluateOnce<std::string>("trim(c0)", str);
  };
  const auto trimWithSpaceSeek = [&](const std::optional<std::string>& str,
                                     const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("trim(c0, c1)", str, seek);
  };

  EXPECT_EQ(trimWithSpaceSeek("  ab  ", " "), trimSpace("  ab  "));
  EXPECT_EQ(trimWithSpaceSeek("\t ab \t", " "), trimSpace("\t ab \t"));
  EXPECT_EQ(trimWithSpaceSeek("   ", " "), trimSpace("   "));
  EXPECT_EQ(trimWithSpaceSeek("", " "), trimSpace(""));
}

// lpad basic padding, truncation, multi-character
// cycling, len = 0 and BMP-wide unicode.
TEST_F(FlinkStringTest, lpad) {
  const auto lpad = [&](const std::optional<std::string>& str,
                        const std::optional<int32_t>& len,
                        const std::optional<std::string>& pad) {
    return evaluateOnce<std::string>("lpad(c0, c1, c2)", str, len, pad);
  };

  EXPECT_EQ("xtext", lpad("text", 5, "x"));
  EXPECT_EQ("  text", lpad("text", 6, " "));
  EXPECT_EQ("text", lpad("text", 4, "x"));
  EXPECT_EQ("xyxtext", lpad("text", 7, "xy"));
  EXPECT_EQ("aaaab", lpad("b", 5, "a"));

  EXPECT_EQ("", lpad("abc", 0, "e"));
  EXPECT_EQ("", lpad("", 0, "e"));

  // Truncation keeps the first len characters.
  EXPECT_EQ("tex", lpad("text", 3, "xy"));

  // BMP unicode is counted in code points.
  EXPECT_EQ("\u671B\u671B\u4FE1\u606F", lpad("\u4FE1\u606F", 4, "\u671B"));
  EXPECT_EQ(
      "\u5E0C\u671B\u5E0C\u4FE1\u606F",
      lpad("\u4FE1\u606F", 5, "\u5E0C\u671B"));
  EXPECT_EQ("\u4FE1\u606F\u6D4B", lpad("\u4FE1\u606F\u6D4B\u8BD5", 3, "x"));
}

TEST_F(FlinkStringTest, rpad) {
  const auto rpad = [&](const std::optional<std::string>& str,
                        const std::optional<int32_t>& len,
                        const std::optional<std::string>& pad) {
    return evaluateOnce<std::string>("rpad(c0, c1, c2)", str, len, pad);
  };

  EXPECT_EQ("textx", rpad("text", 5, "x"));
  EXPECT_EQ("text  ", rpad("text", 6, " "));
  EXPECT_EQ("text", rpad("text", 4, "x"));
  EXPECT_EQ("textxyx", rpad("text", 7, "xy"));

  EXPECT_EQ("", rpad("abc", 0, "e"));
  EXPECT_EQ("tex", rpad("text", 3, "xy"));

  EXPECT_EQ("\u4FE1\u606F\u671B\u671B", rpad("\u4FE1\u606F", 4, "\u671B"));
  EXPECT_EQ(
      "\u4FE1\u606F\u5E0C\u671B\u5E0C",
      rpad("\u4FE1\u606F", 5, "\u5E0C\u671B"));
}

// a negative length or an empty pad yields NULL - unlike
// prestosql/sparksql, which raise a user error.
TEST_F(FlinkStringTest, padInvalidArgumentsReturnNull) {
  const auto lpad = [&](const std::optional<std::string>& str,
                        const std::optional<int32_t>& len,
                        const std::optional<std::string>& pad) {
    return evaluateOnce<std::string>("lpad(c0, c1, c2)", str, len, pad);
  };
  const auto rpad = [&](const std::optional<std::string>& str,
                        const std::optional<int32_t>& len,
                        const std::optional<std::string>& pad) {
    return evaluateOnce<std::string>("rpad(c0, c1, c2)", str, len, pad);
  };

  EXPECT_EQ(lpad("abc", -1, "e"), std::nullopt);
  EXPECT_EQ(rpad("abc", -1, "e"), std::nullopt);
  EXPECT_EQ(lpad("abc", -100, "e"), std::nullopt);
  EXPECT_EQ(rpad("abc", -100, "e"), std::nullopt);

  EXPECT_EQ(lpad("abc", 5, ""), std::nullopt);
  EXPECT_EQ(rpad("abc", 5, ""), std::nullopt);
  EXPECT_EQ(lpad("abc", 0, ""), std::nullopt);
  EXPECT_EQ(rpad("abc", 0, ""), std::nullopt);

  // A malformed pad of UTF-8 continuation bytes has a positive byte size but
  // a zero character length; it is treated like an empty pad (NULL).
  EXPECT_EQ(lpad("abc", 5, "\x80"), std::nullopt);
  EXPECT_EQ(rpad("abc", 5, "\x80"), std::nullopt);
  EXPECT_EQ(lpad("abc", 5, "\x80\x80\x80"), std::nullopt);
}

// NULL in any argument position propagates NULL.
TEST_F(FlinkStringTest, padNullArguments) {
  const auto lpad = [&](const std::optional<std::string>& str,
                        const std::optional<int32_t>& len,
                        const std::optional<std::string>& pad) {
    return evaluateOnce<std::string>("lpad(c0, c1, c2)", str, len, pad);
  };
  const auto rpad = [&](const std::optional<std::string>& str,
                        const std::optional<int32_t>& len,
                        const std::optional<std::string>& pad) {
    return evaluateOnce<std::string>("rpad(c0, c1, c2)", str, len, pad);
  };

  EXPECT_EQ(lpad(std::nullopt, 5, "x"), std::nullopt);
  EXPECT_EQ(rpad(std::nullopt, 5, "x"), std::nullopt);
  EXPECT_EQ(lpad("abc", std::nullopt, "x"), std::nullopt);
  EXPECT_EQ(rpad("abc", std::nullopt, "x"), std::nullopt);
  EXPECT_EQ(lpad("abc", 5, std::nullopt), std::nullopt);
  EXPECT_EQ(rpad("abc", 5, std::nullopt), std::nullopt);
}

// Non-BMP characters: width is counted in UTF-8 code points, while Flink's
// SqlFunctionUtils counts UTF-16 code units (the counts differ for non-BMP
// inputs). These cases pin the code-point behavior.
TEST_F(FlinkStringTest, padNonBmpCodePoints) {
  const auto lpad = [&](const std::optional<std::string>& str,
                        const std::optional<int32_t>& len,
                        const std::optional<std::string>& pad) {
    return evaluateOnce<std::string>("lpad(c0, c1, c2)", str, len, pad);
  };
  const auto rpad = [&](const std::optional<std::string>& str,
                        const std::optional<int32_t>& len,
                        const std::optional<std::string>& pad) {
    return evaluateOnce<std::string>("rpad(c0, c1, c2)", str, len, pad);
  };

  // U+1F600 is one code point (4 UTF-8 bytes, 2 UTF-16 units).
  EXPECT_EQ(lpad("\U0001F600", 2, "x"), "x\U0001F600");
  EXPECT_EQ(rpad("\U0001F600", 2, "x"), "\U0001F600x");
  EXPECT_EQ(lpad("\U0001F600", 1, "x"), "\U0001F600");

  // Truncation cuts on code-point boundaries.
  EXPECT_EQ(lpad("a\U0001F600b", 2, "x"), "a\U0001F600");
  EXPECT_EQ(rpad("a\U0001F600b", 2, "x"), "a\U0001F600");
}

// Runtime-sourced seek: Flink's isSpaceString (BinaryStringDataUtil.java:
// 967-973) tests only byteAt(0) == ' ' for segment-backed strings, so a seek
// starting with a space degrades the trim to space-only. Plan-constant seeks
// take the character-set walk (the plain two-argument functions).
TEST_F(FlinkStringTest, trimRuntimeSeekDegradesToSpaces) {
  const auto rtrimRuntime = [&](const std::optional<std::string>& str,
                                const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("rtrim_runtime(c0, c1)", str, seek);
  };
  const auto ltrimRuntime = [&](const std::optional<std::string>& str,
                                const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("ltrim_runtime(c0, c1)", str, seek);
  };
  const auto trimRuntime = [&](const std::optional<std::string>& str,
                               const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("trim_runtime(c0, c1)", str, seek);
  };
  const auto rtrimCharset = [&](const std::optional<std::string>& str,
                                const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("rtrim(c0, c1)", str, seek);
  };

  // A seek whose first byte is a space trims spaces only.
  EXPECT_EQ(rtrimRuntime("abx ", " x"), "abx");
  EXPECT_EQ(ltrimRuntime(" xab", " x"), "xab");
  EXPECT_EQ(trimRuntime(" xab ", " x"), "xab");

  // A seek that does not start with a space takes the character-set walk.
  EXPECT_EQ(rtrimRuntime("abx ", "x "), "ab");
  EXPECT_EQ(ltrimRuntime(" xab", "x "), "ab");

  // An empty seek returns the string unchanged.
  EXPECT_EQ(rtrimRuntime("abc ", ""), "abc ");

  // Contrast: the plain two-argument function walks the character set for
  // every seek, including one that starts with a space.
  EXPECT_EQ(rtrimCharset("abx ", " x"), "ab");

  // NULL propagation keeps the default behavior.
  EXPECT_EQ(rtrimRuntime("abc", std::nullopt), std::nullopt);
}

// from_base64

TEST_F(FlinkStringTest, fromBase64) {
  auto result = eval(
      "from_base64(c0)",
      makeNullableFlatVector<std::string>({"QTNGMTlDMjdCNEUw"}));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"A3F19C27B4E0"}), result);
}

TEST_F(FlinkStringTest, fromBase64LongInput) {
  std::string raw(100, 'a');
  auto result = eval(
      "from_base64(c0)",
      makeNullableFlatVector<std::string>(
          {"YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYQ=="}));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({raw}), result);
}

TEST_F(FlinkStringTest, fromBase64Batch) {
  auto result = evaluate(
      "from_base64(c0)",
      makeRowVector({makeNullableFlatVector<std::string>(
          {"dGVzdA==", "", std::nullopt, "QQ=="})}));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"test", "", std::nullopt, "A"}),
      result);
}

TEST_F(FlinkStringTest, fromBase64Unpadded) {
  auto result = eval(
      "from_base64(c0)", makeNullableFlatVector<std::string>({"dGVzdA", "YQ"}));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"test", "a"}), result);
}

TEST_F(FlinkStringTest, fromBase64Empty) {
  // An empty input decodes to an empty string, matching Flink's
  // Base64.getDecoder() on "".
  auto result =
      eval("from_base64(c0)", makeNullableFlatVector<std::string>({""}));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({""}), result);
}

TEST_F(FlinkStringTest, fromBase64InvalidInput) {
  // Invalid characters and a dangling unit (length % 4 == 1) error out on both
  // sides: velox raises a user error, Flink's Base64.getDecoder() throws.
  EXPECT_THROW(
      eval("from_base64(c0)", makeNullableFlatVector<std::string>({"!@#$"})),
      VeloxUserError);
  EXPECT_THROW(
      eval("from_base64(c0)", makeNullableFlatVector<std::string>({"a"})),
      VeloxUserError);
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
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"xaxbxcx"}), result);
}

TEST_F(FlinkStringTest, replaceEmptyOldStrMultibyte) {
  // Empty `from` inserts the replacement at UTF-8 character boundaries, not
  // between bytes: replace("你", "", "x") yields "x你x", not invalid UTF-8.
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"你", "a你b"}),
          makeNullableFlatVector<std::string>({"", ""}),
          makeNullableFlatVector<std::string>({"x", "x"}),
      }));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"x你x", "xax你xbx"}), result);
}

TEST_F(FlinkStringTest, replaceNullInput) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({std::nullopt}),
          makeNullableFlatVector<std::string>({"a"}),
          makeNullableFlatVector<std::string>({"b"}),
      }));
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({""}), result);
}

TEST_F(FlinkStringTest, replaceEmptyReplacement) {
  auto result = evaluate(
      "replace(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"abc"}),
          makeNullableFlatVector<std::string>({"b"}),
          makeNullableFlatVector<std::string>({""}),
      }));
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"bc"}), result);
}

TEST_F(FlinkStringTest, rightZeroAndNegative) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({"abc", "abc"}),
      makeNullableFlatVector<int32_t>({0, -5}),
  });
  auto result = evaluate("right(c0, c1)", data);
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"", ""}), result);
}

TEST_F(FlinkStringTest, rightWholeString) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({"abc", "abc"}),
      makeNullableFlatVector<int32_t>({3, 5}),
  });
  auto result = evaluate("right(c0, c1)", data);
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"abc", "abc"}), result);
}

TEST_F(FlinkStringTest, rightMultibyte) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({"abc你好", "abc你好"}),
      makeNullableFlatVector<int32_t>({2, 5}),
  });
  auto result = evaluate("right(c0, c1)", data);
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"你好", "abc你好"}), result);
}

TEST_F(FlinkStringTest, rightNullAndEmpty) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({std::nullopt, "abc", ""}),
      makeNullableFlatVector<int32_t>({2, std::nullopt, 1}),
  });
  auto result = evaluate("right(c0, c1)", data);
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({std::nullopt, std::nullopt, ""}),
      result);
}

TEST_F(FlinkStringTest, rightExtremeN) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>({"abc", "abc"}),
      makeNullableFlatVector<int32_t>({2147483647, -2147483647 - 1}),
  });
  auto result = evaluate("right(c0, c1)", data);
  velox::test::assertEqualVectors(
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
  velox::test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"bc", "o", std::nullopt, "你好"}),
      result);
}

// -------------------- like --------------------

TEST_F(FlinkStringTest, likeEmptyPattern) {
  // A constant empty pattern vacuously matches every input: verified against
  // the Flink 1.19.2 native runtime by an end-to-end dual-run (the
  // master-only `mark > 0 && isEmptyPattern` gate of SqlLikeChainChecker is
  // absent in 1.19.x). This diverges from the SQL standard; we follow the
  // Flink runtime for parity.
  auto result = evaluate(
      "like(c0, '')",
      makeRowVector(
          {makeNullableFlatVector<std::string>({"abc", "", "a\nb"})}));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<bool>({true, true, true}), result);
}

TEST_F(FlinkStringTest, likeBatch) {
  auto result = evaluate(
      "like(c0, c1)",
      makeRowVector({
          makeNullableFlatVector<std::string>(
              {"abc", "abc", std::nullopt, "你好", "", "abc"}),
          makeNullableFlatVector<std::string>(
              {"a%", "_bc", "a%", "__", "", ""}),
      }));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<bool>(
          {true, true, std::nullopt, true, true, false}),
      result);
}

TEST_F(FlinkStringTest, likeLineTerminator) {
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
  velox::test::assertEqualVectors(
      makeNullableFlatVector<bool>(
          {false, false, false, false, false, false, true}),
      result1);

  auto percent = makeRowVector({
      makeNullableFlatVector<std::string>({"\n", "a\nb"}),
      makeNullableFlatVector<std::string>({"%", "a%b"}),
  });
  auto result2 = evaluate("like(c0, c1)", percent);
  velox::test::assertEqualVectors(
      makeNullableFlatVector<bool>({true, true}), result2);
}

TEST_F(FlinkStringTest, likeNonAsciiEscape) {
  // A non-ASCII escape (one Java char, multi-byte UTF-8) is valid: the escaped
  // '%' becomes a literal percent.
  auto result = evaluate(
      "like(c0, 'aé%b', 'é')",
      makeRowVector({makeNullableFlatVector<std::string>({"a%b", "axb"})}));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<bool>({true, false}), result);
}

TEST_F(FlinkStringTest, likeNonAsciiEscapeDynamic) {
  auto result = evaluate(
      "like(c0, c1, c2)",
      makeRowVector({
          makeNullableFlatVector<std::string>({"a%b", "axb", "aé"}),
          makeNullableFlatVector<std::string>({"aé%b", "aé%b", "aéé"}),
          makeNullableFlatVector<std::string>({"é", "é", "é"}),
      }));
  velox::test::assertEqualVectors(
      makeNullableFlatVector<bool>({true, false, true}), result);
}

} // namespace
} // namespace facebook::velox::functions::flinksql::test
