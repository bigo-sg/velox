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
  const auto trim = [&](
                        const std::optional<std::string>& str,
                        const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("trim(c0, c1)", str, seek);
  };
  const auto ltrim = [&](
                         const std::optional<std::string>& str,
                         const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("ltrim(c0, c1)", str, seek);
  };
  const auto rtrim = [&](
                         const std::optional<std::string>& str,
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
  const auto trim = [&](
                        const std::optional<std::string>& str,
                        const std::optional<std::string>& seek) {
    return evaluateOnce<std::string>("trim(c0, c1)", str, seek);
  };
  const auto ltrim = [&](
                         const std::optional<std::string>& str,
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
  const auto trimWithSpaceSeek = [&](
                                     const std::optional<std::string>& str,
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
  const auto lpad =
      [&](const std::optional<std::string>& str,
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
  const auto rpad =
      [&](const std::optional<std::string>& str,
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
  const auto lpad =
      [&](const std::optional<std::string>& str,
          const std::optional<int32_t>& len,
          const std::optional<std::string>& pad) {
        return evaluateOnce<std::string>("lpad(c0, c1, c2)", str, len, pad);
      };
  const auto rpad =
      [&](const std::optional<std::string>& str,
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
}

// NULL in any argument position propagates NULL.
TEST_F(FlinkStringTest, padNullArguments) {
  const auto lpad =
      [&](const std::optional<std::string>& str,
          const std::optional<int32_t>& len,
          const std::optional<std::string>& pad) {
        return evaluateOnce<std::string>("lpad(c0, c1, c2)", str, len, pad);
      };
  const auto rpad =
      [&](const std::optional<std::string>& str,
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

} // namespace
} // namespace facebook::velox::functions::flinksql::test
