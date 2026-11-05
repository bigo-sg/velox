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
#include "velox/functions/flinksql/StringFunctions.h"

#include <cstring>
#include <optional>
#include <string>
#include <string_view>

#include "velox/expression/EvalCtx.h"
#include "velox/expression/StringWriter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions::flinksql {
namespace {

inline size_t utf8CharLength(char c) {
  const unsigned char u = static_cast<unsigned char>(c);
  if (u < 0x80) {
    return 1;
  }
  if ((u & 0xE0) == 0xC0) {
    return 2;
  }
  if ((u & 0xF0) == 0xE0) {
    return 3;
  }
  return 4;
}

// True when s is exactly one UTF-8 code point. Flink's LIKE escape is one Java
// char, which is a single code point but may be a multi-byte UTF-8 sequence
// (e.g. 'é' is 2 bytes).
inline bool isSingleCodePoint(StringView s) {
  return !s.empty() && utf8CharLength(s.data()[0]) == s.size();
}

// -------------------- LIKE --------------------

template <typename T>
re2::StringPiece toStringPiece(const T& s) {
  return re2::StringPiece(s.data(), s.size());
}

void checkForBadPattern(const RE2& re) {
  if (UNLIKELY(!re.ok())) {
    VELOX_USER_FAIL("invalid regular expression:{}", re.error());
  }
}

FlatVector<bool>& ensureWritableBool(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    VectorPtr& result) {
  context.ensureWritable(rows, BOOLEAN(), result);
  return *result->as<FlatVector<bool>>();
}

bool re2FullMatch(StringView str, const RE2& re) {
  return RE2::FullMatch(toStringPiece(str), re);
}

// Flink-aligned LIKE pattern -> RE2 regex.
//
// Mirrors Flink's SqlLikeUtils.sqlToRegexLike:
//   '_' -> matches any single char except Java's line terminators
//         (\n \r \u0085 \u2028 \u2029). RE2's '.' (dot_nl) only excludes '\n',
//         so we use an explicit negated class to match Java's 5 terminators.
//   '%' -> '.*' (combined with dot_nl(true), matches any sequence incl. \n).
std::string likePatternToRe2Flink(
    StringView pattern,
    const std::optional<std::string>& escapeChar,
    bool& validPattern) {
  std::string regex;
  validPattern = true;
  regex.reserve(pattern.size() * 2);
  regex.append("^");

  const size_t escapeSize = escapeChar.has_value() ? escapeChar->size() : 0;
  auto isEscapeAt = [&](size_t pos) {
    return escapeSize > 0 && pos + escapeSize <= pattern.size() &&
        std::memcmp(pattern.data() + pos, escapeChar->data(), escapeSize) == 0;
  };

  bool escaped = false;
  size_t pos = 0;
  while (pos < pattern.size()) {
    const char c = pattern.data()[pos];
    if (escaped) {
      // The escaped character is a literal; only '%', '_' and the escape
      // character itself may follow an escape.
      if (c == '%') {
        regex.append("%");
        ++pos;
      } else if (c == '_') {
        regex.append("_");
        ++pos;
      } else if (isEscapeAt(pos)) {
        regex.append(escapeChar->data(), escapeSize);
        pos += escapeSize;
      } else {
        validPattern = false;
        ++pos;
      }
      escaped = false;
      continue;
    }

    if (isEscapeAt(pos)) {
      escaped = true;
      pos += escapeSize;
      continue;
    }

    switch (c) {
      case '%':
        regex.append(".*");
        break;
      case '_':
        // Flink maps '_' to Java's '.', which counts UTF-16 units, so an astral
        // character (surrogate pair, e.g. emoji) consumes two '_' in Flink. The
        // negated class below matches one UTF-8 code point, so here a single
        // '_' matches one astral character. Accepted platform-representation
        // difference.
        regex.append("[^\\n\\r\\x{85}\\x{2028}\\x{2029}]");
        break;
      // Escape all the meta characters in re2.
      case '\\':
      case '|':
      case '^':
      case '$':
      case '.':
      case '*':
      case '+':
      case '?':
      case '(':
      case ')':
      case '[':
      case ']':
      case '{':
      case '}':
        regex.append("\\");
        [[fallthrough]];
      default:
        regex.append(1, c);
        break;
    }
    ++pos;
  }
  if (escaped) {
    validPattern = false;
  }
  regex.append("$");
  return regex;
}

// Constant pattern: compile once.
class LikeWithRe2Flink final : public exec::VectorFunction {
 public:
  LikeWithRe2Flink(StringView pattern, std::optional<std::string> escapeChar)
      : emptyPattern_(pattern.size() == 0) {
    RE2::Options opt{RE2::Quiet};
    opt.set_dot_nl(true);
    re_.emplace(
        toStringPiece(
            likePatternToRe2Flink(pattern, escapeChar, validPattern_)),
        opt);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);

    if (!validPattern_) {
      auto error = std::make_exception_ptr(std::invalid_argument(
          "Escape character must be followed by '%', '_' or the escape character itself"));
      context.setErrors(rows, error);
      return;
    }

    try {
      checkForBadPattern(*re_);
    } catch (const std::exception&) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);

    if (emptyPattern_) {
      // An empty pattern matches every input: verified against the Flink
      // 1.19.2 native runtime by an end-to-end dual-run (the master-only
      // `mark > 0 && isEmptyPattern` gate of SqlLikeChainChecker is absent
      // in 1.19.x). This diverges from the SQL standard (an empty pattern
      // would match only the empty string); we follow the Flink runtime
      // for parity.
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t i) { result.set(i, true); });
      return;
    }

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto toSearch = decodedArgs.at(0);
    if (toSearch->isIdentityMapping()) {
      auto rawStrings = toSearch->data<StringView>();
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        result.set(i, re2FullMatch(rawStrings[i], *re_));
      });
      return;
    }

    if (toSearch->isConstantMapping()) {
      bool match = re2FullMatch(toSearch->valueAt<StringView>(0), *re_);
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t i) { result.set(i, match); });
      return;
    }

    // Since the pattern and escapeChar (2nd and 3rd args) are constants, the
    // first arg is expected to be flat or constant only.
    VELOX_UNREACHABLE();
  }

 private:
  std::optional<RE2> re_;
  bool validPattern_;
  bool emptyPattern_;
};

// Dynamic pattern: compile on demand, without the fast path.
class LikeGenericFlink final : public exec::VectorFunction {
 public:
  explicit LikeGenericFlink(int64_t maxCompiledRegexes)
      : maxCompiledRegexes_(maxCompiledRegexes) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& type,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    VectorPtr localResult;
    context.ensureWritable(rows, type, localResult);
    exec::VectorWriter<bool> vectorWriter;
    vectorWriter.init(*localResult->asFlatVector<bool>());
    exec::DecodedArgs decodedArgs(rows, args, context);

    exec::VectorReader<Varchar> inputReader(decodedArgs.at(0));
    exec::VectorReader<Varchar> patternReader(decodedArgs.at(1));

    if (args.size() == 2) {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        vectorWriter.setOffset(row);
        auto* re = findOrCompileRegex(patternReader[row], std::nullopt);
        vectorWriter.current() = re2FullMatch(inputReader[row], *re);
        vectorWriter.commit(true);
      });
    } else {
      VELOX_CHECK_EQ(args.size(), 3);
      exec::VectorReader<Varchar> escapeReader(decodedArgs.at(2));
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        vectorWriter.setOffset(row);
        auto escapeChar = escapeReader[row];
        VELOX_USER_CHECK(
            isSingleCodePoint(escapeChar),
            "Escape string must be a single character");
        auto* re = findOrCompileRegex(
            patternReader[row],
            std::string(escapeChar.data(), escapeChar.size()));
        vectorWriter.current() = re2FullMatch(inputReader[row], *re);
        vectorWriter.commit(true);
      });
    }

    vectorWriter.finish();
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  RE2* findOrCompileRegex(
      const StringView& pattern,
      std::optional<std::string> escapeChar) const {
    const auto key =
        std::pair<std::string, std::optional<std::string>>{pattern, escapeChar};

    auto reIt = compiledRegularExpressions_.find(key);
    if (reIt != compiledRegularExpressions_.end()) {
      return reIt->second.get();
    }

    VELOX_USER_CHECK_LT(
        compiledRegularExpressions_.size(),
        maxCompiledRegexes_,
        "Max number of regex reached");

    bool validEscapeUsage;
    auto regex = likePatternToRe2Flink(pattern, escapeChar, validEscapeUsage);
    VELOX_USER_CHECK(
        validEscapeUsage,
        "Escape character must be followed by '%', '_' or the escape character itself");

    RE2::Options opt{RE2::Quiet};
    opt.set_dot_nl(true);
    auto re = std::make_unique<RE2>(toStringPiece(regex), opt);
    checkForBadPattern(*re);

    auto [it, inserted] =
        compiledRegularExpressions_.emplace(key, std::move(re));
    VELOX_CHECK(inserted);

    return it->second.get();
  }

  mutable folly::F14FastMap<
      std::pair<std::string, std::optional<std::string>>,
      std::unique_ptr<RE2>>
      compiledRegularExpressions_;
  int64_t maxCompiledRegexes_;
};

// -------------------- REPLACE --------------------

// Insert `replacement` once before each UTF-8 code point and once at the end,
// matching Java's String.replace("", "x").
void replaceAtCodePointBoundaries(
    exec::StringWriter& output,
    const StringView& input,
    const StringView& replacement) {
  if (replacement.size() == 0) {
    output.resize(input.size());
    if (input.size() > 0) {
      std::memcpy(output.data(), input.data(), input.size());
    }
    return;
  }

  size_t numChars = 0;
  for (size_t i = 0; i < input.size();) {
    i += utf8CharLength(input.data()[i]);
    ++numChars;
  }

  output.reserve(input.size() + (numChars + 1) * replacement.size());
  char* out = output.data();
  size_t pos = 0;
  std::memcpy(out + pos, replacement.data(), replacement.size());
  pos += replacement.size();
  for (size_t i = 0; i < input.size();) {
    const size_t charLen = utf8CharLength(input.data()[i]);
    std::memcpy(out + pos, input.data() + i, charLen);
    pos += charLen;
    i += charLen;
    std::memcpy(out + pos, replacement.data(), replacement.size());
    pos += replacement.size();
  }
  output.resize(pos);
}

class ReplaceFlink final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::LocalDecodedVector decodedStringHolder(context, *args[0], rows);
    auto decodedStringInput = decodedStringHolder.get();
    exec::LocalDecodedVector decodedSearchHolder(context, *args[1], rows);
    auto decodedSearchInput = decodedSearchHolder.get();
    exec::LocalDecodedVector decodedReplaceHolder(context, *args[2], rows);
    auto decodedReplaceInput = decodedReplaceHolder.get();

    auto stringReader = [&](const vector_size_t row) {
      return decodedStringInput->valueAt<StringView>(row);
    };
    auto searchReader = [&](const vector_size_t row) {
      return decodedSearchInput->valueAt<StringView>(row);
    };
    auto replaceReader = [&](const vector_size_t row) {
      return decodedReplaceInput->valueAt<StringView>(row);
    };

    VectorPtr emptyVectorPtr;
    prepareFlatResultsVector(result, rows, context, emptyVectorPtr);
    auto* resultFlatVector = result->as<FlatVector<StringView>>();

    rows.applyToSelected([&](int row) {
      auto proxy = exec::StringWriter(resultFlatVector, row);
      const auto input = stringReader(row);
      const auto search = searchReader(row);
      const auto replacement = replaceReader(row);
      if (search.size() == 0) {
        replaceAtCodePointBoundaries(proxy, input, replacement);
      } else {
        stringImpl::replace(proxy, input, search, replacement, false);
      }
      proxy.finalize();
    });
  }
};

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> replaceSignatures() {
  return {
      // varchar, varchar, varchar -> varchar
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeReplaceFlink(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& /* inputArgs */,
    const core::QueryConfig& /* config */) {
  return std::make_shared<ReplaceFlink>();
}

std::shared_ptr<exec::VectorFunction> makeLikeFlink(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  auto numArgs = inputArgs.size();

  std::optional<std::string> escapeChar;
  if (numArgs == 3) {
    BaseVector* escape = inputArgs[2].constantValue.get();
    if (!escape) {
      return std::make_shared<LikeGenericFlink>(
          config.exprMaxCompiledRegexes());
    }

    auto constantEscape = escape->as<ConstantVector<StringView>>();
    if (constantEscape->isNullAt(0)) {
      return std::make_shared<exec::ApplyNeverCalled>();
    }

    try {
      VELOX_USER_CHECK(
          isSingleCodePoint(constantEscape->valueAt(0)),
          "Escape string must be a single character");
    } catch (...) {
      return std::make_shared<exec::AlwaysFailingVectorFunction>(
          std::current_exception());
    }
    auto escapeStr = constantEscape->valueAt(0);
    escapeChar = std::string(escapeStr.data(), escapeStr.size());
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (!constantPattern) {
    return std::make_shared<LikeGenericFlink>(config.exprMaxCompiledRegexes());
  }

  if (constantPattern->isNullAt(0)) {
    return std::make_shared<exec::ApplyNeverCalled>();
  }
  auto pattern = constantPattern->as<ConstantVector<StringView>>()->valueAt(0);

  return std::make_shared<LikeWithRe2Flink>(pattern, escapeChar);
}

} // namespace facebook::velox::functions::flinksql
