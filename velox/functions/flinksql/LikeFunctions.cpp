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
#include "velox/functions/flinksql/LikeFunctions.h"

#include <optional>
#include <string>

#include "velox/functions/lib/Re2Functions.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions::flinksql {
namespace {

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
    std::optional<char> escapeChar,
    bool& validPattern) {
  std::string regex;
  validPattern = true;
  regex.reserve(pattern.size() * 2);
  regex.append("^");
  bool escaped = false;
  for (const char c : pattern) {
    if (escaped && !(c == '%' || c == '_' || c == escapeChar)) {
      validPattern = false;
    }
    if (!escaped && c == escapeChar) {
      escaped = true;
    } else {
      switch (c) {
        case '%':
          regex.append(escaped ? "%" : ".*");
          escaped = false;
          break;
        case '_':
          regex.append(escaped ? "_" : "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]");
          escaped = false;
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
          escaped = false;
      }
    }
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
  LikeWithRe2Flink(StringView pattern, std::optional<char> escapeChar)
      : emptyPattern_(pattern.size() == 0) {
    RE2::Options opt{RE2::Quiet};
    opt.set_dot_nl(true);
    re_.emplace(
        toStringPiece(likePatternToRe2Flink(pattern, escapeChar, validPattern_)),
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
      // Flink's LikeCallGen routes an empty literal pattern into
      // SqlLikeChainChecker, whose zero-checker chain vacuously matches every
      // input. Reproduce that instead of the SQL-standard '^$' (which only
      // matches the empty string).
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
        VELOX_USER_CHECK_EQ(
            escapeChar.size(), 1, "Escape string must be a single character");
        auto* re = findOrCompileRegex(patternReader[row], escapeChar.data()[0]);
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
      std::optional<char> escapeChar) const {
    const auto key =
        std::pair<std::string, std::optional<char>>{pattern, escapeChar};

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
      std::pair<std::string, std::optional<char>>,
      std::unique_ptr<RE2>>
      compiledRegularExpressions_;
  int64_t maxCompiledRegexes_;
};

} // namespace

std::shared_ptr<exec::VectorFunction> makeLikeFlink(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  auto numArgs = inputArgs.size();

  std::optional<char> escapeChar;
  if (numArgs == 3) {
    BaseVector* escape = inputArgs[2].constantValue.get();
    if (!escape) {
      return std::make_shared<LikeGenericFlink>(config.exprMaxCompiledRegexes());
    }

    auto constantEscape = escape->as<ConstantVector<StringView>>();
    if (constantEscape->isNullAt(0)) {
      return std::make_shared<exec::ApplyNeverCalled>();
    }

    try {
      VELOX_USER_CHECK_EQ(
          constantEscape->valueAt(0).size(),
          1,
          "Escape string must be a single character");
    } catch (...) {
      return std::make_shared<exec::AlwaysFailingVectorFunction>(
          std::current_exception());
    }
    escapeChar = constantEscape->valueAt(0).data()[0];
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
