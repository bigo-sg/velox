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
#pragma once

#include <cstring>
#include <string_view>
#include <utility>

#include "velox/functions/Macros.h"
#include "velox/functions/Udf.h"
#include "velox/functions/lib/string/StringCore.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions::flinksql {

/// Flink TRIM family. Unlike prestosql (which removes all Unicode
/// whitespace), Flink's default trim removes only the 0x20 space character;
/// tabs, newlines and other whitespace are preserved.
template <typename T, bool leftTrim, bool rightTrim>
struct TrimSpaceFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str) {
    // The single-byte predicate is also correct for non-ASCII input: the 0x20
    // code point only appears in UTF-8 as a standalone byte.
    stringImpl::trimAscii<leftTrim, rightTrim>(
        result, str, stringImpl::isAsciiSpace);
  }
};

/// Flink two-argument trim: seek is a character set, not a prefix. Each UTF-8
/// character of str is removed from the ends while it appears in seek. The
/// Flink argument order is (str, seek) — unlike sparksql's (trimStr, srcStr).
template <typename T, bool leftTrim, bool rightTrim>
struct TrimCharsFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument (str).
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& seek) {
    if (str.empty()) {
      result.setEmpty();
      return;
    }
    if (seek.empty()) {
      result.setNoCopy(str);
      return;
    }

    auto seekView = std::string_view(seek);
    size_t resultStartIndex = 0;
    if constexpr (leftTrim) {
      resultStartIndex = std::string_view(str).find_first_not_of(seekView);
      if (resultStartIndex == std::string_view::npos) {
        result.setEmpty();
        return;
      }
    }

    size_t resultSize = str.size() - resultStartIndex;
    if constexpr (rightTrim) {
      size_t lastIndex =
          std::string_view(str.data() + resultStartIndex, resultSize)
              .find_last_not_of(seekView);
      if (lastIndex == std::string_view::npos) {
        result.setEmpty();
        return;
      }
      resultSize = lastIndex + 1;
    }

    result.setNoCopy(StringView(str.data() + resultStartIndex, resultSize));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& seek) {
    if (str.empty()) {
      result.setEmpty();
      return;
    }
    if (seek.empty()) {
      result.setNoCopy(str);
      return;
    }

    auto seekView = std::string_view(seek);
    auto resultBegin = str.begin();
    if constexpr (leftTrim) {
      while (resultBegin < str.end()) {
        int charLen = utf8proc_char_length(resultBegin);
        // Skip malformed bytes one at a time.
        if (UNLIKELY(charLen < 1)) {
          charLen = 1;
        }
        auto c = std::string_view(resultBegin, charLen);
        if (seekView.find(c) == std::string_view::npos) {
          break;
        }
        resultBegin += charLen;
      }
    }

    auto resultEnd = str.end();
    if constexpr (rightTrim) {
      auto curPos = resultEnd - 1;
      while (curPos >= resultBegin) {
        if (utf8proc_char_first_byte(curPos)) {
          auto c = std::string_view(curPos, resultEnd - curPos);
          if (seekView.find(c) == std::string_view::npos) {
            break;
          }
          resultEnd = curPos;
        }
        --curPos;
      }
    }

    result.setNoCopy(StringView(resultBegin, resultEnd - resultBegin));
  }
};

/// Flink trim family with both the one-argument (spaces only) and the
/// two-argument (character set) overloads in one struct, mirroring the
/// prestosql TrimFunctionBase shape.
template <typename T, bool leftTrim, bool rightTrim>
struct TrimFunctionBase : public TrimSpaceFunctionBase<T, leftTrim, rightTrim>,
                          public TrimCharsFunctionBase<T, leftTrim, rightTrim> {
  // Redeclared here: both bases define the same value, and a derived-scope
  // declaration keeps the lookups unambiguous.
  static constexpr int32_t reuse_strings_from_arg = 0;
  static constexpr bool is_default_ascii_behavior = true;

  using TrimSpaceFunctionBase<T, leftTrim, rightTrim>::call;
  using TrimCharsFunctionBase<T, leftTrim, rightTrim>::call;
  using TrimCharsFunctionBase<T, leftTrim, rightTrim>::callAscii;
};

template <typename T>
struct TrimFunction : public TrimFunctionBase<T, true, true> {};

template <typename T>
struct LTrimFunction : public TrimFunctionBase<T, true, false> {};

template <typename T>
struct RTrimFunction : public TrimFunctionBase<T, false, true> {};

/// Flink two-argument trim with a runtime-sourced seek (a column or a
/// computed expression, as opposed to a plan constant). Flink's
/// BinaryStringDataUtil.isSpaceString (BinaryStringDataUtil.java:967-973)
/// tests only byteAt(0) == ' ' for segment-backed strings — their Java object
/// is null — so a seek whose first byte is a space degrades the trim to
/// space-only instead of trimming the character set. Plan constants keep
/// their Java object and take the character-set walk, which is what the
/// plain two-argument functions above implement.
template <typename T, bool leftTrim, bool rightTrim>
struct TrimRuntimeFunctionBase
    : public TrimFunctionBase<T, leftTrim, rightTrim> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  using TrimFunctionBase<T, leftTrim, rightTrim>::call;
  using TrimFunctionBase<T, leftTrim, rightTrim>::callAscii;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& seek) {
    if (UNLIKELY(seek.empty())) {
      result.setNoCopy(str);
      return;
    }
    if (seek.data()[0] == ' ') {
      TrimSpaceFunctionBase<T, leftTrim, rightTrim>::call(result, str);
      return;
    }
    TrimCharsFunctionBase<T, leftTrim, rightTrim>::call(result, str, seek);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& seek) {
    if (UNLIKELY(seek.empty())) {
      result.setNoCopy(str);
      return;
    }
    if (seek.data()[0] == ' ') {
      TrimSpaceFunctionBase<T, leftTrim, rightTrim>::call(result, str);
      return;
    }
    TrimCharsFunctionBase<T, leftTrim, rightTrim>::callAscii(result, str, seek);
  }
};

template <typename T>
struct TrimRuntimeFunction : public TrimRuntimeFunctionBase<T, true, true> {};

template <typename T>
struct LTrimRuntimeFunction : public TrimRuntimeFunctionBase<T, true, false> {};

template <typename T>
struct RTrimRuntimeFunction : public TrimRuntimeFunctionBase<T, false, true> {};

/// Flink LPAD/RPAD. Diverges from prestosql/sparksql in two ways: a negative
/// length or an empty pad returns NULL (velox user check error elsewhere),
/// and there is no 1MB length cap. Width is counted in UTF-8 code points,
/// which matches Flink's UTF-16 code unit count for BMP characters.
template <typename T, bool lpad>
struct PadFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str,
      const arg_type<int32_t>& len,
      const arg_type<Varchar>& pad) {
    return doPad<false>(result, str, len, pad);
  }

  FOLLY_ALWAYS_INLINE bool callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str,
      const arg_type<int32_t>& len,
      const arg_type<Varchar>& pad) {
    return doPad<true>(result, str, len, pad);
  }

 private:
  template <bool isAscii>
  FOLLY_ALWAYS_INLINE bool doPad(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str,
      const arg_type<int32_t>& len,
      const arg_type<Varchar>& pad) {
    // Flink's LPAD/RPAD return NULL for a negative length or an empty pad.
    if (UNLIKELY(len < 0 || pad.size() == 0)) {
      return false;
    }

    const int64_t padCharLength = stringImpl::length<isAscii>(pad);
    // A malformed pad made of UTF-8 continuation bytes has a positive byte
    // size but a zero character length; treat it like an empty pad (NULL) —
    // the padding arithmetic below would otherwise divide by zero.
    if (UNLIKELY(padCharLength == 0)) {
      return false;
    }
    const int64_t strCharLength = stringImpl::length<isAscii>(str);

    // If str already has at least len characters, truncate to the first len
    // characters. This also covers len == 0 (empty result).
    if (UNLIKELY(strCharLength >= len)) {
      size_t prefixByteSize =
          stringCore::getByteRange<isAscii>(str.data(), str.size(), 1, len)
              .second;
      result.resize(prefixByteSize);
      if (LIKELY(prefixByteSize > 0)) {
        std::memcpy(result.data(), str.data(), prefixByteSize);
      }
      return true;
    }

    // How many characters need to be added to str.
    const int64_t fullPaddingCharLength = len - strCharLength;
    // How many full copies of pad need to be added.
    const int64_t fullPadCopies = fullPaddingCharLength / padCharLength;
    // If pad's length does not evenly divide the padding length, how many
    // bytes of pad's prefix need to be added at the end of the padding.
    const size_t padPrefixByteLength =
        stringCore::getByteRange<isAscii>(
            pad.data(), pad.size(), 1, fullPaddingCharLength % padCharLength)
            .second;
    const int64_t fullPaddingByteLength =
        pad.size() * fullPadCopies + padPrefixByteLength;
    const int64_t outputByteLength = str.size() + fullPaddingByteLength;
    // What byte offset in the output the padding starts at.
    int64_t paddingOffset;

    result.resize(outputByteLength);

    if constexpr (lpad) {
      paddingOffset = 0;
      // Copy str after the padding.
      std::memcpy(
          result.data() + fullPaddingByteLength, str.data(), str.size());
    } else {
      paddingOffset = str.size();
      // Copy str at the beginning of the output.
      std::memcpy(result.data(), str.data(), str.size());
    }

    for (int64_t i = 0; i < fullPadCopies; i++) {
      std::memcpy(
          result.data() + paddingOffset + i * pad.size(),
          pad.data(),
          pad.size());
    }

    // Add the final prefix of pad to the padding (if necessary).
    std::memcpy(
        result.data() + paddingOffset + fullPadCopies * pad.size(),
        pad.data(),
        padPrefixByteLength);
    return true;
  }
};

template <typename T>
struct LPadFunction : public PadFunctionBase<T, true> {};

template <typename T>
struct RPadFunction : public PadFunctionBase<T, false> {};

} // namespace facebook::velox::functions::flinksql
