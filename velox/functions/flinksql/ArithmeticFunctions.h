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

#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <vector>

#include "folly/CPortability.h"

#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions::flinksql {
namespace detail {

// Integer division with HALF_UP semantics on a positive divisor: a discarded
// fraction of >= 0.5 rounds away from zero (Flink's BigDecimal.setScale with
// RoundingMode.HALF_UP).
template <typename T>
FOLLY_ALWAYS_INLINE T halfUpDiv(T value, T divisor) {
  T q = value / divisor;
  T r = value % divisor;
  if (value < 0) {
    if (r * 2 <= -divisor) {
      --q;
    }
  } else {
    if (r * 2 >= divisor) {
      ++q;
    }
  }
  return q;
}

FOLLY_ALWAYS_INLINE uint64_t uint64Pow10(int n) {
  uint64_t v = 1;
  for (int i = 0; i < n; ++i) {
    v *= 10;
  }
  return v;
}

// Round a double to b decimals the way Flink does: through the value's
// shortest decimal representation (BigDecimal.valueOf uses Double.toString,
// which selects the same shortest round-trip digits as std::to_chars)
// followed by HALF_UP, all in integer mantissa arithmetic. Ties are decided
// in the decimal domain, so this differs from binary-domain rounding
// (ROUND(2.675, 2) = 2.68 here) and from scale-multiply rounding whose
// product lands below the tie at larger magnitudes (ROUND(647568.815, 2) =
// 647568.82 here, not 647568.81).
FOLLY_ALWAYS_INLINE double decimalHalfUpRound(double value, int32_t b) {
  if (std::isnan(value) || std::isinf(value) || value == 0.0) {
    // BigDecimal has no negative zero: Flink rounds -0.0 to +0.0.
    return std::isnan(value) || std::isinf(value) ? value : 0.0;
  }
  const bool negative = value < 0;
  char buf[64];
  const auto res =
      std::to_chars(buf, buf + sizeof(buf), negative ? -value : value);
  if (res.ec != std::errc()) {
    return value;
  }
  // Parse [digits[.digits]][e[+-]exp] into an integer mantissa D and a
  // decimal exponent E so that |value| == D * 10^E.
  uint64_t mantissa = 0;
  int digits = 0;
  int fracDigits = 0;
  int exp10 = 0;
  bool inFraction = false;
  for (const char* p = buf; p != res.ptr; ++p) {
    const char c = *p;
    if (c >= '0' && c <= '9') {
      mantissa = mantissa * 10 + (c - '0');
      ++digits;
      if (inFraction) {
        ++fracDigits;
      }
    } else if (c == '.') {
      inFraction = true;
    } else if (c == 'e' || c == 'E') {
      exp10 = std::atoi(p + 1);
      break;
    }
  }
  int e = exp10 - fracDigits;
  // Rounding position: |value| * 10^b must become an integer via HALF_UP.
  const int shift = e + static_cast<int>(b);
  double magnitude;
  if (shift >= 0) {
    // |value| already has at most b decimals: nothing to round.
    magnitude = negative ? -value : value;
  } else {
    const int drop = -shift;
    uint64_t rounded;
    if (drop >= digits || drop > 18) {
      // Everything is discarded; a carry is still possible while drop <= 18
      // (the mantissa has at most 17 digits).
      rounded = 0;
      if (drop <= 18) {
        const uint64_t divisor = detail::uint64Pow10(drop);
        if (mantissa * 2 >= divisor) {
          rounded = 1;
        }
      }
    } else {
      const uint64_t divisor = detail::uint64Pow10(drop);
      const uint64_t q = mantissa / divisor;
      const uint64_t r = mantissa % divisor;
      rounded = (r * 2 >= divisor) ? q + 1 : q;
    }
    char out[48];
    const auto outRes = std::to_chars(out, out + sizeof(out), rounded);
    *outRes.ptr = 'e';
    const auto expRes =
        std::to_chars(outRes.ptr + 1, out + sizeof(out), -static_cast<long long>(b));
    // Neither to_chars call terminates the buffer; stale digits from a
    // previous invocation would extend the exponent.
    *expRes.ptr = '\0';
    magnitude = std::strtod(out, nullptr);
  }
  return negative ? -magnitude : magnitude;
}

} // namespace detail

// Flink ABS. The (a <= 0) ? 0 - a : a form mirrors Java Math.abs: -0.0
// maps to +0.0 and the most negative integer wraps to itself.
template <typename TExec>
struct AbsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, const T& a) {
    result = (a <= T(0)) ? T(0) - a : a;
  }
};

template <typename TExec>
struct DecimalAbsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<ShortDecimal<P1, S1>>& result,
      const arg_type<ShortDecimal<P1, S1>>& a) {
    result = (a < 0) ? -a : a;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<LongDecimal<P1, S1>>& result,
      const arg_type<LongDecimal<P1, S1>>& a) {
    result = (a < 0) ? -a : a;
  }
};

// Flink unary minus (-x): the most negative integer wraps, matching Flink's
// inlined -x code generation.
template <typename TExec>
struct NegateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, const T& a) {
    result = -a;
  }
};

// Flink FLOOR/CEIL over numerics: input type preserved.
template <typename TExec>
struct FloorFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, const T& a) {
    if constexpr (std::is_integral_v<T>) {
      result = a;
    } else {
      result = std::floor(a);
    }
  }
};

template <typename TExec>
struct CeilFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, const T& a) {
    if constexpr (std::is_integral_v<T>) {
      result = a;
    } else {
      result = std::ceil(a);
    }
  }
};

// Flink FLOOR/CEIL over decimal(p, s): result type is decimal(p, 0)
// (DecimalScale0TypeStrategy), bound by the registration constraints.
template <typename TExec>
struct DecimalFloorFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      const A* /*a*/) {
    rescaleFactor_ = DecimalUtil::kPowersOfTen
        [getDecimalPrecisionScale(*inputTypes[0]).second];
  }

  template <typename R, typename A>
  FOLLY_ALWAYS_INLINE void call(R& out, const A& a) {
    out = a / rescaleFactor_ - ((a % rescaleFactor_) < 0 ? 1 : 0);
  }

 private:
  int128_t rescaleFactor_;
};

template <typename TExec>
struct DecimalCeilFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      const A* /*a*/) {
    rescaleFactor_ = DecimalUtil::kPowersOfTen
        [getDecimalPrecisionScale(*inputTypes[0]).second];
  }

  template <typename R, typename A>
  FOLLY_ALWAYS_INLINE void call(R& out, const A& a) {
    out = a / rescaleFactor_ + ((a % rescaleFactor_) > 0 ? 1 : 0);
  }

 private:
  int128_t rescaleFactor_;
};

// Flink ROUND over fixed-point types: identity for b >= 0, HALF_UP at the
// 10^-b magnitude otherwise. int128 math narrows on store, matching Java's
// BigDecimal.longValue() wrap.
template <typename TExec>
struct RoundFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, const T& a, const int32_t b = 0) {
    if (b >= 0) {
      result = a;
      return;
    }
    const int magnitude = -static_cast<int>(b);
    if (magnitude > 38) {
      result = 0;
      return;
    }
    const int128_t divisor = DecimalUtil::kPowersOfTen[magnitude];
    result = static_cast<T>(detail::halfUpDiv<int128_t>(a, divisor) * divisor);
  }
};

// Flink ROUND over floating-point types. The float overload follows Flink's
// sround(float, int): widen to double, round, narrow.
template <typename TExec>
struct FloatRoundFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      double& result,
      const double& a,
      const int32_t b = 0) {
    result = detail::decimalHalfUpRound(a, b);
  }

  FOLLY_ALWAYS_INLINE void call(
      float& result, const float& a, const int32_t b = 0) {
    result = static_cast<float>(detail::decimalHalfUpRound(a, b));
  }
};

// Flink ROUND over decimal(p, s): HALF_UP on the unscaled value, keeping
// the input scale. Flink's round type is applied by a cast in the gluten
// plan; at this layer the function stays same-typed.
template <typename TExec>
struct DecimalRoundFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // The framework detects this initialize overload by the one trailing
  // pointer per argument (see SimpleFunctionMetadata.h).
  template <typename A, typename B>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      const A* /*a*/,
      const B* /*b*/) {
    scale_ = getDecimalPrecisionScale(*inputTypes[0]).second;
  }

  template <typename R, typename A>
  FOLLY_ALWAYS_INLINE void call(
      R& out,
      const A& a,
      const int32_t b = 0) {
    const int drop = static_cast<int>(scale_) - b;
    if (drop <= 0) {
      out = a;
      return;
    }
    if (drop > 38) {
      out = 0;
      return;
    }
    const int128_t divisor = DecimalUtil::kPowersOfTen[drop];
    out = detail::halfUpDiv<int128_t>(a, divisor) * divisor;
  }

 private:
  int scale_;
};

} // namespace facebook::velox::functions::flinksql
