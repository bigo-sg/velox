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
#include "velox/functions/flinksql/ArithmeticFunctions.h"

#include <fmt/format.h>

#include "velox/functions/Registerer.h"

namespace facebook::velox::functions::flinksql {

namespace {

// Flink's floor/ceil keep the input precision and drop the scale to 0
// (DecimalScale0TypeStrategy): decimal(p, s) -> decimal(p, 0).
std::vector<exec::SignatureVariable> decimalScaleZeroConstraints() {
  return {
      exec::SignatureVariable(
          P2::name(),
          fmt::format("{}", P1::name()),
          exec::ParameterType::kIntegerParameter),
      exec::SignatureVariable(
          S2::name(), "0", exec::ParameterType::kIntegerParameter),
  };
}

} // namespace

void registerArithmeticFunctions(const std::string& prefix) {
  // ABS over BIGINT is intentionally exact in same-typed int64: Flink
  // evaluates BIGINT operands through its widening fallback as
  // (long) SqlFunctionUtils.abs((double) x), which silently corrupts
  // for |x| >= 2^53 (e.g. abs(-2^53 - 1) yields 2^53 in Flink). The
  // same-typed registrations here keep the exact result, and the most
  // negative integer wraps to itself on both sides. Integral
  // floor/ceil are identities either way.
  // ABS: same type in and out.
  for (const std::string& name : {prefix + "abs"}) {
    registerFunction<AbsFunction, int8_t, int8_t>({name});
    registerFunction<AbsFunction, int16_t, int16_t>({name});
    registerFunction<AbsFunction, int32_t, int32_t>({name});
    registerFunction<AbsFunction, int64_t, int64_t>({name});
    registerFunction<AbsFunction, float, float>({name});
    registerFunction<AbsFunction, double, double>({name});
    registerFunction<
        DecimalAbsFunction,
        ShortDecimal<P1, S1>,
        ShortDecimal<P1, S1>>({name});
    registerFunction<
        DecimalAbsFunction,
        LongDecimal<P1, S1>,
        LongDecimal<P1, S1>>({name});
  }

  // Unary minus (-x): same type in and out.
  for (const std::string& name : {prefix + "negate"}) {
    registerFunction<NegateFunction, int8_t, int8_t>({name});
    registerFunction<NegateFunction, int16_t, int16_t>({name});
    registerFunction<NegateFunction, int32_t, int32_t>({name});
    registerFunction<NegateFunction, int64_t, int64_t>({name});
    registerFunction<NegateFunction, float, float>({name});
    registerFunction<NegateFunction, double, double>({name});
    registerFunction<
        NegateFunction,
        ShortDecimal<P1, S1>,
        ShortDecimal<P1, S1>>({name});
    registerFunction<NegateFunction, LongDecimal<P1, S1>, LongDecimal<P1, S1>>(
        {name});
  }

  // FLOOR/CEIL over numerics: same type in and out.
  for (const std::string& name : {prefix + "floor"}) {
    registerFunction<FloorFunction, int8_t, int8_t>({name});
    registerFunction<FloorFunction, int16_t, int16_t>({name});
    registerFunction<FloorFunction, int32_t, int32_t>({name});
    registerFunction<FloorFunction, int64_t, int64_t>({name});
    registerFunction<FloorFunction, float, float>({name});
    registerFunction<FloorFunction, double, double>({name});
  }
  for (const std::string& name : {prefix + "ceil", prefix + "ceiling"}) {
    registerFunction<CeilFunction, int8_t, int8_t>({name});
    registerFunction<CeilFunction, int16_t, int16_t>({name});
    registerFunction<CeilFunction, int32_t, int32_t>({name});
    registerFunction<CeilFunction, int64_t, int64_t>({name});
    registerFunction<CeilFunction, float, float>({name});
    registerFunction<CeilFunction, double, double>({name});
  }

  // FLOOR/CEIL over decimals: decimal(p, s) -> decimal(p, 0) via signature
  // constraints (the sparksql DecimalCeil pattern); the precision is bound
  // by the caller's type, so no cross-kind bindings are needed.
  const auto scaleZero = decimalScaleZeroConstraints();
  registerFunction<
      DecimalFloorFunction,
      ShortDecimal<P2, S2>,
      ShortDecimal<P1, S1>>({prefix + "floor"}, scaleZero);
  registerFunction<
      DecimalFloorFunction,
      LongDecimal<P2, S2>,
      LongDecimal<P1, S1>>({prefix + "floor"}, scaleZero);
  registerFunction<
      DecimalCeilFunction,
      ShortDecimal<P2, S2>,
      ShortDecimal<P1, S1>>({prefix + "ceil", prefix + "ceiling"}, scaleZero);
  registerFunction<
      DecimalCeilFunction,
      LongDecimal<P2, S2>,
      LongDecimal<P1, S1>>({prefix + "ceil", prefix + "ceiling"}, scaleZero);

  // ROUND over fixed-width numerics: one-arg form (b = 0) and (T, int) form.
  for (const std::string& name : {prefix + "round"}) {
    registerFunction<RoundFunction, int8_t, int8_t>({name});
    registerFunction<RoundFunction, int16_t, int16_t>({name});
    registerFunction<RoundFunction, int32_t, int32_t>({name});
    registerFunction<RoundFunction, int64_t, int64_t>({name});
    registerFunction<RoundFunction, int8_t, int8_t, int32_t>({name});
    registerFunction<RoundFunction, int16_t, int16_t, int32_t>({name});
    registerFunction<RoundFunction, int32_t, int32_t, int32_t>({name});
    registerFunction<RoundFunction, int64_t, int64_t, int32_t>({name});

    // Floating-point rounding goes through the decimal representation.
    registerFunction<FloatRoundFunction, double, double>({name});
    registerFunction<FloatRoundFunction, float, float>({name});
    registerFunction<FloatRoundFunction, double, double, int32_t>({name});
    registerFunction<FloatRoundFunction, float, float, int32_t>({name});

    // Decimal rounding stays same-typed; the gluten plan narrows the result
    // to Flink's round type (scale depends on the b argument value). The
    // one-argument form covers ROUND(x) (b defaults to 0).
    registerFunction<
        DecimalRoundFunction,
        ShortDecimal<P1, S1>,
        ShortDecimal<P1, S1>>({name});
    registerFunction<
        DecimalRoundFunction,
        LongDecimal<P1, S1>,
        LongDecimal<P1, S1>>({name});
    registerFunction<
        DecimalRoundFunction,
        ShortDecimal<P1, S1>,
        ShortDecimal<P1, S1>,
        int32_t>({name});
    registerFunction<
        DecimalRoundFunction,
        LongDecimal<P1, S1>,
        LongDecimal<P1, S1>,
        int32_t>({name});
  }
}

} // namespace facebook::velox::functions::flinksql
