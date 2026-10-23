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
#include "velox/functions/flinksql/aggregates/AverageAggregate.h"

#include "velox/common/base/CheckedArithmetic.h"
#include "velox/functions/lib/aggregates/AverageAggregateBase.h"
#include "velox/functions/lib/aggregates/DecimalAggregate.h"
#include "velox/type/DecimalUtil.h"

namespace facebook::velox::functions::aggregate::flinksql {
namespace {

/// Average over integral inputs with Flink semantics: the sum accumulates in
/// BIGINT with wrap-around addition (uint64_t produces the same bit pattern
/// as Java's long addition without signed-overflow UB), and the final value
/// is a truncating integer division, e.g. avg(7, 2) over BIGINT is 3.
template <typename TInput, typename TResult>
class AverageAggregate
    : public AverageAggregateBase<TInput, uint64_t, TResult> {
 public:
  explicit AverageAggregate(TypePtr resultType)
      : AverageAggregateBase<TInput, uint64_t, TResult>(std::move(resultType)) {
  }

  // The base's extractAccumulators casts the intermediate sum column to
  // FlatVector<TAccumulator>, but a BIGINT column is a FlatVector<int64_t>,
  // which dynamic_cast cannot see as FlatVector<uint64_t>. The uint64_t sum
  // and the BIGINT column share the bit pattern, so extract through
  // int64_t and cast bit-preservingly.
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto sumVector = rowVector->childAt(0)->asFlatVector<int64_t>();
    auto countVector = rowVector->childAt(1)->asFlatVector<int64_t>();

    rowVector->resize(numGroups);
    sumVector->resize(numGroups);
    countVector->resize(numGroups);
    uint64_t* rawNulls = this->getRawNulls(rowVector);

    int64_t* rawCounts = countVector->mutableRawValues();
    int64_t* rawSums = sumVector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (this->isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        this->clearNull(rawNulls, i);
        auto* sumCount = this->accumulator(group);
        rawCounts[i] = sumCount->count;
        rawSums[i] = static_cast<int64_t>(sumCount->sum);
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<FlatVector<TResult>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = this->getRawNulls(vector);

    TResult* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (this->isNull(group)) {
        vector->setNull(i, true);
      } else {
        this->clearNull(rawNulls, i);
        auto* sumCount = this->accumulator(group);
        rawValues[i] = static_cast<TResult>(
            static_cast<int64_t>(sumCount->sum) / sumCount->count);
      }
    }
  }
};

/// Average over DECIMAL(p, s) with Flink semantics: the sum stays at the
/// input scale while the result type is DECIMAL(38, max(6, s)), so the final
/// division rescales the fraction by 10^k, k = max(6, s) - s, with HALF_UP
/// rounding, matching Flink's decimal division.
template <typename TInput>
class DecimalAverageAggregate : public DecimalAggregate<int128_t, TInput> {
 public:
  explicit DecimalAverageAggregate(TypePtr resultType, uint8_t resultRescale)
      : DecimalAggregate<int128_t, TInput>(std::move(resultType)),
        resultRescale_(resultRescale) {}

  int128_t computeFinalValue(functions::aggregate::LongDecimalWithOverflowState*
                                 accumulator) override {
    const int128_t sum = accumulator->sum;
    const int64_t count = accumulator->count;
    VELOX_USER_CHECK_GT(
        count, 0, "Cannot produce the final value of avg without any input");

    // Decompose trueSum = overflow * 2^127 + sum (see
    // DecimalUtil::addWithOverflow) into quotient and remainder over count.
    // With no carry the native 128-bit division is exact; with a carry the
    // 2^127 part is split separately, mirroring DecimalUtil::computeAverage,
    // so neither division overflows 128 bits.
    int128_t quotient;
    int128_t remainder;
    if (accumulator->overflow == 0) {
      quotient = sum / count;
      remainder = sum % count;
    } else {
      uint128_t quotientMul;
      const uint128_t remainderMul =
          DecimalUtil::divideWithRoundUp<uint128_t, uint128_t, int64_t>(
              quotientMul, DecimalUtil::kOverflowMultiplier, count, true, 0, 0);
      int128_t quotientSum;
      const int128_t remainderSum =
          DecimalUtil::divideWithRoundUp<int128_t, int128_t, int64_t>(
              quotientSum, sum, count, true, 0, 0);
      quotient =
          checkedMultiply<int128_t>(
              static_cast<int128_t>(quotientMul), accumulator->overflow) +
          quotientSum;
      remainder =
          checkedMultiply<int128_t>(
              static_cast<int128_t>(remainderMul), accumulator->overflow) +
          remainderSum;
    }

    // trueSum / count = (quotient + extraQuotient) + fractionRemainder /
    // count. Rescale to the result scale with HALF_UP rounding on the
    // fraction: result = (quotient + extraQuotient) * 10^k +
    // round(fractionRemainder * 10^k / count). The dividend of the second
    // division is bounded by count, so rescaling it never overflows for the
    // k = max(6, s) - s <= 6 produced by Flink's avg result type.
    const int128_t extraQuotient = remainder / count;
    int128_t scaledRemainder;
    DecimalUtil::divideWithRoundUp<int128_t, int128_t, int64_t>(
        scaledRemainder, remainder % count, count, false, resultRescale_, 0);
    return checkedMultiply<int128_t>(
               quotient + extraQuotient,
               DecimalUtil::kPowersOfTen[resultRescale_]) +
        scaledRemainder;
  }

 private:
  /// Extra fractional digits of the result type over the input type.
  uint8_t resultRescale_;
};

uint8_t decimalScaleOf(const TypePtr& type) {
  VELOX_CHECK(type->isDecimal());
  if (type->isShortDecimal()) {
    return static_cast<const ShortDecimalType&>(*type).scale();
  }
  return static_cast<const LongDecimalType&>(*type).scale();
}

void checkCountIsBigint(const TypePtr& type, const std::string& name) {
  VELOX_USER_CHECK_EQ(
      type->kind(),
      TypeKind::BIGINT,
      "Input type for count in final {} must be bigint",
      name);
}

} // namespace

/// Count is BIGINT. Following Flink's AvgAggFunction, the sum and result
/// types depend on the input type:
///       INPUT TYPE    |     SUM           |     AVG
///   ------------------|-------------------|------------------------
///     TINYINT         |     BIGINT        |    TINYINT
///     SMALLINT        |     BIGINT        |    SMALLINT
///     INTEGER         |     BIGINT        |    INTEGER
///     BIGINT          |     BIGINT        |    BIGINT
///     REAL            |     DOUBLE        |    REAL
///     DOUBLE          |     DOUBLE        |    DOUBLE
///     DECIMAL(p, s)   |     DECIMAL       |    DECIMAL(38, max(6, s))
/// The integral sums wrap around like Java's long, the integral results use
/// truncating division, and REAL results round the double quotient to
/// FLOAT, all matching Flink's AvgAggFunction.
exec::AggregateRegistrationResult registerAverage(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("row(bigint,bigint)")
                             .argumentType(inputType)
                             .build());
  }

  for (const auto& inputType : {"real", "double"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("row(double,bigint)")
                             .argumentType(inputType)
                             .build());
  }

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .integerVariable("r_scale", "max(6, a_scale)")
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .intermediateType("varbinary")
                           .returnType("DECIMAL(38, r_scale)")
                           .build());

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        const auto& inputType = argTypes[0];
        if (exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case TypeKind::TINYINT:
              return std::make_unique<AverageAggregate<int8_t, int8_t>>(
                  resultType);
            case TypeKind::SMALLINT:
              return std::make_unique<AverageAggregate<int16_t, int16_t>>(
                  resultType);
            case TypeKind::INTEGER:
              return std::make_unique<AverageAggregate<int32_t, int32_t>>(
                  resultType);
            case TypeKind::BIGINT: {
              if (inputType->isShortDecimal()) {
                return std::make_unique<DecimalAverageAggregate<int64_t>>(
                    resultType,
                    decimalScaleOf(resultType) - decimalScaleOf(inputType));
              }
              return std::make_unique<AverageAggregate<int64_t, int64_t>>(
                  resultType);
            }
            case TypeKind::HUGEINT: {
              if (inputType->isLongDecimal()) {
                return std::make_unique<DecimalAverageAggregate<int128_t>>(
                    resultType,
                    decimalScaleOf(resultType) - decimalScaleOf(inputType));
              }
              VELOX_NYI();
            }
            case TypeKind::REAL:
              return std::make_unique<
                  AverageAggregateBase<float, double, float>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<
                  AverageAggregateBase<double, double, double>>(resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          // Final step over intermediate results. The input is either a
          // row(sum, count) for the numeric inputs or varbinary for decimal.
          if (inputType->isVarbinary()) {
            // The serialized decimal sum stays at the input scale, which is
            // only recoverable from the result type when the result scale
            // exceeds 6, because result scale = max(6, input scale).
            VELOX_USER_CHECK_GT(
                decimalScaleOf(resultType),
                6,
                "Cannot infer the input scale of {} from result type {}",
                name,
                resultType->toString());
            return std::make_unique<DecimalAverageAggregate<int64_t>>(
                resultType, 0);
          }
          VELOX_USER_CHECK(
              inputType->isRow(),
              "Input type for final {} must be row type or varbinary type",
              name);
          checkCountIsBigint(inputType->childAt(1), name);
          const auto& sumType = inputType->childAt(0);
          if (sumType->kind() == TypeKind::BIGINT) {
            switch (resultType->kind()) {
              case TypeKind::TINYINT:
                return std::make_unique<AverageAggregate<int64_t, int8_t>>(
                    resultType);
              case TypeKind::SMALLINT:
                return std::make_unique<AverageAggregate<int64_t, int16_t>>(
                    resultType);
              case TypeKind::INTEGER:
                return std::make_unique<AverageAggregate<int64_t, int32_t>>(
                    resultType);
              case TypeKind::BIGINT:
                return std::make_unique<AverageAggregate<int64_t, int64_t>>(
                    resultType);
              default:
                VELOX_FAIL(
                    "Unsupported result type for final aggregation of {}: {}",
                    name,
                    resultType->kindName());
            }
          }
          VELOX_USER_CHECK_EQ(
              sumType->kind(),
              TypeKind::DOUBLE,
              "Input type for sum in final {} must be bigint or double",
              name);
          if (resultType->kind() == TypeKind::REAL) {
            return std::make_unique<AverageAggregateBase<float, double, float>>(
                resultType);
          }
          return std::make_unique<AverageAggregateBase<double, double, double>>(
              resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::functions::aggregate::flinksql
