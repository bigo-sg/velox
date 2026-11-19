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

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/flinksql/aggregates/CountSumAggregates.h"
#include "velox/functions/lib/aggregates/SumAggregateBase.h"

namespace facebook::velox::aggregate::flinksql {

namespace {

using exec::AggregateFunctionSignatureBuilder;
using exec::registerAggregateFunction;
using functions::aggregate::SumAggregateBase;

/// sum: Flink SumAggFunction - null when nothing non-null accumulated, the
/// running sum otherwise. Flink keeps the argument's type (sum(int) is int)
/// where the prestosql baseline widens the integral family to BIGINT, so the
/// flinksql registration preserves it: accumulation widens to int64 / double
/// (the shared sum base's checked-free arithmetic) and the final value casts
/// into the plan's result type, which the factory pins to the input type.
void registerSum(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto* inputType :
       {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("bigint")
                             .argumentType(inputType)
                             .build());
  }
  signatures.push_back(AggregateFunctionSignatureBuilder()
                           .returnType("real")
                           .intermediateType("double")
                           .argumentType("real")
                           .build());
  signatures.push_back(AggregateFunctionSignatureBuilder()
                           .returnType("double")
                           .intermediateType("double")
                           .argumentType("double")
                           .build());
  registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        const auto& inputType = argTypes[0];
        // The plan declares the Flink result type, which is the input type;
        // anything else is a plan / registration mismatch.
        VELOX_CHECK_EQ(
            resultType->kind(),
            inputType->kind(),
            "flinksql {} result type {} must match the input type {}",
            name,
            resultType->toString(),
            inputType->toString());
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<
                SumAggregateBase<int8_t, int64_t, int8_t, false>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<
                SumAggregateBase<int16_t, int64_t, int16_t, false>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<
                SumAggregateBase<int32_t, int64_t, int32_t, false>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<
                SumAggregateBase<int64_t, int64_t, int64_t, false>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<
                SumAggregateBase<float, double, float, false>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<
                SumAggregateBase<double, double, double, false>>(resultType);
          default:
            VELOX_UNSUPPORTED(
                "Unsupported {} input type {}", name, inputType->toString());
        }
      },
      {false /*orderSensitive*/, false /*companionFunction*/},
      withCompanionFunctions,
      overwrite);
}

/// count: Flink CountAggFunction (non-null values) / the no-argument form
/// (all rows, COUNT(*)). BIGINT, never null, zero for nothing accumulated.
void registerCount(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("bigint")
          .build(),
      AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("bigint")
          .argumentType("T")
          .build(),
  };
  registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        VELOX_CHECK_EQ(
            resultType->kind(),
            TypeKind::BIGINT,
            "flinksql {} result type {} must be BIGINT",
            name,
            resultType->toString());
        return std::make_unique<CountAggregate>();
      },
      {false /*orderSensitive*/, false /*companionFunction*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerSum(prefix + "sum", withCompanionFunctions, overwrite);
}

void registerCountAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerCount(prefix + "count", withCompanionFunctions, overwrite);
}

void registerAggregateFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerSumAggregate(prefix, withCompanionFunctions, overwrite);
  registerCountAggregate(prefix, withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::aggregate::flinksql
