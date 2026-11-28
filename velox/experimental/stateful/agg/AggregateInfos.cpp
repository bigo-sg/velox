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
#include "velox/experimental/stateful/agg/AggregateInfos.h"

#include "velox/core/Expressions.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"

namespace facebook::velox::stateful {

namespace {

// Resolves the signature-bound final result type of an aggregate call the
// same way Aggregate::intermediateType resolves the intermediate type: the
// first registered signature whose variables bind to argTypes decides.
TypePtr resolveFinalResultType(
    const std::string& name,
    const std::vector<TypePtr>& argTypes) {
  const auto signatures = exec::getAggregateFunctionSignatures(name);
  VELOX_USER_CHECK(
      signatures.has_value(), "Aggregate function not registered: {}", name);
  for (const auto& signature : signatures.value()) {
    exec::SignatureBinder binder(*signature, argTypes);
    if (binder.tryBind()) {
      auto type = binder.tryResolveReturnType();
      VELOX_USER_CHECK(
          type,
          "Cannot resolve the final result type of aggregate function {}",
          exec::toString(name, argTypes));
      return type;
    }
  }
  VELOX_USER_FAIL(
      "Aggregate function signature is not supported: {}",
      exec::toString(name, argTypes));
}

} // namespace

std::vector<exec::AggregateInfo> makeAggregateInfos(
    const std::vector<core::AggregationNode::Aggregate>& aggregates,
    const RowTypePtr& inputType,
    const RowTypePtr& outputType,
    column_index_t numLeadingColumns,
    const core::QueryConfig& queryConfig,
    memory::MemoryPool* pool) {
  std::vector<exec::AggregateInfo> infos;
  infos.reserve(aggregates.size());
  for (auto i = 0; i < aggregates.size(); ++i) {
    const auto& aggregate = aggregates[i];
    exec::AggregateInfo info;
    for (const auto& arg : aggregate.call->inputs()) {
      if (auto field =
              dynamic_cast<const core::FieldAccessTypedExpr*>(arg.get())) {
        info.inputs.push_back(inputType->getChildIdx(field->name()));
        info.constantInputs.push_back(nullptr);
      } else if (
          auto constant =
              dynamic_cast<const core::ConstantTypedExpr*>(arg.get())) {
        info.inputs.push_back(kConstantChannel);
        info.constantInputs.push_back(constant->toConstantVector(pool));
      } else {
        VELOX_NYI("Unsupported aggregate argument: {}", arg->toString());
      }
    }
    if (aggregate.mask) {
      auto maskField =
          dynamic_cast<const core::FieldAccessTypedExpr*>(aggregate.mask.get());
      VELOX_USER_CHECK(
          maskField != nullptr,
          "Only column-reference aggregation masks are supported, got: {}",
          aggregate.mask->toString());
      info.mask = inputType->getChildIdx(maskField->name());
      VELOX_USER_CHECK_EQ(
          inputType->childAt(*info.mask)->kind(),
          TypeKind::BOOLEAN,
          "Aggregation mask column '{}' must be BOOLEAN",
          maskField->name());
    }
    VELOX_CHECK(
        aggregate.sortingKeys.empty(),
        "Sorted aggregates are not supported yet");
    VELOX_CHECK(
        !aggregate.distinct, "Distinct aggregates are not supported yet");
    info.distinct = false;
    info.intermediateType = exec::Aggregate::intermediateType(
        aggregate.call->name(), aggregate.rawInputTypes);
    // The declared output column must carry exactly the type the
    // function's signature produces; a mismatch would otherwise surface
    // later at value extraction without naming the function.
    const auto& declaredResult = outputType->childAt(numLeadingColumns + i);
    const auto signatureResult =
        resolveFinalResultType(aggregate.call->name(), aggregate.rawInputTypes);
    VELOX_USER_CHECK(
        *signatureResult == *declaredResult,
        "Aggregate function {} produces {}, but the declared output column "
        "carries {}",
        exec::toString(aggregate.call->name(), aggregate.rawInputTypes),
        signatureResult->toString(),
        declaredResult->toString());
    info.function = exec::Aggregate::create(
        aggregate.call->name(),
        core::AggregationNode::Step::kSingle,
        aggregate.rawInputTypes,
        outputType->childAt(numLeadingColumns + i),
        queryConfig);
    info.output = numLeadingColumns + i;
    infos.emplace_back(std::move(info));
  }
  return infos;
}

} // namespace facebook::velox::stateful
