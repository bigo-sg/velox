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
#include "velox/experimental/stateful/window/GroupWindowAggsHandler.h"

#include <numeric>
#include <utility>

#include "velox/common/memory/ByteStream.h"
#include "velox/experimental/stateful/GroupWindowAggregator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/RowContainer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::stateful {

namespace {
// Mirrors exec::toAggregateInfo for the single-step (kSingle) raw-input
// aggregation of the group window agg: field accesses resolve to input
// channels, constants materialize from the pool, masks / sorting keys /
// distinct flags are rejected fail-loud (unsupported window-agg semantics),
// and the registry resolves each function by its bare name (the flinksql
// registrations overwrite the prestosql/sparksql baseline in the Flink
// preset).

std::vector<exec::AggregateInfo> makeWindowAggregateInfos(
    const std::vector<core::AggregationNode::Aggregate>& aggregates,
    const RowTypePtr& inputType,
    const RowTypePtr& outputType,
    uint32_t numKeys,
    const core::QueryConfig& queryConfig,
    memory::MemoryPool* pool) {
  const auto numAggregates = aggregates.size();
  // The handler node carries the window aggregation's full output type:
  // key columns, aggregate columns, then the window property columns the
  // operator fills from the window. The aggregate result column of call i
  // sits at numKeys + i; anything from numKeys + numAggregates on is window
  // property territory and not validated here.
  VELOX_CHECK_GE(
      outputType->size(),
      numKeys + numAggregates,
      "group-window-agg output schema must carry keys and aggregates (then window properties)");

  std::vector<exec::AggregateInfo> infos;
  infos.reserve(numAggregates);
  for (size_t i = 0; i < numAggregates; ++i) {
    const auto& aggregate = aggregates[i];
    exec::AggregateInfo info;
    auto& channels = info.inputs;
    auto& constants = info.constantInputs;
    for (const auto& arg : aggregate.call->inputs()) {
      if (auto field =
              dynamic_cast<const core::FieldAccessTypedExpr*>(arg.get())) {
        channels.push_back(inputType->getChildIdx(field->name()));
        constants.push_back(nullptr);
      } else if (
          auto constant =
              dynamic_cast<const core::ConstantTypedExpr*>(arg.get())) {
        channels.push_back(kConstantChannel);
        constants.push_back(constant->toConstantVector(pool));
      } else {
        VELOX_UNSUPPORTED(
            "group-window-agg arguments must be field accesses or constants: {}",
            arg->toString());
      }
    }

    VELOX_CHECK(
        aggregate.mask == nullptr,
        "group-window-agg aggregate masks are not supported: {}",
        aggregate.call->toString());
    VELOX_CHECK(
        aggregate.sortingKeys.empty(),
        "group-window-agg sorting keys are not supported: {}",
        aggregate.call->toString());
    VELOX_CHECK(
        !aggregate.distinct,
        "group-window-agg DISTINCT aggregates are not supported: {}",
        aggregate.call->toString());

    info.distinct = false;
    info.intermediateType = exec::Aggregate::intermediateType(
        aggregate.call->name(), aggregate.rawInputTypes);

    // The aggregates sit behind the key columns in the output; the window
    // property columns (SESSION_START / SESSION_END) follow them and are
    // filled by the operator from the window, not by the functions.
    info.output = static_cast<column_index_t>(numKeys + i);
    const auto& aggResultType = outputType->childAt(info.output);

    info.function = exec::Aggregate::create(
        aggregate.call->name(),
        core::AggregationNode::Step::kSingle,
        aggregate.rawInputTypes,
        aggResultType,
        queryConfig);
    infos.push_back(std::move(info));
  }
  return infos;
}

} // namespace

GroupWindowAggsHandler::GroupWindowAggsHandler(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& groupAggNode)
    : exec::Operator(
          driverCtx,
          groupAggNode->outputType(),
          operatorId,
          groupAggNode->id(),
          "GroupWindowAggsHandler"),
      handlerNode_(
          std::dynamic_pointer_cast<const GroupWindowAggsHandlerNode>(
              groupAggNode)) {
  VELOX_CHECK_NOT_NULL(
      handlerNode_,
      "GroupWindowAggsHandler must be constructed from a GroupWindowAggsHandlerNode");
}

void GroupWindowAggsHandler::addInput(RowVectorPtr input) {
  // No-op: input is managed via StatefulOperator API (accumulate/merge).
}

RowVectorPtr GroupWindowAggsHandler::getOutput() {
  // No-op: input is managed via StatefulOperator API (accumulate/merge).
  return nullptr;
}

bool GroupWindowAggsHandler::isFinished() {
  return false;
}

void GroupWindowAggsHandler::initializeWindowAggregates(
    const RowTypePtr& inputType,
    uint32_t numKeys,
    const core::QueryConfig& queryConfig) {
  inputType_ = inputType;
  numKeys_ = numKeys;
  VELOX_CHECK_NOT_NULL(
      handlerNode_,
      "GroupWindowAggsHandler has no plan node to resolve aggregates from");
  const auto& aggregates = handlerNode_->aggregates();
  VELOX_CHECK(
      !aggregates.empty(),
      "GroupWindowAggsHandler requires at least one aggregate call");

  aggregates_ = makeWindowAggregateInfos(
      aggregates, inputType, outputType_, numKeys, queryConfig, pool());

  // Cross-check the declared output column types against the resolved
  // functions' result types: the emission path extracts the functions'
  // values straight into the output columns.
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& declared = outputType_->childAt(numKeys + i);
    const auto& resolved = aggregates_[i].function->resultType();
    VELOX_CHECK_EQ(
        declared->kind(),
        resolved->kind(),
        "group-window-agg output column {} declares {} but the aggregate function '{}' produces {}",
        numKeys + i,
        declared->toString(),
        aggregates[i].call->name(),
        resolved->toString());
  }
}

std::vector<VectorPtr> GroupWindowAggsHandler::aggregateArgs(
    const exec::AggregateInfo& info,
    const RowVectorPtr& input) const {
  std::vector<VectorPtr> args;
  args.reserve(info.inputs.size());
  for (size_t i = 0; i < info.inputs.size(); ++i) {
    if (info.inputs[i] == kConstantChannel) {
      args.push_back(info.constantInputs[i]);
    } else {
      args.push_back(input->childAt(info.inputs[i]));
    }
  }
  return args;
}

void GroupWindowAggsHandler::initializeRows(char** rows, size_t numRows) {
  std::vector<vector_size_t> indices(numRows);
  std::iota(indices.begin(), indices.end(), 0);
  const auto indexRange =
      folly::Range<const vector_size_t*>(indices.data(), indices.size());
  for (const auto& aggregate : aggregates_) {
    aggregate.function->initializeNewGroups(rows, indexRange);
  }
}

void GroupWindowAggsHandler::mergeRow(
    char* row,
    const std::vector<VectorPtr>& columns) {
  // Each column carries exactly one row of intermediate input (the state's
  // restore contract); fold it into the target row's accumulators.
  const SelectivityVector rows(1);
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i].function->addSingleGroupIntermediateResults(
        row, rows, {columns[i]}, false);
  }
}

void GroupWindowAggsHandler::mergeAccumulators(char* target, char* source) {
  char* sourceRow = const_cast<char*>(source);
  const SelectivityVector rows(1);
  for (const auto& aggregate : aggregates_) {
    // The same extract-then-merge sequence the state's restore path uses:
    // pull the source row's intermediate representation, then fold it into
    // the target's accumulators.
    VectorPtr intermediate =
        BaseVector::create(aggregate.intermediateType, 1, pool());
    aggregate.function->extractAccumulators(&sourceRow, 1, &intermediate);
    aggregate.function->addSingleGroupIntermediateResults(
        target, rows, {intermediate}, false);
  }
}

void GroupWindowAggsHandler::extractValues(
    const std::vector<char*>& rows,
    std::vector<VectorPtr>& outValues) {
  outValues.clear();
  outValues.reserve(aggregates_.size());
  const auto numRows = static_cast<int32_t>(rows.size());
  for (const auto& aggregate : aggregates_) {
    VectorPtr values =
        BaseVector::create(aggregate.function->resultType(), numRows, pool());
    aggregate.function->extractValues(
        const_cast<char**>(rows.data()), numRows, &values);
    outValues.push_back(std::move(values));
  }
}

void GroupWindowAggsHandler::setOwner(GroupWindowAggregator* owner) {
  owner_ = owner;
}

std::vector<std::string> GroupWindowAggsHandler::snapshotState(
    int64_t checkpointId) {
  VELOX_CHECK_NOT_NULL(
      owner_,
      "GroupWindowAggsHandler has no owning GroupWindowAggregator to snapshot");
  return owner_->snapshotKeyedState();
}

void GroupWindowAggsHandler::restoreState(
    const std::vector<std::string>& checkpointRecords) {
  VELOX_CHECK_NOT_NULL(
      owner_,
      "GroupWindowAggsHandler has no owning GroupWindowAggregator to restore");
  owner_->restoreKeyedState(checkpointRecords);
}

} // namespace facebook::velox::stateful
