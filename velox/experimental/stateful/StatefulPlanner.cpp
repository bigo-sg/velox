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
#include "velox/core/PlanFragment.h"
#include "velox/exec/AssignUniqueId.h"
#include "velox/exec/CallbackSink.h"
#include "velox/exec/EnforceSingleRow.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Expand.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/GroupId.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashProbe.h"
#include "velox/exec/IndexLookupJoin.h"
#include "velox/exec/Limit.h"
#include "velox/exec/MarkDistinct.h"
#include "velox/exec/Merge.h"
#include "velox/exec/MergeJoin.h"
#include "velox/exec/NestedLoopJoinBuild.h"
#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/exec/OrderBy.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/exec/RowNumber.h"
#include "velox/exec/StreamingAggregation.h"
#include "velox/exec/TableScan.h"
#include "velox/exec/TableWriteMerge.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Task.h"
#include "velox/exec/TopN.h"
#include "velox/exec/TopNRowNumber.h"
#include "velox/exec/Unnest.h"
#include "velox/exec/Values.h"
#include "velox/exec/Window.h"
#include "velox/experimental/stateful/StatefulPlanner.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/WatermarkAssigner.h"

namespace facebook::velox::stateful {

// static
void StatefulPlanner::getAllNodesInOrder(
    std::shared_ptr<const core::PlanNode> planNode,
    std::vector<std::shared_ptr<const core::PlanNode>>& planNodes) {

  const auto& sources = planNode->sources();
  if (!sources.empty()) {
    for (int32_t i = 0; i < sources.size(); ++i) {
      getAllNodesInOrder(sources[i], planNodes);
    }
  }
  planNodes.push_back(planNode);
}

// static
void StatefulPlanner::plan(
    const core::PlanFragment& planFragment,
    exec::DriverCtx* ctx,
    std::vector<std::unique_ptr<exec::Operator>>& operators) {
  std::vector<std::shared_ptr<const core::PlanNode>> planNodes;
  getAllNodesInOrder(planFragment.planNode, planNodes);
  operators.reserve(planNodes.size());

  for (int32_t i = 0; i < planNodes.size(); i++) {
    // Id of the Operator being made. This is not the same as 'i'
    // because some PlanNodes may get fused.
    auto id = operators.size();
    auto planNode = planNodes[i];
    if (auto filterNode =
            std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
      if (i < planNodes.size() - 1) {
        auto next = planNodes[i + 1];
        if (auto projectNode =
                std::dynamic_pointer_cast<const core::ProjectNode>(next)) {
          operators.push_back(std::make_unique<exec::FilterProject>(
              id, ctx, filterNode, projectNode));
          i++;
          continue;
        }
      }
      operators.push_back(
          std::make_unique<exec::FilterProject>(id, ctx, filterNode, nullptr));
    } else if (
        auto projectNode =
            std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
      operators.push_back(
          std::make_unique<exec::FilterProject>(id, ctx, nullptr, projectNode));
    } else if (
        auto valuesNode =
            std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
      operators.push_back(std::make_unique<exec::Values>(id, ctx, valuesNode));
    } else if (
        auto tableScanNode =
            std::dynamic_pointer_cast<const core::TableScanNode>(planNode)) {
      operators.push_back(
          std::make_unique<exec::TableScan>(id, ctx, tableScanNode));
    } else if (
        auto tableWriteNode =
            std::dynamic_pointer_cast<const core::TableWriteNode>(planNode)) {
      operators.push_back(
          std::make_unique<exec::TableWriter>(id, ctx, tableWriteNode));
    } else if (
        auto tableWriteMergeNode =
            std::dynamic_pointer_cast<const core::TableWriteMergeNode>(
                planNode)) {
      operators.push_back(std::make_unique<exec::TableWriteMerge>(
          id, ctx, tableWriteMergeNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
      operators.push_back(std::make_unique<exec::HashProbe>(id, ctx, joinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<exec::NestedLoopJoinProbe>(id, ctx, joinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<exec::IndexLookupJoin>(id, ctx, joinNode));
    } else if (
        auto aggregationNode =
            std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
      if (aggregationNode->isPreGrouped()) {
        operators.push_back(std::make_unique<exec::StreamingAggregation>(
            id, ctx, aggregationNode));
      } else {
        operators.push_back(
            std::make_unique<exec::HashAggregation>(id, ctx, aggregationNode));
      }
    } else if (
        auto expandNode =
            std::dynamic_pointer_cast<const core::ExpandNode>(planNode)) {
      operators.push_back(std::make_unique<exec::Expand>(id, ctx, expandNode));
    } else if (
        auto groupIdNode =
            std::dynamic_pointer_cast<const core::GroupIdNode>(planNode)) {
      operators.push_back(
          std::make_unique<exec::GroupId>(id, ctx, groupIdNode));
    } else if (
        auto topNNode =
            std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
      operators.push_back(std::make_unique<exec::TopN>(id, ctx, topNNode));
    } else if (
        auto limitNode =
            std::dynamic_pointer_cast<const core::LimitNode>(planNode)) {
      operators.push_back(std::make_unique<exec::Limit>(id, ctx, limitNode));
    } else if (
        auto orderByNode =
            std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
      operators.push_back(
          std::make_unique<exec::OrderBy>(id, ctx, orderByNode));
    } else if (
        auto windowNode =
            std::dynamic_pointer_cast<const core::WindowNode>(planNode)) {
      operators.push_back(std::make_unique<exec::Window>(id, ctx, windowNode));
    } else if (
        auto rowNumberNode =
            std::dynamic_pointer_cast<const core::RowNumberNode>(planNode)) {
      operators.push_back(
          std::make_unique<exec::RowNumber>(id, ctx, rowNumberNode));
    } else if (
        auto topNRowNumberNode =
            std::dynamic_pointer_cast<const core::TopNRowNumberNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<exec::TopNRowNumber>(id, ctx, topNRowNumberNode));
    } else if (
        auto markDistinctNode =
            std::dynamic_pointer_cast<const core::MarkDistinctNode>(planNode)) {
      operators.push_back(
          std::make_unique<exec::MarkDistinct>(id, ctx, markDistinctNode));
    } else if (
        auto mergeJoin =
            std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
      auto mergeJoinOp = std::make_unique<exec::MergeJoin>(id, ctx, mergeJoin);
      ctx->task->createMergeJoinSource(ctx->splitGroupId, mergeJoin->id());
      operators.push_back(std::move(mergeJoinOp));
    } else if (
        auto unnest =
            std::dynamic_pointer_cast<const core::UnnestNode>(planNode)) {
      operators.push_back(std::make_unique<exec::Unnest>(id, ctx, unnest));
    } else if (
        auto enforceSingleRow =
            std::dynamic_pointer_cast<const core::EnforceSingleRowNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<exec::EnforceSingleRow>(id, ctx, enforceSingleRow));
    } else if (
        auto assignUniqueIdNode =
            std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(
                planNode)) {
      operators.push_back(std::make_unique<exec::AssignUniqueId>(
          id,
          ctx,
          assignUniqueIdNode,
          assignUniqueIdNode->taskUniqueId(),
          assignUniqueIdNode->uniqueIdCounter()));
    } else if (
      auto watermarkAssignerNode =
          std::dynamic_pointer_cast<const stateful::WatermarkAssignerNode>(planNode)) {
      operators.push_back(
          std::make_unique<stateful::WatermarkAssigner>(id, ctx, watermarkAssignerNode));
    } else {
      std::unique_ptr<exec::Operator> extended;
      extended = exec::Operator::fromPlanNode(ctx, id, planNode);
      VELOX_CHECK(extended, "Unsupported plan node: {}", planNode->toString());
      operators.push_back(std::move(extended));
    }
  }
}

} // namespace facebook::velox::stateful
