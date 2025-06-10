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
#include "velox/experimental/stateful/EmptyOperator.h"
#include "velox/experimental/stateful/StatefulPlanner.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamPartition.h"
#include "velox/experimental/stateful/StreamJoin.h"
#include "velox/experimental/stateful/StreamJoinOperator.h"
#include "velox/experimental/stateful/WatermarkAssigner.h"

namespace facebook::velox::stateful {

static int opId = 0;

// static
StatefulOperatorPtr StatefulPlanner::plan(
    const core::PlanFragment& planFragment,
    exec::DriverCtx* ctx) {
  return nodeToStatefulOperator(planFragment.planNode, ctx);
}

//static
StatefulOperatorPtr StatefulPlanner::nodeToStatefulOperator(
    const core::PlanNodePtr& planNode,
    exec::DriverCtx* ctx) {
  auto statefulNode =
      std::dynamic_pointer_cast<const StatefulPlanNode>(planNode);
  VELOX_CHECK(statefulNode, "Not stateful node: {}", planNode->toString());
  std::vector<StatefulOperatorPtr> targets;
  std::unique_ptr<exec::Operator> op = std::move(nodeToOperator(statefulNode->node(), ctx));
  for (auto target : statefulNode->targets()) {
    targets.push_back(std::move(nodeToStatefulOperator(target, ctx)));
  }
  if (auto watermarkAssignerNode =
      std::dynamic_pointer_cast<const WatermarkAssignerNode>(statefulNode->node())) {
    return std::make_unique<WatermarkAssigner>(
        std::move(op),
        std::move(targets),
        watermarkAssignerNode->idleTimeout(),
        watermarkAssignerNode->rowtimeFieldIndex(),
        watermarkAssignerNode->watermarkInterval());
  } else if (auto joinNode =
      std::dynamic_pointer_cast<const core::HashJoinNode>(statefulNode->node())) {
    VELOX_CHECK(joinNode->sources().size() == 2, "HashJoinNode should have 2 sources");
    std::unique_ptr<exec::Operator> left = std::move(nodeToOperator(joinNode->sources()[0], ctx));
    std::unique_ptr<exec::Operator> right = std::move(nodeToOperator(joinNode->sources()[1], ctx));
    return std::make_unique<StreamJoinOperator>(
        std::move(op),
        std::move(targets),
        std::move(left),
        std::move(right));
  } else if (auto partitionNode =
      std::dynamic_pointer_cast<const StreamPartitionNode>(statefulNode->node())) {
    VELOX_CHECK(targets.size() == 0, "StreamPartitionNode should have no targets");
    int numPartitions = partitionNode->numPartitions();
    return std::make_unique<StreamPartition>(
        std::move(op),
        partitionNode->partition()->partitionFunctionSpec(),
        numPartitions);
  }
  return std::make_unique<StatefulOperator>(std::move(op), std::move(targets));
}

//static
std::unique_ptr<exec::Operator> StatefulPlanner::nodeToOperator(
    const core::PlanNodePtr& planNode,
    exec::DriverCtx* ctx) {
  if (auto filterNode =
      std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
    if (planNode->sources().size() == 1) {
      auto next = planNode->sources()[0];
      if (auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(next)) {
        return std::make_unique<exec::FilterProject>(opId++, ctx, filterNode, projectNode);
      }
    }
    return std::make_unique<exec::FilterProject>(opId++, ctx, filterNode, nullptr);
  } else if (
      auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
    return std::make_unique<exec::FilterProject>(opId++, ctx, nullptr, projectNode);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const StreamJoinNode>(planNode)) {
    VELOX_CHECK(joinNode->sources().size() == 2, "StreamJoinNode should have 2 sources");
    std::unique_ptr<exec::Operator> left = std::move(nodeToOperator(joinNode->sources()[0], ctx));
    std::unique_ptr<exec::Operator> right = std::move(nodeToOperator(joinNode->sources()[1], ctx));
    std::unique_ptr<exec::Operator> build =
        std::make_unique<exec::NestedLoopJoinBuild>(opId++, ctx, joinNode->build());
    std::unique_ptr<exec::Operator> probe = std::move(nodeToOperator(joinNode->probe(), ctx));
    return std::make_unique<StreamJoin>(
        opId++,
        ctx,
        std::move(joinNode),
        std::move(left),
        std::move(right),
        std::move(build),
        std::move(probe));
  } else if (
      auto partitionNode =
        std::dynamic_pointer_cast<const StreamPartitionNode>(planNode)) {
    return std::make_unique<EmptyOperator>(opId++, ctx, partitionNode->partition());
  } else if (
      auto valuesNode =
          std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
    return std::make_unique<exec::Values>(opId++, ctx, valuesNode);
  } else if (
      auto tableScanNode =
          std::dynamic_pointer_cast<const core::TableScanNode>(planNode)) {
    return std::make_unique<exec::TableScan>(opId++, ctx, tableScanNode);
  } else if (
      auto tableWriteNode =
          std::dynamic_pointer_cast<const core::TableWriteNode>(planNode)) {
      return std::make_unique<exec::TableWriter>(opId++, ctx, tableWriteNode);
  } else if (
      auto tableWriteMergeNode =
          std::dynamic_pointer_cast<const core::TableWriteMergeNode>(planNode)) {
    return std::make_unique<exec::TableWriteMerge>(opId++, ctx, tableWriteMergeNode);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    return std::make_unique<exec::HashProbe>(opId++, ctx, joinNode);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(planNode)) {
    return std::make_unique<exec::NestedLoopJoinProbe>(opId++, ctx, joinNode);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(planNode)) {
    return std::make_unique<exec::IndexLookupJoin>(opId++, ctx, joinNode);
  } else if (
      auto aggregationNode =
          std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
    if (aggregationNode->isPreGrouped()) {
      return std::make_unique<exec::StreamingAggregation>(opId++, ctx, aggregationNode);
    } else {
      return std::make_unique<exec::HashAggregation>(opId++, ctx, aggregationNode);
    }
  } else if (
      auto expandNode =
          std::dynamic_pointer_cast<const core::ExpandNode>(planNode)) {
    return std::make_unique<exec::Expand>(opId++, ctx, expandNode);
  } else if (
      auto groupIdNode =
          std::dynamic_pointer_cast<const core::GroupIdNode>(planNode)) {
    return std::make_unique<exec::GroupId>(opId++, ctx, groupIdNode);
  } else if (
      auto topNNode =
          std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
      return std::make_unique<exec::TopN>(opId++, ctx, topNNode);
  } else if (
      auto limitNode =
          std::dynamic_pointer_cast<const core::LimitNode>(planNode)) {
    return std::make_unique<exec::Limit>(opId++, ctx, limitNode);
  } else if (
      auto orderByNode =
          std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
    return std::make_unique<exec::OrderBy>(opId++, ctx, orderByNode);
  } else if (
      auto windowNode =
          std::dynamic_pointer_cast<const core::WindowNode>(planNode)) {
    return std::make_unique<exec::Window>(opId++, ctx, windowNode);
  } else if (
      auto rowNumberNode =
          std::dynamic_pointer_cast<const core::RowNumberNode>(planNode)) {
    return std::make_unique<exec::RowNumber>(opId++, ctx, rowNumberNode);
  } else if (
      auto topNRowNumberNode =
          std::dynamic_pointer_cast<const core::TopNRowNumberNode>(planNode)) {
    return std::make_unique<exec::TopNRowNumber>(opId++, ctx, topNRowNumberNode);
  } else if (
      auto markDistinctNode =
          std::dynamic_pointer_cast<const core::MarkDistinctNode>(planNode)) {
    return std::make_unique<exec::MarkDistinct>(opId++, ctx, markDistinctNode);
  } else if (
      auto mergeJoin =
          std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
    auto mergeJoinOp = std::make_unique<exec::MergeJoin>(opId++, ctx, mergeJoin);
    ctx->task->createMergeJoinSource(ctx->splitGroupId, mergeJoin->id());
    return std::move(mergeJoinOp);
  } else if (
      auto unnest =
          std::dynamic_pointer_cast<const core::UnnestNode>(planNode)) {
    return std::make_unique<exec::Unnest>(opId++, ctx, unnest);
  } else if (
      auto enforceSingleRow =
          std::dynamic_pointer_cast<const core::EnforceSingleRowNode>(planNode)) {
    return std::make_unique<exec::EnforceSingleRow>(opId++, ctx, enforceSingleRow);
  } else if (
      auto assignUniqueIdNode =
          std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(planNode)) {
    return std::make_unique<exec::AssignUniqueId>(
        opId++,
        ctx,
        assignUniqueIdNode,
        assignUniqueIdNode->taskUniqueId(),
        assignUniqueIdNode->uniqueIdCounter());
  } else if (
      auto watermarkAssignerNode =
          std::dynamic_pointer_cast<const stateful::WatermarkAssignerNode>(planNode)) {
    return std::make_unique<exec::FilterProject>(opId++, ctx, nullptr, watermarkAssignerNode->project());
  } else {
    std::unique_ptr<exec::Operator> extended;
    extended = exec::Operator::fromPlanNode(ctx, opId++, planNode);
    VELOX_CHECK(extended, "Unsupported plan node: {}", planNode->toString());
    return extended;
  }
}

} // namespace facebook::velox::stateful
