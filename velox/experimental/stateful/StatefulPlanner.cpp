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
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/LocalWindowAggregator.h"
#include "velox/experimental/stateful/StatefulPlanner.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamPartition.h"
#include "velox/experimental/stateful/StreamJoin.h"
#include "velox/experimental/stateful/WatermarkAssigner.h"
#include "velox/experimental/stateful/WindowAggregator.h"
#include "velox/experimental/stateful/WindowJoin.h"
#include "velox/experimental/stateful/GroupWindowAggregator.h"
#include "velox/experimental/stateful/window/GroupWindowAggsHandler.h"
#include "velox/experimental/stateful/StreamKeyedOperator.h"
#include "velox/experimental/stateful/rank/RowTimeDeduplicateRanker.h"
#include "velox/experimental/stateful/rank/AppendOnlyTopNRanker.h"
#include "velox/experimental/stateful/agg/AggsHandleFunction.h"
#include "velox/experimental/stateful/agg/GroupAggregator.h"
#include <algorithm>

namespace facebook::velox::stateful {

static std::atomic<int> opId = 0;

static int nextOperatorId() {
    return opId.fetch_add(1);
}

// static
StatefulOperatorPtr StatefulPlanner::plan(
    const core::PlanFragment& planFragment,
    exec::DriverCtx* ctx,
    StateBackend* stateBackend) {
  // return nodeToStatefulOperator(planFragment.planNode, ctx, stateBackend);
  StatefulPlanner planner(ctx, stateBackend);
  return planner.transformStatefulOperators(planFragment.planNode);
}

#define CHECK_NODE_TYPE(TYPE, node) std::dynamic_pointer_cast<const TYPE>(node->node()) != nullptr

StatefulOperatorPtr StatefulPlanner::transformStatefulOperators(const core::PlanNodePtr& planNode) {
    auto statefulNode = std::dynamic_pointer_cast<const StatefulPlanNode>(planNode);
    VELOX_CHECK(statefulNode, "Not stateful node: {}", planNode->toString());
    StatefulOperatorPtr result;
    if (std::dynamic_pointer_cast<const WatermarkAssignerNode>(statefulNode->node()) != nullptr) {
        result = transformWatermarkAssignerOperator(*statefulNode);
    } else if (std::dynamic_pointer_cast<const StreamPartitionNode>(statefulNode->node()) != nullptr) {
        result = transformStreamPartitionOperator(*statefulNode);
    } else if (std::dynamic_pointer_cast<const StreamJoinNode>(statefulNode->node()) != nullptr) {
        result = transformStreamJoinOperator(*statefulNode);
    } else if (std::dynamic_pointer_cast<const StreamWindowJoinNode>(statefulNode->node()) != nullptr) {
        result = transformStreamWindowJoinOperator(*statefulNode);
    } else if (std::dynamic_pointer_cast<const StreamWindowAggregationNode>(statefulNode->node()) != nullptr) {
        result = transformStreamWindowAggregationOperator(*statefulNode);
    } else if (std::dynamic_pointer_cast<const GroupWindowAggregationNode>(statefulNode->node()) != nullptr) {
        result = transformGroupWindowAggregationOperator(*statefulNode);
    } else if (std::dynamic_pointer_cast<const StreamRankNode>(statefulNode->node()) != nullptr) {
        result = transformStreamRankOperator(*statefulNode);
    } else if (std::dynamic_pointer_cast<const GroupAggregationNode>(statefulNode->node()) != nullptr) {
        result = transformGroupAggregationOperator(*statefulNode);
    } else {
        result = transformGenericOperator(*statefulNode);
    }
    VELOX_CHECK(result, "Failed to build operator for node: {}", planNode->toString());
    return result;
}

std::vector<StatefulOperatorPtr> StatefulPlanner::transformStatefulOperators(const std::vector<core::PlanNodePtr>& targets) {
    std::vector<StatefulOperatorPtr> operators;
    operators.resize(targets.size());
    std::transform(targets.begin(), targets.end(), operators.begin(), [this](const core::PlanNodePtr& target) {
        return transformStatefulOperators(target);
    });
    return operators;
}

StatefulOperatorPtr StatefulPlanner::transformWatermarkAssignerOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());

    auto watermarkAssignerNode = std::dynamic_pointer_cast<const WatermarkAssignerNode>(planNode.node());

    auto op = std::make_unique<exec::FilterProject>(
        nextOperatorId(),
        ctx_,
        nullptr,
        watermarkAssignerNode->project());

    return std::make_unique<WatermarkAssigner>(
        std::move(op),
        std::move(targets),
        watermarkAssignerNode->idleTimeout(),
        watermarkAssignerNode->rowtimeFieldIndex(),
        watermarkAssignerNode->watermarkInterval());
}

StatefulOperatorPtr StatefulPlanner::transformStreamPartitionOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());
    VELOX_CHECK(targets.empty(), "StreamPartitionNode should have no targets");

    auto partitionNode = std::dynamic_pointer_cast<const StreamPartitionNode>(planNode.node());
    VELOX_CHECK(partitionNode, "Failed to cast to StreamPartitionNode");

    auto op = std::make_unique<EmptyOperator>(
        nextOperatorId(),
        ctx_,
        partitionNode->partition());

    return std::make_unique<StreamPartition>(
        std::move(op),
        partitionNode->partition()->partitionFunctionSpec(),
        partitionNode->numPartitions());
}

StatefulOperatorPtr StatefulPlanner::transformStreamJoinOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());

    auto joinNode = std::dynamic_pointer_cast<const StreamJoinNode>(planNode.node());
    VELOX_CHECK(joinNode, "Failed to cast to StreamJoinNode");
    VELOX_CHECK(joinNode->sources().size() == 2, "StreamJoinNode should have 2 sources");

    std::unique_ptr<exec::Operator> left = transformOperator(joinNode->sources()[0]);
    std::unique_ptr<exec::Operator> right = transformOperator(joinNode->sources()[1]);
    std::unique_ptr<exec::Operator> probe = transformOperator(joinNode->probe());

    std::unique_ptr<KeySelector> leftKeySelector =
        std::make_unique<KeySelector>(
            joinNode->leftPartFuncSpec()->create(INT_MAX, false),
            probe->pool(),
            joinNode->numPartitions());
    std::unique_ptr<KeySelector> rightKeySelector =
        std::make_unique<KeySelector>(
            joinNode->rightPartFuncSpec()->create(INT_MAX, false),
            probe->pool(),
            joinNode->numPartitions());

    return std::make_unique<StreamJoin>(
        std::move(left),
        std::move(right),
        std::move(leftKeySelector),
        std::move(rightKeySelector),
        std::move(probe),
        std::move(targets));
}

StatefulOperatorPtr StatefulPlanner::transformStreamWindowJoinOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());

    auto joinNode = std::dynamic_pointer_cast<const StreamWindowJoinNode>(planNode.node());
    VELOX_CHECK(joinNode, "Failed to cast to StreamWindowJoinNode");
    VELOX_CHECK(joinNode->sources().size() == 2, "StreamWindowJoinNode should have 2 sources");

    std::unique_ptr<exec::Operator> left = transformOperator(joinNode->sources()[0]);
    std::unique_ptr<exec::Operator> right = transformOperator(joinNode->sources()[1]);
    std::unique_ptr<exec::Operator> probe = transformOperator(joinNode->probe());

    std::unique_ptr<KeySelector> leftKeySelector =
        std::make_unique<KeySelector>(
            joinNode->leftPartFuncSpec()->create(INT_MAX, false),
            probe->pool(),
            joinNode->numPartitions());
    std::unique_ptr<KeySelector> rightKeySelector =
        std::make_unique<KeySelector>(
            joinNode->rightPartFuncSpec()->create(INT_MAX, false),
            probe->pool(),
            joinNode->numPartitions());

    return std::make_unique<WindowJoin>(
        std::move(left),
        std::move(right),
        std::move(leftKeySelector),
        std::move(rightKeySelector),
        std::move(probe),
        std::move(targets),
        joinNode->leftWindowEndIndex(),
        joinNode->rightWindowEndIndex());
}

StatefulOperatorPtr StatefulPlanner::transformStreamWindowAggregationOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());

    auto windowAggNode = std::dynamic_pointer_cast<const StreamWindowAggregationNode>(planNode.node());
    VELOX_CHECK(windowAggNode, "Failed to cast to StreamWindowAggregationNode");

    auto op = transformOperator(windowAggNode->aggregation());

    std::unique_ptr<KeySelector> keySelector =
        std::make_unique<KeySelector>(
            windowAggNode->keySelectorSpec()->create(INT_MAX, true),
            op->pool());
    std::unique_ptr<KeySelector> sliceAssigner =
        std::make_unique<KeySelector>(
            windowAggNode->sliceAssignerSpec()->create(INT_MAX, true),
            op->pool());

    if (windowAggNode->isLocalAgg()) {
        return std::make_unique<LocalWindowAggregator>(
            std::move(op),
            std::move(targets),
            std::move(keySelector),
            std::move(sliceAssigner),
            windowAggNode->windowInterval(),
            windowAggNode->useDayLightSaving(),
            windowAggNode->outputType());
    } else {
        auto localAggregator = transformOperator(windowAggNode->localAgg());
        std::unique_ptr<SliceAssigner> globalSliceAssigner =
            std::make_unique<SliceAssigner>(
                std::move(sliceAssigner),
                windowAggNode->size(),
                windowAggNode->step(),
                windowAggNode->offset(),
                windowAggNode->windowType(),
                windowAggNode->rowtimeIndex());
        return std::make_unique<WindowAggregator>(
            std::move(localAggregator),
            std::move(op),
            std::move(targets),
            std::move(keySelector),
            std::move(globalSliceAssigner),
            windowAggNode->windowInterval(),
            windowAggNode->useDayLightSaving());
    }
}

StatefulOperatorPtr StatefulPlanner::transformGroupWindowAggregationOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());

    auto windowAggNode = std::dynamic_pointer_cast<const GroupWindowAggregationNode>(planNode.node());
    VELOX_CHECK(windowAggNode, "Failed to cast to GroupWindowAggregationNode");

    VELOX_MEM_LOG(ERROR)<< "transformGroupWindowAggregationOperator:" << planNode.toString();
    auto op = transformOperator(windowAggNode->aggregation());

    std::unique_ptr<KeySelector> keySelector =
        std::make_unique<KeySelector>(
            windowAggNode->keySelectorSpec()->create(INT_MAX, true),
            op->pool());
    std::unique_ptr<KeySelector> sliceAssigner =
        std::make_unique<KeySelector>(
            windowAggNode->sliceAssignerSpec()->create(INT_MAX, true),
            op->pool());
    std::unique_ptr<SliceAssigner> windowAssigner =
        std::make_unique<SliceAssigner>(
            std::move(sliceAssigner),
            0,
            0,
            0,
            windowAggNode->windowType(),
            windowAggNode->rowtimeIndex());

    return std::make_unique<GroupWindowAggregator>(
        std::unique_ptr<GroupWindowAggsHandler>(dynamic_cast<GroupWindowAggsHandler*>(op.release())),
        // TODO: support window parameters
        std::make_unique<SessionWindowAssigner>(10, windowAggNode->isEventTime()),
        std::move(targets),
        std::move(keySelector),
        std::move(windowAssigner),
        windowAggNode->allowedLateness(),
        windowAggNode->produceUpdates(),
        windowAggNode->rowtimeIndex(),
        windowAggNode->isEventTime());
}

StatefulOperatorPtr StatefulPlanner::transformStreamRankOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());

    auto rankNode = std::dynamic_pointer_cast<const StreamRankNode>(planNode.node());
    VELOX_CHECK(rankNode, "Failed to cast to StreamRankNode");

    auto op = transformOperator(rankNode->ranker());

    std::unique_ptr<KeySelector> keySelector =
        std::make_unique<KeySelector>(
            rankNode->keySelectorSpec()->create(INT_MAX, true),
            op->pool());

    return std::make_unique<StreamKeyedOperator>(
        std::move(op),
        std::move(keySelector),
        std::move(targets));
}

StatefulOperatorPtr StatefulPlanner::transformGroupAggregationOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());

    auto aggNode = std::dynamic_pointer_cast<const GroupAggregationNode>(planNode.node());
    VELOX_CHECK(aggNode, "Failed to cast to GroupAggregationNode");

    auto op = transformOperator(aggNode->aggregation());

    std::unique_ptr<KeySelector> keySelector =
        std::make_unique<KeySelector>(
            aggNode->keySelectorSpec()->create(INT_MAX, true),
            op->pool());

    return std::make_unique<StreamKeyedOperator>(
        std::move(op),
        std::move(keySelector),
        std::move(targets));
}

std::unique_ptr<exec::Operator> StatefulPlanner::transformOperator(const core::PlanNodePtr& planNode) {
    if (auto filterNode = std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
        if (planNode->sources().size() == 1) {
            auto next = planNode->sources()[0];
            if (auto projectNode = std::dynamic_pointer_cast<const core::ProjectNode>(next)) {
                return std::make_unique<exec::FilterProject>(
                    nextOperatorId(),
                    ctx_,
                    filterNode,
                    projectNode);
            }
        }
        return std::make_unique<exec::FilterProject>(
            nextOperatorId(),
            ctx_,
            filterNode,
            nullptr);
    } else if (auto projectNode = std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
        std::shared_ptr<const core::FilterNode> filterNode = nullptr;
        const std::vector<core::PlanNodePtr>& sources = projectNode->sources();
        if (sources.size() == 1) {
            filterNode = std::dynamic_pointer_cast<const core::FilterNode>(sources[0]);
        }
        return std::make_unique<exec::FilterProject>(nextOperatorId(), ctx_, filterNode, projectNode);
    } else if (auto valuesNode = std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
        return std::make_unique<exec::Values>(nextOperatorId(), ctx_, valuesNode);
    } else if (auto tableScanNode = std::dynamic_pointer_cast<const core::TableScanNode>(planNode)) {
        return std::make_unique<exec::TableScan>(nextOperatorId(), ctx_, tableScanNode);
    } else if (auto tableWriteNode = std::dynamic_pointer_cast<const core::TableWriteNode>(planNode)) {
        return std::make_unique<exec::TableWriter>(nextOperatorId(), ctx_, tableWriteNode);
    } else if (auto tableWriteMergeNode = std::dynamic_pointer_cast<const core::TableWriteMergeNode>(planNode)) {
        return std::make_unique<exec::TableWriteMerge>(nextOperatorId(), ctx_, tableWriteMergeNode);
    } else if (auto joinNode = std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
        return std::make_unique<exec::HashProbe>(nextOperatorId(), ctx_, joinNode);
    } else if (auto joinNode = std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(planNode)) {
        return std::make_unique<exec::NestedLoopJoinProbe>(nextOperatorId(), ctx_, joinNode);
    } else if (auto joinNode = std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(planNode)) {
        return std::make_unique<exec::IndexLookupJoin>(nextOperatorId(), ctx_, joinNode);
    } else if (auto aggregationNode = std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
        if (aggregationNode->isPreGrouped()) {
            return std::make_unique<exec::StreamingAggregation>(nextOperatorId(), ctx_, aggregationNode);
        } else {
            return std::make_unique<exec::HashAggregation>(nextOperatorId(), ctx_, aggregationNode);
        }
    } else if (auto expandNode = std::dynamic_pointer_cast<const core::ExpandNode>(planNode)) {
        return std::make_unique<exec::Expand>(nextOperatorId(), ctx_, expandNode);
    } else if (auto groupIdNode = std::dynamic_pointer_cast<const core::GroupIdNode>(planNode)) {
        return std::make_unique<exec::GroupId>(nextOperatorId(), ctx_, groupIdNode);
    } else if (auto topNNode = std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
        return std::make_unique<exec::TopN>(nextOperatorId(), ctx_, topNNode);
    } else if (auto limitNode = std::dynamic_pointer_cast<const core::LimitNode>(planNode)) {
        return std::make_unique<exec::Limit>(nextOperatorId(), ctx_, limitNode);
    } else if (auto orderByNode = std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
        return std::make_unique<exec::OrderBy>(nextOperatorId(), ctx_, orderByNode);
    } else if (auto windowNode = std::dynamic_pointer_cast<const core::WindowNode>(planNode)) {
        return std::make_unique<exec::Window>(nextOperatorId(), ctx_, windowNode);
    } else if (auto rowNumberNode = std::dynamic_pointer_cast<const core::RowNumberNode>(planNode)) {
        return std::make_unique<exec::RowNumber>(nextOperatorId(), ctx_, rowNumberNode);
    } else if (auto topNRowNumberNode = std::dynamic_pointer_cast<const core::TopNRowNumberNode>(planNode)) {
        return std::make_unique<exec::TopNRowNumber>(nextOperatorId(), ctx_, topNRowNumberNode);
    } else if (auto markDistinctNode = std::dynamic_pointer_cast<const core::MarkDistinctNode>(planNode)) {
        return std::make_unique<exec::MarkDistinct>(nextOperatorId(), ctx_, markDistinctNode);
    } else if (auto mergeJoin = std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
        auto mergeJoinOp = std::make_unique<exec::MergeJoin>(nextOperatorId(), ctx_, mergeJoin);
        ctx_->task->createMergeJoinSource(ctx_->splitGroupId, mergeJoin->id());
        return std::move(mergeJoinOp);
    } else if (auto unnest = std::dynamic_pointer_cast<const core::UnnestNode>(planNode)) {
        return std::make_unique<exec::Unnest>(nextOperatorId(), ctx_, unnest);
    } else if (auto enforceSingleRow = std::dynamic_pointer_cast<const core::EnforceSingleRowNode>(planNode)) {
        return std::make_unique<exec::EnforceSingleRow>(nextOperatorId(), ctx_, enforceSingleRow);
    } else if (auto assignUniqueIdNode = std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(planNode)) {
        return std::make_unique<exec::AssignUniqueId>(
            nextOperatorId(),
            ctx_,
            assignUniqueIdNode,
            assignUniqueIdNode->taskUniqueId(),
            assignUniqueIdNode->uniqueIdCounter());
    } else if (auto aggsHandlerNode = std::dynamic_pointer_cast<const GroupWindowAggsHandlerNode>(planNode)) {
        return std::make_unique<GroupWindowAggsHandler>(nextOperatorId(), ctx_, aggsHandlerNode);
    } else if (auto aggsHandlerNode = std::dynamic_pointer_cast<const GroupAggsHandlerNode>(planNode)) {
        return std::make_unique<GroupAggregator>(
            nextOperatorId(),
            ctx_,
            aggsHandlerNode,
            std::make_unique<AggsHandleFunction>(), // TODO: not complete yet
            0, // stateRetentionTime: default to 0
            aggsHandlerNode->generateUpdateBefore());
    } else if (auto deduplicateNode = std::dynamic_pointer_cast<const DeduplicateNode>(planNode)) {
        return std::make_unique<RowTimeDeduplicateRanker>(
            nextOperatorId(),
            ctx_,
            deduplicateNode,
            deduplicateNode->minRetentionTime(),
            deduplicateNode->rowtimeIndex(),
            deduplicateNode->generateUpdateBefore(),
            deduplicateNode->generateInsert(),
            deduplicateNode->keepLastRow());
    } else if (auto topNNode = std::dynamic_pointer_cast<const StreamTopNNode>(planNode)) {
        auto op = transformOperator(topNNode->topN());
        std::shared_ptr<KeySelector> sortKeySelector =
            std::make_shared<KeySelector>(
                topNNode->sortKeySelectorSpec()->create(INT_MAX, true),
                op->pool());
        return std::make_unique<AppendOnlyTopNRanker>(
            nextOperatorId(),
            ctx_,
            topNNode,
            std::move(op),
            sortKeySelector,
            topNNode->generateUpdateBefore(),
            topNNode->outputRankNumber(),
            topNNode->cacheSize());
    }
    std::unique_ptr<exec::Operator> extended;
    extended = exec::Operator::fromPlanNode(ctx_, nextOperatorId(), planNode);
    if (!extended) {
        VELOX_MEM_LOG(ERROR)<< "Failed to create operator for plan node:" << process::StackTrace().toString();
    }
    VELOX_CHECK(extended, "Unsupported plan node: {}", planNode->toString());
    return extended;
}

StatefulOperatorPtr StatefulPlanner::transformGenericOperator(const StatefulPlanNode& planNode) {
    std::vector<StatefulOperatorPtr> targets = transformStatefulOperators(planNode.targets());
    std::unique_ptr<exec::Operator> op = transformOperator(planNode.node());
    return std::make_unique<StatefulOperator>(
        std::move(op),
        std::move(targets));
}

//static
StatefulOperatorPtr StatefulPlanner::nodeToStatefulOperator(
    const core::PlanNodePtr& planNode,
    exec::DriverCtx* ctx,
    StateBackend* stateBackend) {
  auto statefulNode =
      std::dynamic_pointer_cast<const StatefulPlanNode>(planNode);
  VELOX_CHECK(statefulNode, "Not stateful node: {}", planNode->toString());
  std::vector<StatefulOperatorPtr> targets;
  std::unique_ptr<exec::Operator> op = std::move(nodeToOperator(statefulNode->node(), ctx));
  for (auto target : statefulNode->targets()) {
    targets.push_back(std::move(nodeToStatefulOperator(target, ctx, stateBackend)));
  }
  if (auto watermarkAssignerNode =
      std::dynamic_pointer_cast<const WatermarkAssignerNode>(statefulNode->node())) {
    return std::make_unique<WatermarkAssigner>(
        std::move(op),
        std::move(targets),
        watermarkAssignerNode->idleTimeout(),
        watermarkAssignerNode->rowtimeFieldIndex(),
        watermarkAssignerNode->watermarkInterval());
  } else if (auto partitionNode =
      std::dynamic_pointer_cast<const StreamPartitionNode>(statefulNode->node())) {
    VELOX_CHECK(targets.size() == 0, "StreamPartitionNode should have no targets");
    int numPartitions = partitionNode->numPartitions();
    return std::make_unique<StreamPartition>(
        std::move(op),
        partitionNode->partition()->partitionFunctionSpec(),
        numPartitions);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const StreamJoinNode>(statefulNode->node())) {
    VELOX_CHECK(joinNode->sources().size() == 2, "StreamJoinNode should have 2 sources");
    std::unique_ptr<exec::Operator> left = std::move(nodeToOperator(joinNode->sources()[0], ctx));
    std::unique_ptr<exec::Operator> right = std::move(nodeToOperator(joinNode->sources()[1], ctx));
    std::unique_ptr<KeySelector> leftKeySelector =
        std::make_unique<KeySelector>(
            std::move(joinNode->leftPartFuncSpec()->create(INT_MAX, false)),
            op->pool(),
            joinNode->numPartitions());
    std::unique_ptr<KeySelector> rightKeySelector =
        std::make_unique<KeySelector>(
            std::move(joinNode->rightPartFuncSpec()->create(INT_MAX, false)),
            op->pool(),
            joinNode->numPartitions());
    return std::make_unique<StreamJoin>(
        std::move(left),
        std::move(right),
        std::move(leftKeySelector),
        std::move(rightKeySelector),
        std::move(op),
        std::move(targets));
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const StreamWindowJoinNode>(statefulNode->node())) {
    VELOX_CHECK(joinNode->sources().size() == 2, "StreamWindowJoinNode should have 2 sources");
    std::unique_ptr<exec::Operator> left = std::move(nodeToOperator(joinNode->sources()[0], ctx));
    std::unique_ptr<exec::Operator> right = std::move(nodeToOperator(joinNode->sources()[1], ctx));
    std::unique_ptr<KeySelector> leftKeySelector =
        std::make_unique<KeySelector>(
            std::move(joinNode->leftPartFuncSpec()->create(INT_MAX, false)),
            op->pool(),
            joinNode->numPartitions());
    std::unique_ptr<KeySelector> rightKeySelector =
        std::make_unique<KeySelector>(
            std::move(joinNode->rightPartFuncSpec()->create(INT_MAX, false)),
            op->pool(),
            joinNode->numPartitions());
    return std::make_unique<WindowJoin>(
        std::move(left),
        std::move(right),
        std::move(leftKeySelector),
        std::move(rightKeySelector),
        std::move(op),
        std::move(targets),
        joinNode->leftWindowEndIndex(),
        joinNode->rightWindowEndIndex());
  } else if (
      auto windowAggNode =
          std::dynamic_pointer_cast<const StreamWindowAggregationNode>(statefulNode->node())) {
    std::unique_ptr<KeySelector> keySelector =
        std::make_unique<KeySelector>(
            std::move(windowAggNode->keySelectorSpec()->create(INT_MAX, true)),
            op->pool());
    std::unique_ptr<KeySelector> sliceAssigner =
        std::make_unique<KeySelector>(
            std::move(windowAggNode->sliceAssignerSpec()->create(INT_MAX, true)),
            op->pool());
    if (windowAggNode->isLocalAgg()) {
      return std::make_unique<LocalWindowAggregator>(
          std::move(op),
          std::move(targets),
          std::move(keySelector),
          std::move(sliceAssigner),
          windowAggNode->windowInterval(),
          windowAggNode->useDayLightSaving(),
          windowAggNode->outputType());
    } else {
      auto localAggregator = nodeToOperator(windowAggNode->localAgg(), ctx);
      std::unique_ptr<SliceAssigner> globalSliceAssigner =
          std::make_unique<SliceAssigner>(
              std::move(sliceAssigner),
              windowAggNode->size(),
              windowAggNode->step(),
              windowAggNode->offset(),
              windowAggNode->windowType(),
              windowAggNode->rowtimeIndex());
      return std::make_unique<WindowAggregator>(
          std::move(localAggregator),
          std::move(op),
          std::move(targets),
          std::move(keySelector),
          std::move(globalSliceAssigner),
          windowAggNode->windowInterval(),
          windowAggNode->useDayLightSaving());
    }
  } else if (
      auto windowAggNode =
          std::dynamic_pointer_cast<const GroupWindowAggregationNode>(statefulNode->node())) {
    std::unique_ptr<KeySelector> keySelector =
        std::make_unique<KeySelector>(
            std::move(windowAggNode->keySelectorSpec()->create(INT_MAX, true)),
            op->pool());
    std::unique_ptr<KeySelector> sliceAssigner =
        std::make_unique<KeySelector>(
            std::move(windowAggNode->sliceAssignerSpec()->create(INT_MAX, true)),
            op->pool());
    std::unique_ptr<SliceAssigner> windowAssigner =
        std::make_unique<SliceAssigner>(
            std::move(sliceAssigner),
            0,
            0,
            0,
            windowAggNode->windowType(),
            windowAggNode->rowtimeIndex());
    return std::make_unique<GroupWindowAggregator>(
        std::unique_ptr<GroupWindowAggsHandler>(dynamic_cast<GroupWindowAggsHandler*>(op.release())),
        // TODO: support window parameters
        std::make_unique<SessionWindowAssigner>(10, windowAggNode->isEventTime()),
        std::move(targets),
        std::move(keySelector),
        std::move(windowAssigner),
        windowAggNode->allowedLateness(),
        windowAggNode->produceUpdates(),
        windowAggNode->rowtimeIndex(),
        windowAggNode->isEventTime());
  } else if (
      auto rankNode =
          std::dynamic_pointer_cast<const StreamRankNode>(statefulNode->node())) {
    std::unique_ptr<KeySelector> keySelector =
        std::make_unique<KeySelector>(
            std::move(rankNode->keySelectorSpec()->create(INT_MAX, true)),
            op->pool());
    return std::make_unique<StreamKeyedOperator>(
        std::move(op),
        std::move(keySelector),
        std::move(targets));
  } else if (
      auto aggNode =
          std::dynamic_pointer_cast<const GroupAggregationNode>(statefulNode->node())) {
    std::unique_ptr<KeySelector> keySelector =
        std::make_unique<KeySelector>(
            std::move(aggNode->keySelectorSpec()->create(INT_MAX, true)),
            op->pool());
    return std::make_unique<StreamKeyedOperator>(
        std::move(op),
        std::move(keySelector),
        std::move(targets));
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
        return std::make_unique<exec::FilterProject>(
            opId.fetch_add(1),
            ctx,
            filterNode,
            projectNode);
      }
    }
    return std::make_unique<exec::FilterProject>(opId.fetch_add(1), ctx, filterNode, nullptr);
  } else if (
      auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
    std::shared_ptr<const core::FilterNode> filterNode = nullptr;
    const std::vector<core::PlanNodePtr>& sources = projectNode->sources();
    if (sources.size() == 1) {
        filterNode = std::dynamic_pointer_cast<const core::FilterNode>(sources[0]);
    }
    return std::make_unique<exec::FilterProject>(opId.fetch_add(1), ctx, filterNode, projectNode);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const StreamJoinNode>(planNode)) {
    return nodeToOperator(joinNode->probe(), ctx);
  } else if (
      auto partitionNode =
        std::dynamic_pointer_cast<const StreamPartitionNode>(planNode)) {
    return std::make_unique<EmptyOperator>(opId.fetch_add(1), ctx, partitionNode->partition());
  } else if (
      auto valuesNode =
          std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
    return std::make_unique<exec::Values>(opId.fetch_add(1), ctx, valuesNode);
  } else if (
      auto tableScanNode =
          std::dynamic_pointer_cast<const core::TableScanNode>(planNode)) {
    return std::make_unique<exec::TableScan>(opId.fetch_add(1), ctx, tableScanNode);
  } else if (
      auto tableWriteNode =
          std::dynamic_pointer_cast<const core::TableWriteNode>(planNode)) {
      return std::make_unique<exec::TableWriter>(opId.fetch_add(1), ctx, tableWriteNode);
  } else if (
      auto tableWriteMergeNode =
          std::dynamic_pointer_cast<const core::TableWriteMergeNode>(planNode)) {
    return std::make_unique<exec::TableWriteMerge>(opId.fetch_add(1), ctx, tableWriteMergeNode);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    return std::make_unique<exec::HashProbe>(opId.fetch_add(1), ctx, joinNode);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(planNode)) {
    return std::make_unique<exec::NestedLoopJoinProbe>(opId.fetch_add(1), ctx, joinNode);
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(planNode)) {
    return std::make_unique<exec::IndexLookupJoin>(opId.fetch_add(1), ctx, joinNode);
  } else if (
      auto aggregationNode =
          std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
    if (aggregationNode->isPreGrouped()) {
      return std::make_unique<exec::StreamingAggregation>(opId.fetch_add(1), ctx, aggregationNode);
    } else {
      return std::make_unique<exec::HashAggregation>(opId.fetch_add(1), ctx, aggregationNode);
    }
  } else if (
      auto expandNode =
          std::dynamic_pointer_cast<const core::ExpandNode>(planNode)) {
    return std::make_unique<exec::Expand>(opId.fetch_add(1), ctx, expandNode);
  } else if (
      auto groupIdNode =
          std::dynamic_pointer_cast<const core::GroupIdNode>(planNode)) {
    return std::make_unique<exec::GroupId>(opId.fetch_add(1), ctx, groupIdNode);
  } else if (
      auto topNNode =
          std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
      return std::make_unique<exec::TopN>(opId.fetch_add(1), ctx, topNNode);
  } else if (
      auto limitNode =
          std::dynamic_pointer_cast<const core::LimitNode>(planNode)) {
    return std::make_unique<exec::Limit>(opId.fetch_add(1), ctx, limitNode);
  } else if (
      auto orderByNode =
          std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
    return std::make_unique<exec::OrderBy>(opId.fetch_add(1), ctx, orderByNode);
  } else if (
      auto windowNode =
          std::dynamic_pointer_cast<const core::WindowNode>(planNode)) {
    return std::make_unique<exec::Window>(opId.fetch_add(1), ctx, windowNode);
  } else if (
      auto rowNumberNode =
          std::dynamic_pointer_cast<const core::RowNumberNode>(planNode)) {
    return std::make_unique<exec::RowNumber>(opId.fetch_add(1), ctx, rowNumberNode);
  } else if (
      auto topNRowNumberNode =
          std::dynamic_pointer_cast<const core::TopNRowNumberNode>(planNode)) {
    return std::make_unique<exec::TopNRowNumber>(opId.fetch_add(1), ctx, topNRowNumberNode);
  } else if (
      auto markDistinctNode =
          std::dynamic_pointer_cast<const core::MarkDistinctNode>(planNode)) {
    return std::make_unique<exec::MarkDistinct>(opId.fetch_add(1), ctx, markDistinctNode);
  } else if (
      auto mergeJoin =
          std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
    auto mergeJoinOp = std::make_unique<exec::MergeJoin>(opId.fetch_add(1), ctx, mergeJoin);
    ctx->task->createMergeJoinSource(ctx->splitGroupId, mergeJoin->id());
    return std::move(mergeJoinOp);
  } else if (
      auto unnest =
          std::dynamic_pointer_cast<const core::UnnestNode>(planNode)) {
    return std::make_unique<exec::Unnest>(opId.fetch_add(1), ctx, unnest);
  } else if (
      auto enforceSingleRow =
          std::dynamic_pointer_cast<const core::EnforceSingleRowNode>(planNode)) {
    return std::make_unique<exec::EnforceSingleRow>(opId.fetch_add(1), ctx, enforceSingleRow);
  } else if (
      auto assignUniqueIdNode =
          std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(planNode)) {
    return std::make_unique<exec::AssignUniqueId>(
        opId.fetch_add(1),
        ctx,
        assignUniqueIdNode,
        assignUniqueIdNode->taskUniqueId(),
        assignUniqueIdNode->uniqueIdCounter());
  } else if (
      auto watermarkAssignerNode =
          std::dynamic_pointer_cast<const stateful::WatermarkAssignerNode>(planNode)) {
    return std::make_unique<exec::FilterProject>(
        opId.fetch_add(1),
        ctx,
        nullptr,
        watermarkAssignerNode->project());
  } else if (
      auto joinNode =
          std::dynamic_pointer_cast<const StreamWindowJoinNode>(planNode)) {
    return nodeToOperator(joinNode->probe(), ctx);
  } else if (
      auto windowAggNode =
          std::dynamic_pointer_cast<const StreamWindowAggregationNode>(planNode)) {
    return nodeToOperator(windowAggNode->aggregation(), ctx);
  } else if (
      auto windowAggNode =
          std::dynamic_pointer_cast<const GroupWindowAggregationNode>(planNode)) {
    return nodeToOperator(windowAggNode->aggregation(), ctx);
  } else if (
      auto aggsHandlerNode =
          std::dynamic_pointer_cast<const GroupWindowAggsHandlerNode>(planNode)) {
    return std::make_unique<GroupWindowAggsHandler>(opId.fetch_add(1), ctx, aggsHandlerNode);
  } else if (
      auto deduplicateNode =
          std::dynamic_pointer_cast<const DeduplicateNode>(planNode)) {
    return std::make_unique<RowTimeDeduplicateRanker>(
        opId.fetch_add(1),
        ctx,
        deduplicateNode,
        deduplicateNode->rowtimeIndex(),
        deduplicateNode->minRetentionTime(),
        deduplicateNode->generateUpdateBefore(),
        deduplicateNode->generateInsert(),
        deduplicateNode->keepLastRow());
  } else if (
      auto topNNode =
          std::dynamic_pointer_cast<const StreamTopNNode>(planNode)) {
    auto op = nodeToOperator(topNNode->topN(), ctx);
    std::unique_ptr<KeySelector> sortKeySelector =
        std::make_unique<KeySelector>(
            topNNode->sortKeySelectorSpec()->create(INT_MAX, true),
            op->pool());
    return std::make_unique<AppendOnlyTopNRanker>(
        opId.fetch_add(1),
        ctx,
        topNNode,
        std::move(op),
        std::move(sortKeySelector),
        topNNode->generateUpdateBefore(),
        topNNode->outputRankNumber(),
        topNNode->cacheSize());
  } else if (
      auto rankNode =
          std::dynamic_pointer_cast<const StreamRankNode>(planNode)) {
    return nodeToOperator(rankNode->ranker(), ctx);
  } else if (
      auto groupAggNode =
          std::dynamic_pointer_cast<const GroupAggregationNode>(planNode)) {
    return nodeToOperator(groupAggNode->aggregation(), ctx);
  } else if (
      auto aggsHandlerNode =
          std::dynamic_pointer_cast<const GroupAggsHandlerNode>(planNode)) {
    return std::make_unique<GroupAggregator>(
        opId.fetch_add(1),
        ctx,
        aggsHandlerNode,
        std::make_unique<AggsHandleFunction>(), // TODO: not complete yet
        aggsHandlerNode->generateUpdateBefore(),
        aggsHandlerNode->needRetraction());
  } else {
    std::unique_ptr<exec::Operator> extended;
    extended = exec::Operator::fromPlanNode(ctx, opId.fetch_add(1), planNode);
    VELOX_CHECK(extended, "Unsupported plan node: {}", planNode->toString());
    return extended;
  }
}

} // namespace facebook::velox::stateful
