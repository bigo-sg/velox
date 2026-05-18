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
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include <cstdint>

namespace facebook::velox::stateful {

namespace {
const std::vector<core::PlanNodePtr> kEmptySources;
}

const std::vector<core::PlanNodePtr>& StatefulPlanNode::sources() const {
  return node_->sources();
}

void StatefulPlanNode::addDetails(std::stringstream& stream) const {
  stream << "Node: " << node_->toString(true, true);
  stream << "Targets: [" << std::endl;
  for (auto target : targets_) {
    stream << target->toString(true, true) << "," << std::endl;
  }
  stream << "]" << std::endl;
}

folly::dynamic StatefulPlanNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["node"] = node_->serialize();
  obj["targets"] = folly::dynamic::array;
  for (const auto& target : targets_) {
    obj["targets"].push_back(target->serialize());
  }
  return obj;
}

// static
core::PlanNodePtr StatefulPlanNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto node = ISerializable::deserialize<core::PlanNode>(obj["node"], context);
  auto targets = std::vector<core::PlanNodePtr>();
  if (obj.count("targets")) {
    targets = ISerializable::deserialize<std::vector<core::PlanNode>>(
        obj["targets"], context);
  }

  return std::make_shared<const StatefulPlanNode>(
      std::move(node), std::move(targets));
}

void StatefulPlanNode::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register(
      "WatermarkPushDownSpec", WatermarkPushDownSpec::deserialize);
  registry.Register(
      "TableScanWithWatermarkNode", TableScanNodeWithWatermark::create);
  registry.Register("WatermarkAssignerNode", WatermarkAssignerNode::create);
  registry.Register("StatefulPlanNode", StatefulPlanNode::create);
  registry.Register("EmptyNode", EmptyNode::create);
  registry.Register("StreamJoinNode", StreamJoinNode::create);
  registry.Register("StreamPartitionNode", StreamPartitionNode::create);
  registry.Register("StreamWindowJoinNode", StreamWindowJoinNode::create);
  registry.Register(
      "StreamWindowAggregationNode", StreamWindowAggregationNode::create);
  registry.Register(
      "GroupWindowAggsHandlerNode", GroupWindowAggsHandlerNode::create);
  registry.Register(
      "GroupWindowAggregationNode", GroupWindowAggregationNode::create);
  registry.Register("StreamRankNode", StreamRankNode::create);
  registry.Register("DeduplicateNode", DeduplicateNode::create);
  registry.Register("StreamTopNNode", StreamTopNNode::create);
  registry.Register("GroupAggsHandlerNode", GroupAggsHandlerNode::create);
  registry.Register("GroupAggregationNode", GroupAggregationNode::create);
}

folly::dynamic WatermarkPushDownSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "WatermarkPushDownSpec";
  obj["project"] = project_->serialize();
  obj["idleTimeout"] = idleTimeout_;
  obj["rowtimeFieldIndex"] = rowtimeFieldIndex_;
  obj["watermarkInterval"] = watermarkInterval_;
  return obj;
}

// static
std::shared_ptr<WatermarkPushDownSpec> WatermarkPushDownSpec::deserialize(
    const folly::dynamic& obj,
    void* context) {
  auto project =
      ISerializable::deserialize<core::ProjectNode>(obj["project"], context);
  auto idleTimeout = obj["idleTimeout"].asInt();
  auto rowtimeFieldIndex =
      static_cast<int32_t>(obj["rowtimeFieldIndex"].asInt());
  int64_t watermarkInterval = obj["watermarkInterval"].asInt();
  return std::make_shared<WatermarkPushDownSpec>(
      std::move(project),
      idleTimeout,
      watermarkInterval,
      rowtimeFieldIndex);
}

folly::dynamic TableScanNodeWithWatermark::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["outputType"] = outputType()->serialize();
  obj["tableHandle"] = tableHandle()->serialize();
  folly::dynamic serializedAssignments = folly::dynamic::array;
  for (const auto& [assign, columnHandle] : assignments()) {
    folly::dynamic assignmentPair = folly::dynamic::object;
    assignmentPair["assign"] = assign;
    assignmentPair["columnHandle"] = columnHandle->serialize();
    serializedAssignments.push_back(std::move(assignmentPair));
  }
  obj["assignments"] = std::move(serializedAssignments);
  obj["watermarkPushDownSpec"] = watermarkPushDownSpec_
      ? watermarkPushDownSpec_->serialize()
      : folly::dynamic(nullptr);
  return obj;
}

// static
core::PlanNodePtr TableScanNodeWithWatermark::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  auto tableHandle = std::const_pointer_cast<connector::ConnectorTableHandle>(
      ISerializable::deserialize<connector::ConnectorTableHandle>(
          obj["tableHandle"], context));

  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (const auto& pair : obj["assignments"]) {
    auto assign = pair["assign"].asString();
    auto columnHandle = ISerializable::deserialize<connector::ColumnHandle>(
        pair["columnHandle"]);
    assignments[assign] =
        std::const_pointer_cast<connector::ColumnHandle>(columnHandle);
  }

  std::shared_ptr<WatermarkPushDownSpec> watermarkPushDownSpec;
  if (obj.count("watermarkPushDownSpec") &&
      !obj["watermarkPushDownSpec"].isNull()) {
    watermarkPushDownSpec = std::const_pointer_cast<WatermarkPushDownSpec>(
        ISerializable::deserialize<WatermarkPushDownSpec>(
            obj["watermarkPushDownSpec"], context));
  }

  return std::make_shared<const TableScanNodeWithWatermark>(
      planNodeId,
      outputType,
      tableHandle,
      assignments,
      watermarkPushDownSpec);
}

const std::vector<core::PlanNodePtr>& WatermarkAssignerNode::sources() const {
  return kEmptySources;
}

void WatermarkAssignerNode::addDetails(std::stringstream& stream) const {
  stream << project_->toString();
}

folly::dynamic WatermarkAssignerNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["project"] = project_->serialize();
  obj["idleTimeout"] = idleTimeout_;
  obj["rowtimeFieldIndex"] = rowtimeFieldIndex_;
  obj["watermarkInterval"] = watermarkInterval_;
  return obj;
}

// static
core::PlanNodePtr WatermarkAssignerNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto project =
      ISerializable::deserialize<core::ProjectNode>(obj["project"], context);
  auto idleTimeout = obj["idleTimeout"].asInt();
  int rowtimeFieldIndex = obj["rowtimeFieldIndex"].asInt();
  int64_t watermarkInterval = obj["watermarkInterval"].asInt();

  return std::make_shared<const WatermarkAssignerNode>(
      planNodeId, project, idleTimeout, rowtimeFieldIndex, watermarkInterval);
}

void StreamJoinNode::addDetails(std::stringstream& stream) const {
  stream << "leftPartFuncSpec: " << leftPartFuncSpec_->toString();
  stream << ", rightPartFuncSpec: " << rightPartFuncSpec_->toString();
  stream << ", probe: " << probe_->toString();
}

folly::dynamic StreamJoinNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["leftPartFuncSpec"] = leftPartFuncSpec_->serialize();
  obj["rightPartFuncSpec"] = rightPartFuncSpec_->serialize();
  obj["probe"] = probe_->serialize();
  obj["outputType"] = outputType_->serialize();
  obj["numPartitions"] = numPartitions_;
  return obj;
}

// static
core::PlanNodePtr StreamJoinNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto sources = ISerializable::deserialize<std::vector<PlanNode>>(
      obj["sources"], context);
  VELOX_CHECK_EQ(2, sources.size());

  auto leftPartFuncSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["leftPartFuncSpec"], context);
  auto rightPartFuncSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["rightPartFuncSpec"], context);
  auto probe = ISerializable::deserialize<core::NestedLoopJoinNode>(
      obj["probe"], context);

  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);

  return std::make_shared<StreamJoinNode>(
      planNodeId,
      std::move(sources),
      std::move(leftPartFuncSpec),
      std::move(rightPartFuncSpec),
      std::move(probe),
      outputType,
      obj["numPartitions"].asInt());
}

const std::vector<core::PlanNodePtr>& StreamPartitionNode::sources() const {
  return kEmptySources;
}

folly::dynamic StreamPartitionNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["numPartitions"] = numPartitions_;
  obj["partition"] = partition_->serialize();
  return obj;
}

// static
core::PlanNodePtr StreamPartitionNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto numPartitions = obj["numPartitions"].asInt();
  auto partition = ISerializable::deserialize<core::LocalPartitionNode>(
      obj["partition"], context);
  return std::make_shared<const StreamPartitionNode>(
      planNodeId, partition, numPartitions);
}

const std::vector<core::PlanNodePtr>& EmptyNode::sources() const {
  return kEmptySources;
}

folly::dynamic EmptyNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

// static
core::PlanNodePtr EmptyNode::create(const folly::dynamic& obj, void* context) {
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  return std::make_shared<const EmptyNode>(outputType);
}

void StreamWindowJoinNode::addDetails(std::stringstream& stream) const {
  stream << "leftPartFuncSpec: " << leftPartFuncSpec_->toString();
  stream << ", rightPartFuncSpec: " << rightPartFuncSpec_->toString();
  stream << ", probe: " << probe_->toString();
}

folly::dynamic StreamWindowJoinNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["leftPartFuncSpec"] = leftPartFuncSpec_->serialize();
  obj["rightPartFuncSpec"] = rightPartFuncSpec_->serialize();
  obj["probe"] = probe_->serialize();
  obj["outputType"] = outputType_->serialize();
  obj["numPartitions"] = numPartitions_;
  obj["leftWindowEndIndex"] = leftWindowEndIndex_;
  obj["rightWindowEndIndex"] = rightWindowEndIndex_;
  return obj;
}

// static
core::PlanNodePtr StreamWindowJoinNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto sources = ISerializable::deserialize<std::vector<PlanNode>>(
      obj["sources"], context);
  VELOX_CHECK_EQ(2, sources.size());

  auto leftPartFuncSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["leftPartFuncSpec"], context);
  auto rightPartFuncSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["rightPartFuncSpec"], context);
  auto probe = ISerializable::deserialize<core::NestedLoopJoinNode>(
      obj["probe"], context);

  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);

  return std::make_shared<StreamWindowJoinNode>(
      planNodeId,
      std::move(sources),
      std::move(leftPartFuncSpec),
      std::move(rightPartFuncSpec),
      std::move(probe),
      outputType,
      obj["numPartitions"].asInt(),
      obj["leftWindowEndIndex"].asInt(),
      obj["rightWindowEndIndex"].asInt());
}

const std::vector<core::PlanNodePtr>& StreamWindowAggregationNode::sources()
    const {
  return kEmptySources;
}

folly::dynamic StreamWindowAggregationNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["aggregation"] = aggregation_->serialize();
  obj["localAgg"] = localAgg_ ? localAgg_->serialize() : nullptr;
  obj["keySelectorSpec"] = keySelectorSpec_->serialize();
  obj["sliceAssignerSpec"] = sliceAssignerSpec_->serialize();
  obj["windowInterval"] = windowInterval_;
  obj["useDayLightSaving"] = useDayLightSaving_;
  obj["isLocalAgg"] = isLocalAgg_;
  obj["size"] = size_;
  obj["step"] = step_;
  obj["offset"] = offset_;
  obj["windowType"] = windowType_;
  obj["outputType"] = outputType_->serialize();
  obj["isEventTime"] = isEventTime_;
  obj["rowtimeIndex"] = rowtimeIndex_;
  obj["windowStartIndex"] = windowStartIndex_;
  obj["windowEndIndex"] = windowEndIndex_;
  return obj;
}

void StreamWindowAggregationNode::addDetails(std::stringstream& stream) const {
  stream << "aggregation: " << aggregation_->toString();
  stream << ", keySelector: " << keySelectorSpec_->toString();
  stream << ", sliceAssigner: " << sliceAssignerSpec_->toString();
  stream << ", windowType: " << windowType_;
  stream << ", isLocalAgg: " << isLocalAgg_;
  stream << ", output: " << outputType_->toString();
}

// static
core::PlanNodePtr StreamWindowAggregationNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto aggregation = ISerializable::deserialize<core::AggregationNode>(
      obj["aggregation"], context);
  auto keySelectorSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["keySelectorSpec"], context);
  auto sliceAssignerSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["sliceAssignerSpec"], context);
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  std::shared_ptr<const core::AggregationNode> localAgg;
  if (obj.count("localAgg")) {
    localAgg = ISerializable::deserialize<core::AggregationNode>(
        obj["localAgg"], context);
  }
  return std::make_shared<const StreamWindowAggregationNode>(
      planNodeId,
      aggregation,
      localAgg,
      keySelectorSpec,
      sliceAssignerSpec,
      obj["windowInterval"].asInt(),
      obj["useDayLightSaving"].asBool(),
      obj["isLocalAgg"].asBool(),
      obj["size"].asInt(),
      obj["step"].asInt(),
      obj["offset"].asInt(),
      obj["windowType"].asInt(),
      outputType,
      obj["isEventTime"].asBool(),
      obj["rowtimeIndex"].asInt(),
      obj["windowStartIndex"].asInt(),
      obj["windowEndIndex"].asInt());
}

const std::vector<core::PlanNodePtr>& GroupWindowAggsHandlerNode::sources()
    const {
  return kEmptySources;
}

folly::dynamic GroupWindowAggsHandlerNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void GroupWindowAggsHandlerNode::addDetails(std::stringstream& stream) const {
  stream << "output: " << outputType_->toString();
}

// static
core::PlanNodePtr GroupWindowAggsHandlerNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  return std::make_shared<const GroupWindowAggsHandlerNode>(
      planNodeId, outputType);
}

const std::vector<core::PlanNodePtr>& GroupWindowAggregationNode::sources()
    const {
  return kEmptySources;
}

folly::dynamic GroupWindowAggregationNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["aggregation"] = aggregation_->serialize();
  obj["keySelectorSpec"] = keySelectorSpec_->serialize();
  obj["sliceAssignerSpec"] = sliceAssignerSpec_->serialize();
  obj["allowedLateness"] = allowedLateness_;
  obj["produceUpdates"] = produceUpdates_;
  obj["rowtimeIndex"] = rowtimeIndex_;
  obj["isEventTime"] = isEventTime_;
  obj["windowType"] = windowType_;
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void GroupWindowAggregationNode::addDetails(std::stringstream& stream) const {
  stream << "aggregation: " << aggregation_->toString();
  stream << ", keySelector: " << keySelectorSpec_->toString();
  stream << ", sliceAssigner: " << sliceAssignerSpec_->toString();
  stream << ", windowType: " << windowType_;
  stream << ", output: " << outputType_->toString();
}

// static
core::PlanNodePtr GroupWindowAggregationNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto aggregation = ISerializable::deserialize<GroupWindowAggsHandlerNode>(
      obj["aggregation"], context);
  auto keySelectorSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["keySelectorSpec"], context);
  auto sliceAssignerSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["sliceAssignerSpec"], context);
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  return std::make_shared<const GroupWindowAggregationNode>(
      planNodeId,
      aggregation,
      keySelectorSpec,
      sliceAssignerSpec,
      obj["allowedLateness"].asInt(),
      obj["produceUpdates"].asBool(),
      obj["rowtimeIndex"].asInt(),
      obj["isEventTime"].asBool(),
      obj["windowType"].asInt(),
      outputType);
}

const std::vector<core::PlanNodePtr>& StreamRankNode::sources() const {
  return kEmptySources;
}

folly::dynamic StreamRankNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["ranker"] = ranker_->serialize();
  obj["keySelectorSpec"] = keySelectorSpec_->serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void StreamRankNode::addDetails(std::stringstream& stream) const {
  stream << "ranker: " << ranker_->toString();
  stream << ", keySelector: " << keySelectorSpec_->toString();
  stream << ", output: " << outputType_->toString();
}

// static
core::PlanNodePtr StreamRankNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto ranker =
      ISerializable::deserialize<core::PlanNode>(obj["ranker"], context);
  auto keySelectorSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["keySelectorSpec"], context);
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  return std::make_shared<const StreamRankNode>(
      planNodeId, ranker, keySelectorSpec, outputType);
}

const std::vector<core::PlanNodePtr>& DeduplicateNode::sources() const {
  return kEmptySources;
}

folly::dynamic DeduplicateNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["rowtimeIndex"] = rowtimeIndex_;
  obj["minRetentionTime"] = minRetentionTime_;
  obj["generateUpdateBefore"] = generateUpdateBefore_;
  obj["generateInsert"] = generateInsert_;
  obj["keepLastRow"] = keepLastRow_;
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void DeduplicateNode::addDetails(std::stringstream& stream) const {
  stream << "rowtimeIndex: " << rowtimeIndex_;
  stream << ", minRetentionTime: " << minRetentionTime_;
  stream << ", generateUpdateBefore: " << generateUpdateBefore_;
  stream << ", generateInsert: " << generateInsert_;
  stream << ", keepLastRow: " << keepLastRow_;
}

// static
core::PlanNodePtr DeduplicateNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  return std::make_shared<const DeduplicateNode>(
      planNodeId,
      outputType,
      obj["minRetentionTime"].asInt(),
      obj["rowtimeIndex"].asInt(),
      obj["generateUpdateBefore"].asBool(),
      obj["generateInsert"].asBool(),
      obj["keepLastRow"].asBool());
}

const std::vector<core::PlanNodePtr>& StreamTopNNode::sources() const {
  return kEmptySources;
}

folly::dynamic StreamTopNNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["topN"] = topN_->serialize();
  obj["sortKeySelectorSpec"] = sortKeySelectorSpec_->serialize();
  obj["generateUpdateBefore"] = generateUpdateBefore_;
  obj["outputRankNumber"] = outputRankNumber_;
  obj["cacheSize"] = cacheSize_;
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void StreamTopNNode::addDetails(std::stringstream& stream) const {
  stream << "generateUpdateBefore: " << generateUpdateBefore_;
  stream << ", outputRankNumber: " << outputRankNumber_;
  stream << ", cacheSize: " << cacheSize_;
}

// static
core::PlanNodePtr StreamTopNNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto topN = ISerializable::deserialize<core::PlanNode>(obj["topN"], context);
  auto sortKeySelectorSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["sortKeySelectorSpec"], context);
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  return std::make_shared<const StreamTopNNode>(
      planNodeId,
      topN,
      sortKeySelectorSpec,
      outputType,
      obj["generateUpdateBefore"].asBool(),
      obj["outputRankNumber"].asBool(),
      obj["cacheSize"].asInt());
}

const std::vector<core::PlanNodePtr>& GroupAggsHandlerNode::sources() const {
  return kEmptySources;
}

folly::dynamic GroupAggsHandlerNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["outputType"] = outputType_->serialize();
  obj["generateUpdateBefore"] = generateUpdateBefore_;
  obj["needRetraction"] = needRetraction_;
  return obj;
}

void GroupAggsHandlerNode::addDetails(std::stringstream& stream) const {
  stream << "output: " << outputType_->toString();
}

// static
core::PlanNodePtr GroupAggsHandlerNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  return std::make_shared<const GroupAggsHandlerNode>(
      planNodeId,
      outputType,
      obj["generateUpdateBefore"].asBool(),
      obj["needRetraction"].asBool());
}

const std::vector<core::PlanNodePtr>& GroupAggregationNode::sources() const {
  return kEmptySources;
}

folly::dynamic GroupAggregationNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["aggregation"] = aggregation_->serialize();
  obj["keySelectorSpec"] = keySelectorSpec_->serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

void GroupAggregationNode::addDetails(std::stringstream& stream) const {
  stream << "aggregation: " << aggregation_->toString();
  stream << ", keySelector: " << keySelectorSpec_->toString();
  stream << ", output: " << outputType_->toString();
}

// static
core::PlanNodePtr GroupAggregationNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto planNodeId = obj["id"].asString();
  auto aggregation = ISerializable::deserialize<GroupAggsHandlerNode>(
      obj["aggregation"], context);
  auto keySelectorSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["keySelectorSpec"], context);
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  return std::make_shared<const GroupAggregationNode>(
      planNodeId, aggregation, keySelectorSpec, outputType);
}

} // namespace facebook::velox::stateful
