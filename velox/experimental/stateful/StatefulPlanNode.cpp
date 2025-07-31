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
core::PlanNodePtr StatefulPlanNode::create(const folly::dynamic& obj, void* context) {
  auto node = ISerializable::deserialize<core::PlanNode>(
      obj["node"], context);
  auto targets = std::vector<core::PlanNodePtr>();
  if (obj.count("targets")) {
    targets = ISerializable::deserialize<std::vector<core::PlanNode>>(
        obj["targets"], context);
  }

  return std::make_shared<const StatefulPlanNode>(std::move(node), std::move(targets));
}

void StatefulPlanNode::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register("WatermarkAssignerNode", WatermarkAssignerNode::create);
  registry.Register("StatefulPlanNode", StatefulPlanNode::create);
  registry.Register("EmptyNode", EmptyNode::create);
  registry.Register("StreamJoinNode", StreamJoinNode::create);
  registry.Register("StreamPartitionNode", StreamPartitionNode::create);
  registry.Register("WindowJoinNode", WindowJoinNode::create);
  registry.Register("WindowAggregationNode", WindowAggregationNode::create);
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
core::PlanNodePtr WatermarkAssignerNode::create(const folly::dynamic& obj, void* context) {
  auto planNodeId = obj["id"].asString();
  auto project = ISerializable::deserialize<core::ProjectNode>(
      obj["project"], context);
  auto idleTimeout = obj["idleTimeout"].asInt();
  int rowtimeFieldIndex = obj["rowtimeFieldIndex"].asInt();
  long watermarkInterval = obj["watermarkInterval"].asInt();

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
core::PlanNodePtr StreamJoinNode::create(const folly::dynamic& obj, void* context) {
  auto planNodeId = obj["id"].asString();
  auto sources = ISerializable::deserialize<std::vector<PlanNode>>(
      obj["sources"], context);
  VELOX_CHECK_EQ(2, sources.size());

  auto leftPartFuncSpec = ISerializable::deserialize<core::PartitionFunctionSpec>(
      obj["leftPartFuncSpec"], context);
  auto rightPartFuncSpec = ISerializable::deserialize<core::PartitionFunctionSpec>(
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
core::PlanNodePtr StreamPartitionNode::create(const folly::dynamic& obj, void* context) {
  auto planNodeId = obj["id"].asString();
  auto numPartitions = obj["numPartitions"].asInt();
  auto partition = ISerializable::deserialize<core::LocalPartitionNode>(
      obj["partition"], context);
  return std::make_shared<const StreamPartitionNode>(planNodeId, partition, numPartitions);
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

void WindowJoinNode::addDetails(std::stringstream& stream) const {
  stream << "leftPartFuncSpec: " << leftPartFuncSpec_->toString();
  stream << ", rightPartFuncSpec: " << rightPartFuncSpec_->toString();
  stream << ", probe: " << probe_->toString();
}

folly::dynamic WindowJoinNode::serialize() const {
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
core::PlanNodePtr WindowJoinNode::create(const folly::dynamic& obj, void* context) {
  auto planNodeId = obj["id"].asString();
  auto sources = ISerializable::deserialize<std::vector<PlanNode>>(
      obj["sources"], context);
  VELOX_CHECK_EQ(2, sources.size());

  auto leftPartFuncSpec = ISerializable::deserialize<core::PartitionFunctionSpec>(
      obj["leftPartFuncSpec"], context);
  auto rightPartFuncSpec = ISerializable::deserialize<core::PartitionFunctionSpec>(
      obj["rightPartFuncSpec"], context);
  auto probe = ISerializable::deserialize<core::NestedLoopJoinNode>(
      obj["probe"], context);

  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);

  return std::make_shared<WindowJoinNode>(
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

const std::vector<core::PlanNodePtr>& WindowAggregationNode::sources() const {
  return kEmptySources;
}

folly::dynamic WindowAggregationNode::serialize() const {
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
  return obj;
}

void WindowAggregationNode::addDetails(std::stringstream& stream) const {
  stream << "aggregation: " << aggregation_->toString();
  stream << ", keySelector: " << keySelectorSpec_->toString();
  stream << ", sliceAssigner: " << sliceAssignerSpec_->toString();
  stream << ", windowType: " << windowType_;
  stream << ", isLocalAgg: " << isLocalAgg_;
  stream << ", output: " << outputType_->toString();
}

// static
core::PlanNodePtr WindowAggregationNode::create(const folly::dynamic& obj, void* context) {
  auto planNodeId = obj["id"].asString();
  auto aggregation = ISerializable::deserialize<core::AggregationNode>(
      obj["aggregation"], context);
  auto keySelectorSpec = ISerializable::deserialize<core::PartitionFunctionSpec>(
      obj["keySelectorSpec"], context);
  auto sliceAssignerSpec = ISerializable::deserialize<core::PartitionFunctionSpec>(
      obj["sliceAssignerSpec"], context);
  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);
  std::shared_ptr<const core::AggregationNode> localAgg;
  if (obj.count("localAgg")) {
    localAgg = ISerializable::deserialize<core::AggregationNode>(
        obj["localAgg"], context);
  }
  return std::make_shared<const WindowAggregationNode>(
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
      outputType);
}

} // namespace facebook::velox::stateful
