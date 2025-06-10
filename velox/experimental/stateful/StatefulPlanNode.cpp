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

#include <iostream>

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
  stream << "build: " << build_->toString();
  stream << ", probe: " << probe_->toString();
}

folly::dynamic StreamJoinNode::serialize() const {
  auto obj = core::PlanNode::serialize();
  obj["build"] = build_->serialize();
  obj["probe"] = probe_->serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

// static
core::PlanNodePtr StreamJoinNode::create(const folly::dynamic& obj, void* context) {
  auto planNodeId = obj["id"].asString();
  auto sources = ISerializable::deserialize<std::vector<PlanNode>>(
      obj["sources"], context);
  VELOX_CHECK_EQ(2, sources.size());

  auto build = ISerializable::deserialize<core::NestedLoopJoinNode>(
      obj["build"], context);
  auto probe = ISerializable::deserialize<core::NestedLoopJoinNode>(
      obj["probe"], context);

  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);

  return std::make_shared<StreamJoinNode>(
      planNodeId,
      std::move(sources),
      std::move(build),
      std::move(probe),
      outputType);
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
  std::cout << "" << "StreamPartitionNode created 1" << std::endl;
  auto planNodeId = obj["id"].asString();
  auto numPartitions = obj["numPartitions"].asInt();
  std::cout << "" << "StreamPartitionNode created 2" << std::endl;
  auto partition = ISerializable::deserialize<core::LocalPartitionNode>(
      obj["partition"], context);
  std::cout << "" << "StreamPartitionNode created with numPartitions: " << numPartitions << std::endl;
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

} // namespace facebook::velox::stateful
