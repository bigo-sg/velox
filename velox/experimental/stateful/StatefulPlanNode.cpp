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

void WatermarkAssignerNode::addDetails(std::stringstream& stream) const {
  stream << project_->toString();
}

folly::dynamic WatermarkAssignerNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["project"] = project_->serialize();
  return obj;
}

// static
core::PlanNodePtr WatermarkAssignerNode::create(const folly::dynamic& obj, void* context) {
  auto planNodeId = obj["id"].asString();
  auto sources = ISerializable::deserialize<std::vector<core::PlanNode>>(
      obj["sources"], context);
  VELOX_CHECK_EQ(1, sources.size());
  auto project = ISerializable::deserialize<core::ProjectNode>(
      obj["project"], context);
  auto idleTimeout = obj["idleTimeout"].asInt();
  int rowtimeFieldIndex = obj["rowtimeFieldIndex"].asInt();

  return std::make_shared<const WatermarkAssignerNode>(
      planNodeId, sources[0], project, idleTimeout, rowtimeFieldIndex);
}

void WatermarkAssignerNode::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register("WatermarkAssignerNode", WatermarkAssignerNode::create);
}
} // namespace facebook::velox::stateful
