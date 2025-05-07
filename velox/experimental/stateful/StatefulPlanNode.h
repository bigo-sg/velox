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
#pragma once

#include "velox/core/PlanNode.h"

namespace facebook::velox::stateful {

class WatermarkAssignerNode :  public core::PlanNode {
 public:
  WatermarkAssignerNode(
      const core::PlanNodeId& id,
      core::PlanNodePtr source,
      std::shared_ptr<const core::ProjectNode> project,
      long idleTimeout,
      int rowtimeFieldIndex)
      : PlanNode(id),
        sources_{std::move(source)},
        project_(std::move(project)),
        idleTimeout_(idleTimeout),
        rowtimeFieldIndex_(rowtimeFieldIndex) {}
 
  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }
 
  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "WatermarkAssigner";
  }
 
  folly::dynamic serialize() const override;
 
  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

  static void registerSerDe();

  const std::shared_ptr<const core::ProjectNode> project() const {
    return project_;
  }

  const long idleTimeout() const {
    return idleTimeout_;
  }

  const int rowtimeFieldIndex() const {
    return rowtimeFieldIndex_;
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<core::PlanNodePtr> sources_;
  const std::shared_ptr<const core::ProjectNode> project_;
  const long idleTimeout_;
  const int rowtimeFieldIndex_;
};
} // namespace facebook::velox::stateful
