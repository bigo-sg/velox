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

class StatefulPlanNode : public core::PlanNode {
 public:
  StatefulPlanNode(
    const core::PlanNodePtr& node,
    const std::vector<core::PlanNodePtr>& targets)
    : PlanNode(node->id()), node_(std::move(node)), targets_(targets) {}

  const RowTypePtr& outputType() const override {
      return node_->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return node_->name();
  }

  bool requiresSplits() const override {
    return node_->requiresSplits();
  }

  static void registerSerDe();

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

  const core::PlanNodePtr node() const {
    return node_;
  }

  const std::vector<core::PlanNodePtr> targets() const {
    return targets_;
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const core::PlanNodePtr node_;
  const std::vector<core::PlanNodePtr> targets_;
};

class WatermarkAssignerNode :  public core::PlanNode {
 public:
  WatermarkAssignerNode(
      const core::PlanNodeId& id,
      std::shared_ptr<const core::ProjectNode> project,
      long idleTimeout,
      int rowtimeFieldIndex,
      long watermarkInterval)
      : PlanNode(id),
        project_(std::move(project)),
        idleTimeout_(idleTimeout),
        rowtimeFieldIndex_(rowtimeFieldIndex),
        watermarkInterval_(watermarkInterval) {}

  const RowTypePtr& outputType() const override {
    return project_->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "WatermarkAssigner";
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

  const std::shared_ptr<const core::ProjectNode> project() const {
    return project_;
  }

  const long idleTimeout() const {
    return idleTimeout_;
  }

  const int rowtimeFieldIndex() const {
    return rowtimeFieldIndex_;
  }

  const long watermarkInterval() const {
    return watermarkInterval_;
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::shared_ptr<const core::ProjectNode> project_;
  const long idleTimeout_;
  const int rowtimeFieldIndex_;
  const long watermarkInterval_;
};

class StreamJoinNode :  public core::PlanNode {
 public:
  StreamJoinNode(
      const core::PlanNodeId& id,
      core::JoinType joinType,
      bool nullAware,
      const std::vector<core::FieldAccessTypedExprPtr>& leftKeys,
      const std::vector<core::FieldAccessTypedExprPtr>& rightKeys,
      core::TypedExprPtr filter,
      core::PlanNodePtr leftNode,
      core::PlanNodePtr rightNode,
      RowTypePtr outputType)
      : core::PlanNode(id),
        joinType_(joinType),
        leftKeys_(std::move(leftKeys)),
        rightKeys_(std::move(rightKeys)),
        filter_(std::move(filter)),
        sources_({std::move(leftNode), std::move(rightNode)}),
        outputType_(std::move(outputType)),
        nullAware_(nullAware) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "StreamJoin";
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const core::JoinType joinType_;
  const std::vector<core::FieldAccessTypedExprPtr> leftKeys_;
  const std::vector<core::FieldAccessTypedExprPtr> rightKeys_;
  const core::TypedExprPtr filter_;
  const std::vector<core::PlanNodePtr> sources_;
  const RowTypePtr outputType_;
  const bool nullAware_;
};

// Generate hash for RowVector to exchange by key.
class StreamExchangeNode : public core::PlanNode {
  public:
  StreamExchangeNode(const core::PlanNodeId& id, RowTypePtr outputType) :
       PlanNode(id),
       outputType_(std::move(outputType)) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "Exchange";
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override {}

  const RowTypePtr outputType_;
};

// Only used to make other PlanNode sources valid.
class EmptyNode : public core::PlanNode {
 public:
  EmptyNode(RowTypePtr outputType) :
      PlanNode("empty"),
      outputType_(std::move(outputType)) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "Empty";
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override {}

  const RowTypePtr outputType_;
};
} // namespace facebook::velox::stateful
