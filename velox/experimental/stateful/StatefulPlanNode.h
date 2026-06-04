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
#include <cstdint>

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

class WatermarkAssignerNode : public core::PlanNode {
 public:
  WatermarkAssignerNode(
      const core::PlanNodeId& id,
      std::shared_ptr<const core::ProjectNode>& project,
      int64_t idleTimeout,
      int rowtimeFieldIndex,
      int64_t watermarkInterval)
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

  const std::shared_ptr<const core::ProjectNode>& project() const {
    return project_;
  }

  const int64_t idleTimeout() const {
    return idleTimeout_;
  }

  const int rowtimeFieldIndex() const {
    return rowtimeFieldIndex_;
  }

  const int64_t watermarkInterval() const {
    return watermarkInterval_;
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::shared_ptr<const core::ProjectNode> project_;
  const int64_t idleTimeout_;
  const int rowtimeFieldIndex_;
  const int64_t watermarkInterval_;
};

class WatermarkPushDownSpec : public ISerializable {
public:
  WatermarkPushDownSpec(
      std::shared_ptr<const core::ProjectNode> project,
      int64_t idleTimeout,
      int64_t watermarkInterval,
      int32_t rowtimeFieldIndex)
      : project_(std::move(project)),
        idleTimeout_(idleTimeout),
        watermarkInterval_(watermarkInterval),
        rowtimeFieldIndex_(rowtimeFieldIndex) {}

  int64_t watermarkInterval() const {
    return watermarkInterval_;
  }

  int64_t idleTimeout() const {
    return idleTimeout_;
  }

  int32_t rowtimeFieldIndex() const {
    return rowtimeFieldIndex_;
  }

  const std::shared_ptr<const core::ProjectNode>& project() const {
    return project_;
  }

  folly::dynamic serialize() const override;

  static std::shared_ptr<WatermarkPushDownSpec> deserialize(
      const folly::dynamic& obj,
      void* context);

private:
  const std::shared_ptr<const core::ProjectNode> project_;
  const int64_t idleTimeout_;
  const int64_t watermarkInterval_;
  const int32_t rowtimeFieldIndex_;
};
  

class TableScanNodeWithWatermark : public core::TableScanNode {
  public:
    TableScanNodeWithWatermark(
      const core::PlanNodeId& id,
      RowTypePtr outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments,
      const std::shared_ptr<WatermarkPushDownSpec>& watermarkPushDownSpec)
         : core::TableScanNode(id, outputType, tableHandle, assignments),
         watermarkPushDownSpec_(watermarkPushDownSpec) {}
  
    const std::shared_ptr<WatermarkPushDownSpec>& watermarkPushDownSpec() const {
      return watermarkPushDownSpec_;
    }
  
    std::string_view name() const override {
      return "TableScanWithWatermark";
    }
  
    folly::dynamic serialize() const override;
  
    static core::PlanNodePtr create(const folly::dynamic& obj, void* context);
  
  private:
    const std::shared_ptr<WatermarkPushDownSpec> watermarkPushDownSpec_;
    
};

class StreamJoinNode : public core::PlanNode {
 public:
  StreamJoinNode(
      const core::PlanNodeId& id,
      const std::vector<core::PlanNodePtr>& sources,
      const std::shared_ptr<const core::PartitionFunctionSpec>&
          leftPartFuncSpec,
      const std::shared_ptr<const core::PartitionFunctionSpec>&
          rightPartFuncSpec,
      const std::shared_ptr<const core::NestedLoopJoinNode>& probe,
      RowTypePtr outputType,
      int numPartitions)
      : core::PlanNode(id),
        sources_(std::move(sources)),
        leftPartFuncSpec_(std::move(leftPartFuncSpec)),
        rightPartFuncSpec_(std::move(rightPartFuncSpec)),
        probe_(std::move(probe)),
        outputType_(std::move(outputType)),
        numPartitions_(numPartitions) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "StreamJoin";
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& leftPartFuncSpec()
      const {
    return leftPartFuncSpec_;
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& rightPartFuncSpec()
      const {
    return rightPartFuncSpec_;
  }

  const std::shared_ptr<const core::NestedLoopJoinNode>& probe() const {
    return probe_;
  }

  const int numPartitions() const {
    return numPartitions_;
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<core::PlanNodePtr> sources_;
  const std::shared_ptr<const core::PartitionFunctionSpec> leftPartFuncSpec_;
  const std::shared_ptr<const core::PartitionFunctionSpec> rightPartFuncSpec_;
  const std::shared_ptr<const core::NestedLoopJoinNode> probe_;
  const RowTypePtr outputType_;
  const int numPartitions_;
};

// Generate hash for RowVector to exchange by key.
class StreamPartitionNode : public core::PlanNode {
 public:
  StreamPartitionNode(
      const core::PlanNodeId& id,
      std::shared_ptr<const core::LocalPartitionNode>& partitionNode,
      int numPartitions)
      : PlanNode(id),
        partition_(std::move(partitionNode)),
        numPartitions_(numPartitions) {}

  const RowTypePtr& outputType() const override {
    return partition_->outputType();
  }

  const int numPartitions() const {
    return numPartitions_;
  }

  const std::shared_ptr<const core::LocalPartitionNode>& partition() const {
    return partition_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "Partition";
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override {}

  const std::shared_ptr<const core::LocalPartitionNode> partition_;
  const int numPartitions_;
};

// Only used to make other PlanNode sources valid.
class EmptyNode : public core::PlanNode {
 public:
  EmptyNode(RowTypePtr outputType)
      : PlanNode("empty"), outputType_(std::move(outputType)) {}

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

class StreamWindowJoinNode : public core::PlanNode {
 public:
  StreamWindowJoinNode(
      const core::PlanNodeId& id,
      const std::vector<core::PlanNodePtr>& sources,
      const std::shared_ptr<const core::PartitionFunctionSpec>&
          leftPartFuncSpec,
      const std::shared_ptr<const core::PartitionFunctionSpec>&
          rightPartFuncSpec,
      const std::shared_ptr<const core::NestedLoopJoinNode>& probe,
      RowTypePtr outputType,
      int numPartitions,
      int leftWindowEndIndex,
      int rightWindowEndIndex)
      : core::PlanNode(id),
        sources_(std::move(sources)),
        leftPartFuncSpec_(std::move(leftPartFuncSpec)),
        rightPartFuncSpec_(std::move(rightPartFuncSpec)),
        probe_(std::move(probe)),
        outputType_(std::move(outputType)),
        numPartitions_(numPartitions),
        leftWindowEndIndex_(leftWindowEndIndex),
        rightWindowEndIndex_(rightWindowEndIndex) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "StreamWindowJoin";
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& leftPartFuncSpec()
      const {
    return leftPartFuncSpec_;
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& rightPartFuncSpec()
      const {
    return rightPartFuncSpec_;
  }

  const std::shared_ptr<const core::NestedLoopJoinNode>& probe() const {
    return probe_;
  }

  const int numPartitions() const {
    return numPartitions_;
  }

  const int leftWindowEndIndex() const {
    return leftWindowEndIndex_;
  }

  const int rightWindowEndIndex() const {
    return rightWindowEndIndex_;
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<core::PlanNodePtr> sources_;
  const std::shared_ptr<const core::PartitionFunctionSpec> leftPartFuncSpec_;
  const std::shared_ptr<const core::PartitionFunctionSpec> rightPartFuncSpec_;
  const std::shared_ptr<const core::NestedLoopJoinNode> probe_;
  const RowTypePtr outputType_;
  const int numPartitions_;
  const int leftWindowEndIndex_;
  const int rightWindowEndIndex_;
};

class StreamWindowAggregationNode : public core::PlanNode {
 public:
  StreamWindowAggregationNode(
      const core::PlanNodeId& id,
      std::shared_ptr<const core::AggregationNode>& aggregationNode,
      std::shared_ptr<const core::AggregationNode>& localAgg,
      const std::shared_ptr<const core::PartitionFunctionSpec>& keySelectorSpec,
      const std::shared_ptr<const core::PartitionFunctionSpec>&
          sliceAssignerSpec,
      int64_t windowInterval,
      bool useDayLightSaving,
      bool isLocalAgg,
      int64_t size,
      int64_t step,
      int64_t offset,
      int windowType,
      const RowTypePtr& outputType,
      bool isEventTime,
      int rowtimeIndex,
      int windowStartIndex,
      int windowEndIndex) :
        PlanNode(id),
        aggregation_(std::move(aggregationNode)),
        localAgg_(std::move(localAgg)),
        keySelectorSpec_(std::move(keySelectorSpec)),
        sliceAssignerSpec_(std::move(sliceAssignerSpec)),
        windowInterval_(windowInterval),
        useDayLightSaving_(useDayLightSaving),
        isLocalAgg_(isLocalAgg),
        size_(size),
        step_(step),
        offset_(offset),
        windowType_(windowType),
        outputType_(std::move(outputType)),
        isEventTime_(isEventTime),
        rowtimeIndex_(rowtimeIndex),
        windowStartIndex_(windowStartIndex),
        windowEndIndex_(windowEndIndex) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::shared_ptr<const core::AggregationNode>& aggregation() const {
    return aggregation_;
  }

  const std::shared_ptr<const core::AggregationNode>& localAgg() const {
    return localAgg_;
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& keySelectorSpec()
      const {
    return keySelectorSpec_;
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& sliceAssignerSpec()
      const {
    return sliceAssignerSpec_;
  }

  int64_t windowInterval() const {
    return windowInterval_;
  }

  bool useDayLightSaving() const {
    return useDayLightSaving_;
  }

  bool isLocalAgg() const {
    return isLocalAgg_;
  }

  int64_t size() const {
    return size_;
  }

  int64_t step() const {
    return step_;
  }

  int64_t offset() const {
    return offset_;
  }

  int windowType() const {
    return windowType_;
  }

  bool isEventTime() const {
    return isEventTime_;
  }

  int rowtimeIndex() const {
    return rowtimeIndex_;
  }

  int windowStartIndex() const {
    return windowStartIndex_;
  }

  int windowEndIndex() const {
    return windowEndIndex_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "StreamWindowAggregation";
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::shared_ptr<const core::AggregationNode> aggregation_;
  const std::shared_ptr<const core::AggregationNode> localAgg_;
  const std::shared_ptr<const core::PartitionFunctionSpec> keySelectorSpec_;
  const std::shared_ptr<const core::PartitionFunctionSpec> sliceAssignerSpec_;
  int64_t windowInterval_ = 0;
  bool useDayLightSaving_ = false;
  bool isLocalAgg_;
  int64_t size_;
  int64_t step_;
  int64_t offset_;
  int windowType_;
  const RowTypePtr outputType_;
  bool isEventTime_;
  int rowtimeIndex_;
  int windowStartIndex_;
  int windowEndIndex_;
};

class GroupWindowAggsHandlerNode : public core::PlanNode {
 public:
  // TODO: finish this class
  GroupWindowAggsHandlerNode(
      const core::PlanNodeId& id,
      const RowTypePtr& outputType)
      : PlanNode(id), outputType_(std::move(outputType)) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  std::string_view name() const override {
    return "GroupWindowAggsHandler";
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const RowTypePtr outputType_;
};

class GroupWindowAggregationNode : public core::PlanNode {
 public:
  GroupWindowAggregationNode(
      const core::PlanNodeId& id,
      std::shared_ptr<const GroupWindowAggsHandlerNode>& aggregationNode,
      const std::shared_ptr<const core::PartitionFunctionSpec>& keySelectorSpec,
      const std::shared_ptr<const core::PartitionFunctionSpec>&
          sliceAssignerSpec,
      int64_t allowedLateness,
      bool produceUpdates,
      int rowtimeIndex,
      bool isEventTime,
      int windowType,
      const RowTypePtr& outputType)
      : PlanNode(id),
        aggregation_(std::move(aggregationNode)),
        keySelectorSpec_(std::move(keySelectorSpec)),
        sliceAssignerSpec_(std::move(sliceAssignerSpec)),
        allowedLateness_(allowedLateness),
        produceUpdates_(produceUpdates),
        rowtimeIndex_(rowtimeIndex),
        isEventTime_(isEventTime),
        windowType_(windowType),
        outputType_(std::move(outputType)) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::shared_ptr<const GroupWindowAggsHandlerNode>& aggregation() const {
    return aggregation_;
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& keySelectorSpec()
      const {
    return keySelectorSpec_;
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& sliceAssignerSpec()
      const {
    return sliceAssignerSpec_;
  }

  int64_t allowedLateness() const {
    return allowedLateness_;
  }

  bool produceUpdates() const {
    return produceUpdates_;
  }

  bool isEventTime() const {
    return isEventTime_;
  }

  int windowType() const {
    return windowType_;
  }

  int rowtimeIndex() const {
    return rowtimeIndex_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "GroupWindowAggregation";
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::shared_ptr<const GroupWindowAggsHandlerNode> aggregation_;
  const std::shared_ptr<const core::PartitionFunctionSpec> keySelectorSpec_;
  const std::shared_ptr<const core::PartitionFunctionSpec> sliceAssignerSpec_;
  int64_t allowedLateness_ = 0;
  bool produceUpdates_;
  int rowtimeIndex_;
  bool isEventTime_;
  int windowType_;
  const RowTypePtr outputType_;
};

class StreamRankNode : public core::PlanNode {
 public:
  StreamRankNode(
      const core::PlanNodeId& id,
      const std::shared_ptr<const core::PlanNode>& ranker,
      const std::shared_ptr<const core::PartitionFunctionSpec>& keySelectorSpec,
      RowTypePtr outputType)
      : core::PlanNode(id),
        ranker_(std::move(ranker)),
        keySelectorSpec_(std::move(keySelectorSpec)),
        outputType_(std::move(outputType)) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "StreamRank";
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& keySelectorSpec()
      const {
    return keySelectorSpec_;
  }

  const std::shared_ptr<const core::PlanNode>& ranker() const {
    return ranker_;
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::shared_ptr<const core::PlanNode> ranker_;
  const std::shared_ptr<const core::PartitionFunctionSpec> keySelectorSpec_;
  const RowTypePtr outputType_;
};

class DeduplicateNode : public core::PlanNode {
 public:
  DeduplicateNode(
      const core::PlanNodeId& id,
      RowTypePtr outputType,
      int64_t minRetentionTime,
      int rowtimeIndex,
      bool generateUpdateBefore,
      bool generateInsert,
      bool keepLastRow)
      : core::PlanNode(id),
        outputType_(std::move(outputType)),
        minRetentionTime_(minRetentionTime),
        rowtimeIndex_(rowtimeIndex),
        generateUpdateBefore_(generateUpdateBefore),
        generateInsert_(generateInsert),
        keepLastRow_(keepLastRow) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "Deduplicate";
  }

  int rowtimeIndex() const {
    return rowtimeIndex_;
  }

  int64_t minRetentionTime() const {
    return minRetentionTime_;
  }

  bool generateUpdateBefore() const {
    return generateUpdateBefore_;
  }

  bool generateInsert() const {
    return generateInsert_;
  }

  bool keepLastRow() const {
    return keepLastRow_;
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const RowTypePtr outputType_;
  int64_t minRetentionTime_;
  int rowtimeIndex_;
  bool generateUpdateBefore_;
  bool generateInsert_;
  bool keepLastRow_;
};

class StreamTopNNode : public core::PlanNode {
 public:
  StreamTopNNode(
      const core::PlanNodeId& id,
      const std::shared_ptr<const core::PlanNode>& topN,
      const std::shared_ptr<const core::PartitionFunctionSpec>&
          sortKeySelectorSpec,
      RowTypePtr outputType,
      bool generateUpdateBefore,
      bool outputRankNumber,
      int64_t cacheSize)
      : core::PlanNode(id),
        topN_(std::move(topN)),
        sortKeySelectorSpec_(std::move(sortKeySelectorSpec)),
        outputType_(std::move(outputType)),
        generateUpdateBefore_(generateUpdateBefore),
        outputRankNumber_(outputRankNumber),
        cacheSize_(cacheSize) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "StreamTopN";
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>&
  sortKeySelectorSpec() const {
    return sortKeySelectorSpec_;
  }

  const std::shared_ptr<const core::PlanNode>& topN() const {
    return topN_;
  }

  bool generateUpdateBefore() const {
    return generateUpdateBefore_;
  }

  bool outputRankNumber() const {
    return outputRankNumber_;
  }

  int64_t cacheSize() const {
    return cacheSize_;
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::shared_ptr<const core::PlanNode> topN_;
  const std::shared_ptr<const core::PartitionFunctionSpec> sortKeySelectorSpec_;
  const RowTypePtr outputType_;
  bool generateUpdateBefore_;
  bool outputRankNumber_;
  int64_t cacheSize_;
};

class GroupAggsHandlerNode : public core::PlanNode {
 public:
  // TODO: finish this class
  GroupAggsHandlerNode(
      const core::PlanNodeId& id,
      const RowTypePtr& outputType,
      bool generateUpdateBefore,
      bool needRetraction)
      : PlanNode(id),
        outputType_(std::move(outputType)),
        generateUpdateBefore_(generateUpdateBefore),
        needRetraction_(needRetraction) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  bool generateUpdateBefore() const {
    return generateUpdateBefore_;
  }

  bool needRetraction() const {
    return needRetraction_;
  }

  std::string_view name() const override {
    return "GroupAggsHandler";
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const RowTypePtr outputType_;
  bool generateUpdateBefore_;
  bool needRetraction_;
};

class GroupAggregationNode : public core::PlanNode {
 public:
  GroupAggregationNode(
      const core::PlanNodeId& id,
      std::shared_ptr<const GroupAggsHandlerNode>& aggregationNode,
      const std::shared_ptr<const core::PartitionFunctionSpec>& keySelectorSpec,
      const RowTypePtr& outputType)
      : PlanNode(id),
        aggregation_(std::move(aggregationNode)),
        keySelectorSpec_(std::move(keySelectorSpec)),
        outputType_(std::move(outputType)) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::shared_ptr<const GroupAggsHandlerNode>& aggregation() const {
    return aggregation_;
  }

  const std::shared_ptr<const core::PartitionFunctionSpec>& keySelectorSpec()
      const {
    return keySelectorSpec_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override {
    return "GroupAggregation";
  }

  folly::dynamic serialize() const override;

  static core::PlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::shared_ptr<const GroupAggsHandlerNode> aggregation_;
  const std::shared_ptr<const core::PartitionFunctionSpec> keySelectorSpec_;
  const RowTypePtr outputType_;
};

} // namespace facebook::velox::stateful
