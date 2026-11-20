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
#include <vector>

#include "velox/core/QueryConfig.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

class GroupWindowAggregator;
class GroupWindowAggsHandlerNode;

/// This is relevant to Flink generated NamespaceAggsHandleFunction. It stays
/// the exec::Operator of the window chain (the GroupWindowAggregator wraps
/// it), and now carries the aggregate-details wiring of the typed keyed
/// state: the plan's aggregate calls resolved into function instances
/// (aggregate details), the AggregatingState descriptor callbacks
/// (construct fresh accumulators / merge intermediate columns back in), the
/// live merge of two accumulator rows (the session-window merging path), and
/// the final-value extraction of the emission path. All of these act on
/// char* accumulator rows of the state's value RowContainer; the handler
/// owns no state itself.
class GroupWindowAggsHandler : public exec::Operator {
 public:
  GroupWindowAggsHandler(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::PlanNode>& groupAggNode);

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  /// Resolves the plan-declared aggregate calls into AggregateInfo entries
  /// (function instances + input channels + intermediate types), driven by
  /// the window operator with the parent node's context (input schema, key
  /// count, query config). Rejects masks, sorting keys and DISTINCT
  /// fail-loud, and cross-checks the plan's declared output column types
  /// against the resolved functions' result types.
  void initializeWindowAggregates(
      const RowTypePtr& inputType,
      uint32_t numKeys,
      const core::QueryConfig& queryConfig);

  const std::vector<exec::AggregateInfo>& aggregates() const {
    return aggregates_;
  }

  /// AggregatingState descriptor callback: constructs fresh accumulators for
  /// a batch of newly materialized rows (wraps Aggregate::initializeNewGroups).
  void initializeRows(char** rows, size_t numRows);

  /// AggregatingState descriptor callback: merges row 0 of 'columns' (one
  /// one-row intermediate column per accumulator, produced by
  /// Accumulator::extractForSpill) into 'row' - the same construct-then-merge
  /// sequence velox uses to reload spilled aggregation state.
  void mergeRow(char* row, const std::vector<VectorPtr>& columns);

  /// Live accumulator merge of the session-window path: merges 'source' into
  /// 'target' by extracting the source row's intermediate columns and
  /// folding them into the target's accumulators.
  void mergeAccumulators(char* target, char* source);

  /// Extracts the final values of every aggregate for 'rows' (one column
  /// per aggregate) - the emission path's value read.
  void extractValues(
      const std::vector<char*>& rows,
      std::vector<VectorPtr>& outValues);

  /// The owning window operator registers itself at construction: the
  /// operator owns the keyed backend and the state handles, and the handler
  /// routes the exec::Operator checkpoint plane (snapshotState / restoreState)
  /// to it.
  void setOwner(GroupWindowAggregator* owner);

  std::vector<std::string> snapshotState(int64_t checkpointId) override;

  void restoreState(const std::vector<std::string>& checkpointRecords) override;

  /// Argument columns of aggregate 'a' over 'input' (channels and constants):
  /// the addInput loop of the window operator consumes them per row.
  std::vector<VectorPtr> aggregateArgs(
      const exec::AggregateInfo& info,
      const RowVectorPtr& input) const;

 private:
  std::vector<exec::AggregateInfo> aggregates_;
  GroupWindowAggregator* owner_ = nullptr;
  // The plan node this handler was built from, retained at construction:
  // exec::Operator keeps only the plan node id, and the aggregate details
  // (the plan-declared aggregate calls) hang off the node.
  std::shared_ptr<const GroupWindowAggsHandlerNode> handlerNode_;
};
} // namespace facebook::velox::stateful
