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

#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/state/StateBackend.h"

namespace facebook::velox::core {
struct PlanFragment;
} // namespace facebook::velox::core

namespace facebook::velox::stateful {

class StatefulPlanner {
 public:
  // Create stateful operator chain according to plan.
  static StatefulOperatorPtr plan(
      const core::PlanFragment& planFragment,
      exec::DriverCtx* ctx,
      StateBackend* stateBackend);

 protected:
  StatefulPlanner(exec::DriverCtx* ctx, StateBackend* stateBackend)
      : ctx_(ctx), stateBackend_(stateBackend) {}

 private:
  exec::DriverCtx* ctx_ = nullptr;
  StateBackend* stateBackend_ = nullptr;

  StatefulOperatorPtr transformStatefulOperators(
      const core::PlanNodePtr& planNode);
  std::vector<StatefulOperatorPtr> transformStatefulOperators(
      const std::vector<core::PlanNodePtr>& targets);
  std::unique_ptr<exec::Operator> transformOperator(
      const core::PlanNodePtr& planNode);
  StatefulOperatorPtr transformWatermarkAssignerOperator(
      const StatefulPlanNode& planNode);
  StatefulOperatorPtr transformStreamPartitionOperator(
      const StatefulPlanNode& planNode);
  StatefulOperatorPtr transformStreamJoinOperator(
      const StatefulPlanNode& planNode);
  StatefulOperatorPtr transformStreamWindowJoinOperator(
      const StatefulPlanNode& planNode);
  StatefulOperatorPtr transformStreamWindowAggregationOperator(
      const StatefulPlanNode& planNode);
  StatefulOperatorPtr transformGroupWindowAggregationOperator(
      const StatefulPlanNode& planNode);
  StatefulOperatorPtr transformStreamRankOperator(
      const StatefulPlanNode& planNode);
  StatefulOperatorPtr transformGroupAggregationOperator(
      const StatefulPlanNode& planNode);
  StatefulOperatorPtr transformGenericOperator(
      const StatefulPlanNode& planNode);
};
} // namespace facebook::velox::stateful
