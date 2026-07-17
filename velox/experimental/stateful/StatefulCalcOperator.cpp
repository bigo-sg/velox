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
#include "velox/experimental/stateful/StatefulCalcOperator.h"

#include "velox/core/Expressions.h"
#include "velox/exec/FilterProject.h"
#include "velox/experimental/stateful/RowKind.h"

namespace facebook::velox::stateful {

namespace {

// Returns a ProjectNode identical to 'projectNode' but with an extra trailing
// $row_kind identity column. Lets the underlying FilterProject preserve the
// $row_kind column through projection.
std::shared_ptr<const core::ProjectNode> augmentProjectWithRowKind(
    const std::shared_ptr<const core::ProjectNode>& projectNode) {
  auto names = projectNode->names();
  auto projections = projectNode->projections();
  names.emplace_back(std::string(kRowKindColumnName));
  projections.emplace_back(std::make_shared<const core::FieldAccessTypedExpr>(
      TINYINT(), std::string(kRowKindColumnName)));
  return std::make_shared<core::ProjectNode>(
      projectNode->id(),
      std::move(names),
      std::move(projections),
      projectNode->sources()[0]);
}

// Builds a synthetic ProjectNode that identity-projects every column of
// 'inputType' plus a trailing $row_kind. Used for the Filter-only path so
// the underlying FilterProject output schema includes $row_kind.
std::shared_ptr<const core::ProjectNode> identityProjectWithRowKind(
    const core::PlanNodeId& nodeId,
    const RowTypePtr& inputType,
    const core::PlanNodePtr& source) {
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> projections;
  names.reserve(inputType->size() + 1);
  projections.reserve(inputType->size() + 1);
  for (size_t i = 0; i < inputType->size(); ++i) {
    const auto& name = inputType->nameOf(i);
    names.emplace_back(name);
    projections.emplace_back(std::make_shared<const core::FieldAccessTypedExpr>(
        inputType->childAt(i), name));
  }
  names.emplace_back(std::string(kRowKindColumnName));
  projections.emplace_back(std::make_shared<const core::FieldAccessTypedExpr>(
      TINYINT(), std::string(kRowKindColumnName)));
  return std::make_shared<core::ProjectNode>(
      nodeId, std::move(names), std::move(projections), source);
}

// Builds the underlying FilterProject with an augmented project so $row_kind
// survives the projection. 'projectNode' may be null only when 'filterNode'
// is non-null (Filter-only path); in that case a synthetic identity project
// is built from filterNode's source output type.
std::unique_ptr<exec::FilterProject> buildCalcFilterProject(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::FilterNode>& filterNode,
    const std::shared_ptr<const core::ProjectNode>& projectNode) {
  std::shared_ptr<const core::ProjectNode> augmentedProject;
  if (projectNode) {
    augmentedProject = augmentProjectWithRowKind(projectNode);
  } else {
    augmentedProject = identityProjectWithRowKind(
        filterNode->id(), filterNode->outputType(), filterNode->sources()[0]);
  }
  return std::make_unique<exec::FilterProject>(
      operatorId, driverCtx, filterNode, augmentedProject);
}

} // namespace

StatefulCalcOperator::StatefulCalcOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    std::shared_ptr<const core::FilterNode> filterNode,
    std::shared_ptr<const core::ProjectNode> projectNode,
    std::vector<StatefulOperatorPtr> targets)
    : StatefulOperator(
          buildCalcFilterProject(
              operatorId,
              driverCtx,
              filterNode,
              projectNode),
          std::move(targets)) {}

void StatefulCalcOperator::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  // Always merge with trailing $row_kind (ConstantVector<int8_t>(INSERT) for
  // appendOnly inputs) so the augmented FilterProject finds the column on
  // its input regardless of appendOnly state.
  auto merged = record->toMergedRowVector(/*alwaysAppendRowKind=*/true);
  op()->traceInput(merged);
  op()->addInput(std::move(merged));
}

void StatefulCalcOperator::advance() {
  auto out = op()->getOutput();
  if (!out) {
    return;
  }
  // StreamRecord::create splits the merged RowVector and normalizes a
  // ConstantVector<int8_t>(INSERT) trailing column back to appendOnly.
  pushOutput(StreamRecord::create(getPlanNodeId(), std::move(out)));
}

void StatefulCalcOperator::finish() {
  if (needsFinishDrain()) {
    op()->noMoreInput();
    do {
      auto out = op()->getOutput();
      if (!out) {
        break;
      }
      pushOutput(StreamRecord::create(getPlanNodeId(), std::move(out)));
    } while (!op()->isFinished());
  }
  for (auto& target : targets()) {
    target->finish();
  }
}

} // namespace facebook::velox::stateful
