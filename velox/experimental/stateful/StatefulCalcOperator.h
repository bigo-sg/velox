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
#include <memory>
#include <vector>

#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"

namespace facebook::velox::stateful {

// Wraps a velox FilterProject (Calc) operator so rowKind propagates through
// filter and projection. The underlying FilterProject is built with an
// augmented ProjectNode that adds a trailing identity $row_kind column, so
// velox's native filter/project logic applies to $row_kind automatically
// (filter re-indexes it; project carries it through).
//
// On the input side, the wrapper merges record + rowKind into a single
// RowVector via StreamRecord::toMergedRowVector(/*alwaysAppendRowKind=*/true)
// so the augmented project finds the trailing column on its input.
// On the output side, StreamRecord::create splits the merged RowVector back
// into a StreamRecord and normalizes a constant INSERT trailing column to
// appendOnly (rowKind=nullptr).
class StatefulCalcOperator : public StatefulOperator {
 public:
  // Constructs the underlying FilterProject from raw plan nodes, augmenting
  // the project to keep $row_kind as a trailing identity column. Either
  // filterNode or projectNode (or both) must be non-null.
  StatefulCalcOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      std::shared_ptr<const core::FilterNode> filterNode,
      std::shared_ptr<const core::ProjectNode> projectNode,
      std::vector<StatefulOperatorPtr> targets);

  // For tests and other callers that already hold a constructed operator
  // (FilterProject or otherwise). The caller is responsible for ensuring the
  // wrapped operator's output schema carries a trailing $row_kind column.
  using StatefulOperator::StatefulOperator;

  void addInput(StreamElementPtr input) override;
  void advance() override;
  void finish() override;
};

} // namespace facebook::velox::stateful
