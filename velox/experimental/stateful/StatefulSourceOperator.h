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

#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"

namespace facebook::velox::stateful {

// Wraps a source operator (TableScan, etc.) that has no upstream input.
// StreamRecord::create splits the source RowVector: if it carries a trailing
// $row_kind column, the result carries per-row RowKind; otherwise the result
// is appendOnly.
class StatefulSourceOperator : public StatefulOperator {
 public:
  using StatefulOperator::StatefulOperator;

  // Sources never receive input from upstream. Reject loudly to catch wiring
  // bugs in the operator chain.
  void addInput(StreamElementPtr /*input*/) final {
    VELOX_FAIL("StatefulSourceOperator does not support addInput");
  }

  void advance() override {
    setSourceEmpty(true);
    auto intermediateResult = op()->getOutput();
    if (!intermediateResult) {
      return;
    }
    setSourceEmpty(false);
    pushOutput(
        StreamRecord::create(getPlanNodeId(), std::move(intermediateResult)));
  }
};

} // namespace facebook::velox::stateful
