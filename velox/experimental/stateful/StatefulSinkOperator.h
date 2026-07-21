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

/// Wraps a sink operator (exec::TableWriter). When the underlying sink declares
/// supportsRowKind (ConnectorInsertTableHandle::supportsRowKind), addInput
/// feeds a merged RowVector (user columns + trailing $row_kind) so per-row
/// RowKind flows through TableWriter's name-based column selection into the
/// connector DataSink. Otherwise it feeds the plain record value (append-only
/// path). This keeps the RowKind merge logic out of the base StatefulOperator
/// and lets the capability drive behavior instead of a hardcoded connector id.
class StatefulSinkOperator : public StatefulOperator {
 public:
  StatefulSinkOperator(
      std::unique_ptr<exec::Operator> op,
      std::vector<StatefulOperatorPtr> targets,
      bool supportsRowKind)
      : StatefulOperator(std::move(op), std::move(targets)),
        supportsRowKind_(supportsRowKind) {}

  void addInput(StreamElementPtr input) override {
    auto record = std::static_pointer_cast<StreamRecord>(input);
    RowVectorPtr rowVector = supportsRowKind_
        ? record->toMergedRowVector(/*alwaysAppendRowKind=*/true)
        : record->record();
    op()->traceInput(rowVector);
    op()->addInput(rowVector);
  }

 private:
  const bool supportsRowKind_;
};

} // namespace facebook::velox::stateful
