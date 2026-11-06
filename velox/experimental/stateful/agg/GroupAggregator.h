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
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/State.h"

namespace facebook::velox::stateful {

/// Grouped streaming aggregation over the keyed state backend, the
/// counterpart of Flink's GroupAggFunction. Each input batch is probed by
/// user key; every key's accumulators live in one row of an AggregatingState
/// value container laid out in the accumulator format velox's
/// HashAggregation rows use, so the aggregate functions update their slots
/// in place. Per batch the operator emits one record with the changelog of
/// every key whose final values changed: +I for a key's first appearance,
/// and for a changed existing key the -U row (carrying the previous values)
/// immediately followed by the +U row (carrying the new values) when the
/// plan asks for update-before rows, +U alone for a changed key otherwise,
/// and nothing for keys whose values did not change. The prototype covers
/// append-only inputs and accumulate-only aggregates: a plan with
/// needRetraction, or an input row whose rowKind is not INSERT, fails loud.
class GroupAggregator : public StatefulOperator {
 public:
  /// 'op' is the placeholder batch operator (EmptyOperator); all processing
  /// happens on the streaming path.
  GroupAggregator(
      std::unique_ptr<exec::Operator> op,
      std::vector<StatefulOperatorPtr> targets,
      std::shared_ptr<const GroupAggregationNode> node);

  /// Creates the aggregate functions: operator functionality with no state
  /// dependency, so it belongs to the operator's own init phase rather than
  /// the state-backend handover below.
  void initialize() override;

  /// Builds this operator's keyed state backend from the plan's
  /// KeyedStateSpec and the task backend's key-group parameters, binds the
  /// key selector to the backend's key table, creates the AggregatingState,
  /// and wires the aggregate functions (created in initialize()) to the
  /// state's value rows before any row can be materialized.
  void initializeStateBackend(StateBackend* stateBackend) override;

  /// Probes the batch by key, accumulates the rows into their key's
  /// accumulators and emits the changelog (see the class comment).
  void addInput(StreamElementPtr input) override;

  void close() override;

  std::string name() const override {
    return "GroupAggregator";
  }

  /// The keyed state backend this operator owns. Checkpoint drivers
  /// snapshot and restore the aggregation state through it; the task-level
  /// wiring that persists the checkpoint bytes is delivered separately.
  HeapKeyedStateBackend<RowContainerStateKey>& keyedStateBackend() {
    return *keyedBackend_;
  }

 private:

  // Initializes freshly materialized value rows: one initializeNewGroups
  // per aggregate, over the whole batch. Registered as the descriptor's init
  // callback, so it runs inside the state on each batch of misses.
  void initializeRows(char** rows, size_t numRows);

  // Merges row 0 of 'columns' (one one-row intermediate column per aggregate)
  // into 'row': one addSingleGroupIntermediateResults per aggregate. Registered
  // as the descriptor's merge callback; restore restores one row per keyed
  // entry, so the columns always carry exactly one row.
  void mergeRow(char* row, const std::vector<VectorPtr>& columns);

  // Fails loud on anything but INSERT rows: the prototype has no retraction
  // support, so an updating input cannot be consumed.
  void checkInputRowKinds(const StreamRecord& record) const;

  // Extracts the final values of 'numRows' state rows into freshly created
  // vectors, one per aggregate - the same extraction HashAggregation's
  // output path performs.
  std::vector<VectorPtr> extractFinalValues(char** rows, vector_size_t numRows);

  // Probes the batch by key and resolves every distinct key's state row;
  // misses materialize and initialize fresh accumulators inside the state.
  // Fills the per-batch key scratch (rowToDistinct_, distinctKeyRows_,
  // isNewKey_, stateRows_) and returns the number of distinct keys.
  vector_size_t resolveKeys(const RowVectorPtr& inputVector);

  // Accumulates every input row into its key's state row: one vectorized
  // addRawInput call per aggregate, the groups array aligned with the input
  // rows.
  void accumulateInput(const RowVectorPtr& inputVector, vector_size_t numRows);

  // Plans the changelog emission from the before/after final values and
  // fills emitRowKeys_/emitRowKinds_: new keys emit +I; a changed key emits
  // its update pair, the -U row carrying the previous image immediately
  // ahead of the +U row carrying the new one; unchanged keys emit nothing.
  void planEmission(
      const std::vector<VectorPtr>& prevValues,
      const std::vector<VectorPtr>& newValues);

  // Materializes the emission plan into one output record: the key columns
  // extracted from the key rows of the emitting keys, and each aggregate
  // column copying its image from the matching side of the before/after
  // extraction - previous values on -U rows, new values on +I/+U rows.
  StreamRecordPtr buildEmitRecord(
      const std::vector<VectorPtr>& prevValues,
      const std::vector<VectorPtr>& newValues);

  // Declared before the state members below: the state's callbacks capture
  // these functions by raw pointer, so the aggregates must outlive every
  // state handle and the backend that owns them (members destruct in
  // reverse declaration order, and close() resets the backend first).
  std::vector<exec::AggregateInfo> aggregates_;

  std::shared_ptr<const GroupAggregationNode> node_;
  const uint32_t numKeys_;
  const bool generateUpdateBefore_;

  std::unique_ptr<HeapKeyedStateBackend<RowContainerStateKey>> keyedBackend_;
  std::unique_ptr<KeySelector> keySelector_;
  std::shared_ptr<AggregatingState<RowContainerStateKey, VoidNamespace>> state_;

  // Per-batch scratch, reused across batches.
  std::unordered_map<const char*, vector_size_t> rowToDistinct_;
  std::vector<char*> stateRows_;
  std::vector<char*> rowGroups_;
  std::vector<const char*> distinctKeyRows_;
  // Emission plan: the distinct index each output row reports and its row
  // kind, one entry per output row. A changed key contributes its -U and +U
  // rows back to back; a new key contributes a single +I row.
  std::vector<vector_size_t> emitRowKeys_;
  std::vector<RowKind> emitRowKinds_;
  std::vector<bool> isNewKey_;
};

} // namespace facebook::velox::stateful
