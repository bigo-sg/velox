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
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/State.h"

namespace facebook::velox::stateful {

// Defined in GroupAggregator.cpp; the record-count channel of a retracting
// plan (see GroupAggregator::initialize).
class RecordCounter;

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
/// and nothing for keys whose values did not change. A plan with
/// needRetraction requires every aggregate to implement RetractableAggregate
/// and fails loud at initialization otherwise; its input rows split by
/// rowKind (+I/+U accumulate, -U/-D retract), a reused-or-hidden count(*)
/// column carries each key's record count, and the count reaching zero
/// emits -D (carrying the previous values) and clears the key's state. A
/// plan without needRetraction receives insert-only input by the planner's
/// contract and takes the plain emission path.
class GroupAggregator : public StatefulOperator {
 public:
  /// 'op' is the placeholder batch operator (EmptyOperator); all processing
  /// happens on the streaming path.
  GroupAggregator(
      std::unique_ptr<exec::Operator> op,
      std::vector<StatefulOperatorPtr> targets,
      std::shared_ptr<const GroupAggregationNode> node);

  /// Defined in the .cpp, where RecordCounter is complete: the record-count
  /// handle holds a pointer into aggregates_.
  ~GroupAggregator() override;

  /// Creates the aggregate functions: operator functionality with no state
  /// dependency, so it belongs to the operator's own init phase rather than
  /// the state-backend handover below.
  void initialize() override;

  /// Builds this operator's keyed state backend from the plan's
  /// KeyedStateSpec and the task backend's key-group parameters, binds the
  /// key selector to the backend's key table and creates the
  /// AggregatingState from a descriptor carrying the aggregate functions
  /// (created in initialize()); the state wires each function to its value
  /// rows, before any row can be materialized.
  void initializeStateBackend(StateBackend* stateBackend) override;

  /// Probes the batch by key, accumulates the rows into their key's
  /// accumulators and emits the changelog (see the class comment).
  void addInput(StreamElementPtr input) override;

  void close() override;

  std::string name() const override {
    return "GroupAggregator";
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

  // Splits the batch rows by rowKind into the accumulate selection (+I/+U)
  // and the retract selection (-U/-D) - the vectorized form of Flink's
  // isAccumulateMsg split. An append-only record accumulates every row.
  void splitByRowKind(
      const StreamRecord& record,
      SelectivityVector& accRows,
      SelectivityVector& retractRows) const;

  // Extracts the final values of 'numRows' state rows into freshly created
  // vectors, one per plan-visible aggregate (the hidden record-count column
  // of a retracting plan has no output) - the same extraction
  // HashAggregation's output path performs.
  std::vector<VectorPtr> extractFinalValues(char** rows, vector_size_t numRows);

  // Probes the batch by key and resolves every distinct key's state row;
  // misses materialize and initialize fresh accumulators inside the state.
  // Fills the per-batch key scratch (rowToDistinct_, isNewKey_, stateRows_)
  // and returns the number of distinct keys.
  vector_size_t resolveKeys(const RowVectorPtr& inputVector);

  // Accumulates / retracts the selected rows into their keys' state rows:
  // one vectorized addRawInput / retractRawInput call per aggregate over the
  // same groups/mask/argument plumbing (see applyInput).
  void accumulateInput(
      const RowVectorPtr& inputVector,
      const SelectivityVector& rows);
  void retractInput(
      const RowVectorPtr& inputVector,
      const SelectivityVector& rows);

  // Shared driver of the accumulate/retract input paths: resolves every
  // input row's state row into the groups array, filters 'rows' through the
  // aggregation masks, and hands each aggregate its selected rows -
  // addRawInput, or retractRawInput through the RetractableAggregate mix-in
  // (the initialize() probe guarantees the cast on a retracting plan).
  void applyInput(
      const RowVectorPtr& inputVector,
      const SelectivityVector& rows,
      bool retract);

  // Whether any aggregate's final value differs between the before/after
  // extraction of distinct key 'd'.
  bool valuesChanged(
      const std::vector<VectorPtr>& prevValues,
      const std::vector<VectorPtr>& newValues,
      vector_size_t d);

  // Plans the changelog emission and fills emitRowKeys_/emitRowKinds_ (and
  // keysToClear_ on a retracting plan). An insert-only plan emits +I for
  // physically new keys and the update pair for changed existing keys. A
  // retracting plan decides by the record count (RecordCounter): a fresh key
  // (before == 0) emits +I when net-positive and is silently cleared
  // otherwise (Flink drops a first message that retracts); a live key whose
  // count reaches exactly zero emits -D carrying the previous values and is
  // cleared; any other live key (negative counts included - zero is the
  // only death) emits the plain update pair when its values changed.
  void planEmission(
      const std::vector<VectorPtr>& prevValues,
      const std::vector<VectorPtr>& newValues,
      const VectorPtr& countBefore,
      const VectorPtr& countAfter);

  // Materializes the emission plan into one output record: the key columns
  // extracted from the key rows of the emitting keys, and each aggregate
  // column copying its image from the matching side of the before/after
  // extraction - previous values on -U/-D rows, new values on +I/+U rows.
  StreamRecordPtr buildEmitRecord(
      const std::vector<VectorPtr>& prevValues,
      const std::vector<VectorPtr>& newValues);

  // Declared before the state members below: the state's callbacks capture
  // these functions by raw pointer, so the aggregates must outlive every
  // state handle (members destruct in reverse declaration order). The
  // owned backend lives in the StatefulOperator base and therefore
  // destructs after these members; that ordering is safe because
  // destruction never fires the descriptor callbacks (they run only on
  // materialize and restore), and close() resets the backend explicitly
  // before teardown.
  std::vector<exec::AggregateInfo> aggregates_;

  std::shared_ptr<const GroupAggregationNode> node_;
  const uint32_t numKeys_;
  const bool generateUpdateBefore_;
  const bool needRetraction_;

  // The record-count channel of a retracting plan, over a qualifying count(*)
  // the plan already carries or the hidden count appended in initialize();
  // null on insert-only plans, which take the plain emission path.
  std::unique_ptr<RecordCounter> recordCounter_;

  std::unique_ptr<KeySelector> keySelector_;
  std::shared_ptr<AggregatingState<RowContainerStateKey>> state_;

  // Per-batch scratch, reused across batches.
  std::unordered_map<const char*, vector_size_t> rowToDistinct_;
  std::vector<char*> stateRows_;
  std::vector<char*> rowGroups_;
  // Emission plan: the distinct index each output row reports and its row
  // kind, one entry per output row. A changed key contributes its -U and +U
  // rows back to back; a new key contributes a single +I row.
  std::vector<vector_size_t> emitRowKeys_;
  std::vector<RowKind> emitRowKinds_;
  // Distinct keys whose state the emission plan retires this batch (-D rows
  // and silently-cleaned fresh keys); addInput drops them - hidden count
  // column included - after pushing the output record.
  std::vector<vector_size_t> keysToClear_;
  std::vector<bool> isNewKey_;
};

} // namespace facebook::velox::stateful
