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
#include <vector>

#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/rank/CompactRow.h"
#include "velox/experimental/stateful/rank/TopNCache.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/HeapListState.h"
#include "velox/experimental/stateful/state/HeapValueState.h"
#include "velox/experimental/stateful/state/StateBackend.h"

namespace facebook::velox::stateful {

class RankNode;
class RankOperator;

namespace test {
class RankOperatorTest;
} // namespace test

/// Placeholder exec::Operator of the rank task: the operator_ slot of
/// RankOperator. Carries no data-plane behavior (rank processes whole
/// StreamRecords in RankOperator::addInput); it exists so the checkpoint
/// walk of StatefulOperator reaches the rank keyed backend through the
/// exec::Operator virtual snapshotState/restoreState hooks - the same shape
/// as the CheckpointSpyOperator precedent in StatefulOperatorCheckpointTest.
/// The wrapped operator owns nothing; the closures delegate to RankOperator.
class RankCheckpointOperator : public exec::Operator {
 public:
  RankCheckpointOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const RankNode>& node);

  void initialize() override {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr /*input*/) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }

  /// Wired by the RankOperator constructor right after the base class stores
  /// the wrapped operator (design D2/gate-8: the placeholder holds the
  /// checkpoint closures, keyedBackend_ readiness precedes the first
  /// checkpoint because initializeStateBackend runs at task init).
  void setRankOperator(RankOperator* rankOperator) {
    rankOperator_ = rankOperator;
  }

  std::vector<std::string> snapshotState(int64_t checkpointId) override;

  void restoreState(const std::vector<std::string>& checkpointRecords) override;

 private:
  RankOperator* rankOperator_ = nullptr;
};

/// Typed keyed rank operator covering the Flink Rank shapes
/// (grouped/global x FirstN/TopN/Top1) for ROW_NUMBER over a constant rank
/// range with insert-only input. One class with a three-way dispatch: the
/// FirstN branch mirrors AppendOnlyFirstNFunction (per-key counter, +I
/// only), the Top1 branch mirrors FastTop1Function (ValueState, -U/+U even
/// without a rank-number column), the TopN branches mirror
/// AppendOnlyTopNFunction (ordered buffer + rank displacement chain with the
/// number, eager eviction -D/+I without it) - the offset rank range
/// (rankStart > 1) forces the with-number algorithm and gates every
/// rank-bearing emission through isInRankRange. Ordering lives in the
/// per-key buffer; the state only stores the rows (Top1: ValueState, TopN:
/// ListState, FirstN: single-element ListState of int64, design D1 of the
/// rank2 task).
///
/// Row storage is the compact form of the perf-rank-opt task: every stored
/// row is one contiguous CompactRow blob (single heap allocation, shared by
/// the state list, the TopNCache entry and the emissions - zero copies
/// across the three holders), sort-key reads are O(1) slot loads, and the
/// TopN buffers live in a bounded LRU (TopNCache, write-through: the state
/// list keeps every row, a miss replays it in arrival order).
class RankOperator : public StatefulOperator {
 public:
  RankOperator(
      std::unique_ptr<exec::Operator> op,
      std::vector<StatefulOperatorPtr> targets,
      std::shared_ptr<const RankNode> node);

  void initialize() override;

  void initializeStateBackend(StateBackend* stateBackend) override;

  void addInput(StreamElementPtr input) override;

  /// Rank emits from addInput; there is nothing to pull from the wrapped
  /// placeholder operator, so the base advance() (getOutput-driven) would be
  /// a no-op that only misreports sourceEmpty_ on a mid-chain operator.
  void advance() override {}

  void close() override;

  /// Checkpoint plane, reached only through RankCheckpointOperator.
  std::vector<std::string> snapshotRankState(int64_t checkpointId);

  void restoreRankState(const std::vector<std::string>& checkpointRecords);

  /// State introspection for the unit tests (upper-bound and rescale
  /// assertions of the design test matrix).
  friend class test::RankOperatorTest;

 private:
  /// One planned changelog row: the stored compact row, its target Flink
  /// RowKind and (when outputRankNumber) the rank value it carries.
  struct EmitRow {
    CompactRowPtr row;
    RowKind kind;
    int64_t rank;
  };

  /// Per-key ordered buffer of stored rows (the TopNBuffer mirror): rows
  /// sorted by the sort-key comparator, ties in arrival order. Keyed by the
  /// key row pointer - one key is one row of the backend-owned key table.
  using TopNBuffer = TopNCache::TopNBuffer;

  // addInput steps (design S3.2 seven-step pattern).
  void checkInputRowKinds(const StreamRecord& record) const;

  /// Encodes input row 'row' (out of the batch-decoded columns) into one
  /// compact row - the inputRowSer.copy mirror of Flink's storage write
  /// (design alignment #59): one contiguous blob, one heap allocation.
  CompactRowPtr encodeRow(vector_size_t row);

  /// Batch-decodes every input column of the current batch once (the
  /// per-compare DecodedVector construction of the stored-vector shape is
  /// gone); the comparator reads input rows through these.
  void decodeInputBatch(const RowVectorPtr& input);

  TopNBuffer& bufferFor(const RowContainerStateKey& key);

  void processTop1Row(const RowContainerStateKey& key, vector_size_t inputRow);

  // The FirstN fast path (Flink AppendOnlyFirstNFunction): a per-key
  // counter admits the first rankEnd_ rows and drops the rest; every
  // admitted row emits +I (rank = counter + 1 when the number column or
  // the offset window carries a rank, plain +I otherwise). Batch form:
  // the counter gates every row before any row work, so no admitted or
  // dropped row pays a data copy, and the emitted batch dictionary-wraps
  // the input columns (AppendOnlyFirstN emits the incoming row by
  // reference; the wrap keeps the output sharing the input's buffers).
  void processFirstNRows(
      const RowVectorPtr& input,
      folly::Range<const RowContainerStateKey*> keys,
      vector_size_t numRows);

  void processTopNWithRowNumber(
      const RowContainerStateKey& key,
      vector_size_t inputRow);

  void processTopNWithoutRowNumber(
      const RowContainerStateKey& key,
      vector_size_t inputRow);

  bool checkRange(vector_size_t inputRow, const TopNBuffer& buffer) const;

  /// First position where the input row sorts strictly before the stored
  /// row; equal sort keys keep the stored rows first, so a tie lands after
  /// them (TopNBuffer list tail-insert semantics).
  size_t insertionPosition(vector_size_t inputRow, const TopNBuffer& buffer)
      const;

  /// The stored-row form of insertionPosition (the lazy rebuild compares
  /// two compact rows).
  size_t insertionPosition(const CompactRowPtr& row, const TopNBuffer& buffer)
      const;

  /// Column-wise comparison of the sort-key channels (ascending x
  /// nullsFirst per column), input-batch row against a stored compact row.
  /// Returns <0 / 0 / >0.
  int compareSortKeys(vector_size_t inputRow, const CompactRowPtr& stored)
      const;

  /// The stored x stored form (lazy rebuild, Top1 against the stored best).
  int compareSortKeys(const CompactRowPtr& lhs, const CompactRowPtr& rhs) const;

  /// Flink's input.equals(lastElement) of the no-rank-number eviction:
  /// defensive dead code, kept in the same position as Flink's equals
  /// check - a full buffer only admits strictly-better rows through
  /// checkRange, so the value-equal branch is unreachable in practice.
  bool sameStoredRow(const CompactRowPtr& lhs, const CompactRowPtr& rhs) const {
    return lhs == rhs;
  }

  void rewriteListState(
      const RowContainerStateKey& key,
      const TopNBuffer& buffer);

  void emit(CompactRowPtr row, RowKind kind, int64_t rank);

  RowVectorPtr buildEmitRowVector(const std::vector<EmitRow>& emissions);

  /// Columnar decode of one emitted input column out of the compact rows:
  /// a fresh output column per kind, fixed-width slots read directly,
  /// strings copied into the output vector's own string buffers (design
  /// S3.2 step 6).
  VectorPtr decodeEmitColumn(
      const std::vector<EmitRow>& emissions,
      column_index_t c);

  template <typename T>
  void fillEmitColumn(
      const std::vector<EmitRow>& emissions,
      column_index_t c,
      FlatVector<T>& column) const;

  std::shared_ptr<FlatVector<int8_t>> buildEmitRowKinds(
      const std::vector<EmitRow>& emissions);

  const std::shared_ptr<const RankNode> node_;
  const int64_t rankStart_;
  const int64_t rankEnd_;
  const bool firstN_;
  const bool isTop1_;
  const bool hasOffset_;
  const bool outputRankNumber_;
  const bool generateUpdateBefore_;
  /// LRU key bound: max(1, cacheSize / rankEnd) - the
  /// AppendOnlyTopNFunction#open fold of the plan's cacheSize (the constant
  /// rank range's default top N size equals rankEnd, design alignment #61).
  const size_t lruCapacity_;
  const VoidNamespace ns_ = VoidNamespace::instance();

  /// Slot offset table of the compact row layout (built per input row type,
  /// design S3.1).
  std::unique_ptr<CompactRowLayout> compactLayout_;
  /// Batch-decoded input columns of the current addInput batch (filled by
  /// decodeInputBatch, read by encodeRow and the input-side comparator);
  /// mutable because the const compare path reads the batch state.
  mutable std::vector<DecodedVector> decodedInput_;
  std::unique_ptr<HeapKeyedStateBackend<RowContainerStateKey>> keyedBackend_;
  std::unique_ptr<KeySelector> keySelector_;
  std::shared_ptr<ListState<RowContainerStateKey, VoidNamespace, int64_t>>
      counterState_;
  std::shared_ptr<
      ValueState<RowContainerStateKey, VoidNamespace, CompactRowPtr>>
      valueState_;
  std::shared_ptr<ListState<RowContainerStateKey, VoidNamespace, CompactRowPtr>>
      listState_;

  /// Bounded LRU of the per-key ordered TopN buffers; absent for Top1
  /// (typed ValueState direct reads carry no deserialization tax, design
  /// alignment #66) and FirstN (no row storage).
  std::unique_ptr<TopNCache> cache_;

  /// Changelog rows planned by the current input batch (design S5:
  /// batch-granularity output, row-granularity semantics).
  std::vector<EmitRow> emissions_;
};

} // namespace facebook::velox::stateful
