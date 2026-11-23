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
#include <utility>
#include <vector>

#include <folly/Range.h>

#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::stateful {

/// Shared holder for the state row store. The operator and its snapshot
/// serializer both reference it, so the row container and the row type can be
/// adopted from the first runtime batch: the declared plan type of proc-time
/// deduplicate uses TIMESTAMP for the proctime column, while the batch carries
/// the runtime BIGINT representation.
struct DedupRowStore {
  std::shared_ptr<exec::RowContainer> rows;
  RowTypePtr type;
  std::shared_ptr<TypeSerializer<RowVectorPtr>> rowSerializer;
};

/// Snapshot serializer for one deduplicate state row. The rows themselves live
/// in the operator-owned DedupRowStore, and the state table stores the row
/// pointer; only checkpointing materializes and serializes a row, so the hot
/// path never (de)serializes.
class DedupRowStateSerializer : public TypeSerializer<char*> {
 public:
  DedupRowStateSerializer(
      std::shared_ptr<DedupRowStore> store,
      memory::MemoryPool* pool)
      : store_(std::move(store)), pool_(pool) {}

  std::string schemaId() const override {
    return store_->rowSerializer->schemaId() + ":dedup-row-container-v1";
  }

  std::string serialize(char* const& row) override;

  char* deserialize(std::string_view str) override;

 private:
  const std::shared_ptr<DedupRowStore> store_;
  memory::MemoryPool* const pool_;
};

/// Flink RowTimeDeduplicateFunction / ProcTimeDeduplicateKeepFirstRowFunction /
/// ProcTimeDeduplicateKeepLastRowFunction equivalent. Keeps a single
/// representative row per key (earliest/latest by rowtime, or first/last by
/// arrival order) and emits the changelog (INSERT / UPDATE_BEFORE /
/// UPDATE_AFTER) accordingly.
///
/// State layout: every key's representative row is one row of a shared
/// exec::RowContainer (only the deduplicate row columns, no whole input
/// batches), and the state table stores its pointer. This keeps memory
/// proportional to (keys x row width) instead of (input batches x batch
/// width), while new-key rows are appended and existing-key rows are
/// overwritten in place.
class DeduplicateRanker : public exec::Operator {
 public:
  DeduplicateRanker(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::PlanNode>& dedupNode,
      RowTypePtr inputType,
      int64_t minRetentionTime,
      int rowtimeIndex,
      bool generateUpdateBefore,
      bool generateInsert,
      bool keepLastRow);

  // The operator is driven through StreamDeduplicateOperator: input is fed via
  // addInput(StreamElementPtr), and processKeyRows()/processBatch() run the
  // state machine.
  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr input) override {
    // No-op: input is fed through StreamDeduplicateOperator::addInput().
  }

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }

  void close() override;

  /// Snapshot/restore the operator-owned keyed backend. Called through the
  /// exec::Operator virtuals by StatefulOperator::snapshotState/restoreState.
  std::vector<std::string> snapshotState(int64_t checkpointId) override;

  void restoreState(const std::vector<std::string>& checkpointRecords) override;

  /// Creates the value state from the operator-owned keyed state backend. Must
  /// run during initializeStateBackend, before any checkpoint restore, so the
  /// state id is registered when the backend restores its entries.
  void open(HeapKeyedStateBackend<RowContainerStateKey>* backend);

  /// Normalizes a runtime input batch to the declared operator/state type.
  /// Proc-time deduplicate is the motivating case: the plan declares the
  /// proctime column as TIMESTAMP, while the runtime calc produces the
  /// unix_timestamp() BIGINT (epoch seconds). The normalized batch is what
  /// KeySelector, state and output all use, so the state schema and checkpoint
  /// schema are stable and the operator output matches outputType_.
  RowVectorPtr normalizeInput(const RowVectorPtr& input);

  /// Runs the deduplicate state machine over 'input' (one or more rows, all
  /// belonging to 'key', in arrival order). Returns a merged RowVector (user
  /// columns plus a trailing $row_kind column) or nullptr when nothing is
  /// emitted.
  RowVectorPtr processKeyRows(
      const RowContainerStateKey& key,
      RowVectorPtr input);

  /// Runs the state machine over one input batch. distinctKeys[d] is the key
  /// of group d and rowToGroup[row] is that row's group index. Produces one
  /// output batch (or nullptr) for the whole input batch.
  RowVectorPtr processBatch(
      const folly::Range<const RowContainerStateKey*>& distinctKeys,
      const std::vector<int32_t>& rowToGroup,
      RowVectorPtr input);

 private:
  /// One emitted row. For a row of the current input batch, source/input index
  /// are set. For a retracted state row (rowtime mode), stateRow is set until
  /// all referenced state rows are materialized into one batch after the scan;
  /// in proc-time mode the state batch exists up front, so source/index are
  /// set directly.
  struct OutputEntry {
    // Non-owning: all sources (input, stateBatch, lazily materialized state
    // rows) outlive buildOutput(), and the output either copies values or
    // flattens dictionary children. Avoiding a shared_ptr per emitted row
    // removes two atomic refcount operations per output row.
    const RowVector* source{nullptr};
    vector_size_t index{0};
    int8_t kind{0};
    char* stateRow{nullptr};
  };

  /// Batch-scoped state for one key. stateRow is the persistent row pointer
  /// (nullptr for a new key); preTs is the rowtime of that row (rowtime mode);
  /// preInputIndex points at the last accepted row in the current input batch.
  struct KeyState {
    KeyState(RowContainerStateKey key, char* stateRow)
        : key(std::move(key)), stateRow(stateRow) {}

    RowContainerStateKey key;
    char* stateRow{nullptr};
    vector_size_t stateIndex{-1};
    Timestamp preTs{};
    vector_size_t preInputIndex{-1};
    bool dirty{false};
  };

  /// Runs the state machine for one input row and appends emitted rows.
  void processRow(
      KeyState& keyState,
      const RowVectorPtr& input,
      vector_size_t row,
      const DecodedVector* inputTimestamps,
      const RowVectorPtr& stateBatch,
      std::vector<OutputEntry>& outputs);

  /// Batched variant: writes all dirty key rows of one input batch. The
  /// per-column store is issued once for all dirty keys via a dictionary of
  /// their last-accepted input rows, instead of 7 store calls per key.
  void flushStates(std::vector<KeyState>& keyStates, const RowVectorPtr& input);

  /// Materializes the given state rows as one RowVector (column-wise extract).
  RowVectorPtr materializeRows(const std::vector<char*>& rows);

  /// Builds one output batch from the emitted row entries (user columns +
  /// trailing $row_kind).
  RowVectorPtr buildOutput(const std::vector<OutputEntry>& outputs);

  /// Validates that the batch type matches the normalized state type.
  void ensureRowStore(const TypePtr& batchType);

  std::shared_ptr<ValueState<RowContainerStateKey, VoidNamespace, char*>>
      state_;
  std::shared_ptr<DedupRowStore> store_;
  /// Non-owning: StreamDeduplicateOperator owns the backend and outlives the
  /// ranker. Used only by snapshotState/restoreState.
  HeapKeyedStateBackend<RowContainerStateKey>* backend_{nullptr};
  RowTypePtr outputType_;
  int64_t minRetentionTime_;
  int rowtimeIndex_;
  bool generateUpdateBefore_;
  bool generateInsert_;
  bool keepLastRow_;
  bool isRowtime_;
};

} // namespace facebook::velox::stateful
