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
#include "velox/experimental/stateful/rank/StreamDeduplicateOperator.h"

#include <cstdint>
#include <vector>

#include "velox/experimental/stateful/KeyedStateSpec.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StreamElement.h"

namespace facebook::velox::stateful {

StreamDeduplicateOperator::StreamDeduplicateOperator(
    std::unique_ptr<DeduplicateRanker> ranker,
    std::vector<column_index_t> keyChannels,
    std::vector<TypePtr> keyTypes,
    std::vector<std::unique_ptr<StatefulOperator>> targets)
    : StatefulOperator(std::move(ranker), std::move(targets)),
      ranker_(dynamic_cast<DeduplicateRanker*>(op().get())),
      keyChannels_(std::move(keyChannels)),
      keyTypes_(std::move(keyTypes)),
      global_(keyChannels_.empty()) {
  VELOX_CHECK_NOT_NULL(ranker_);
}

void StreamDeduplicateOperator::initializeStateBackend(
    StateBackend* stateBackend) {
  // Global deduplicate has no user key columns; synthesize a single constant
  // BIGINT key so the keyed state machinery (key table, snapshot, restore)
  // still applies.
  auto backendKeyTypes = global_ ? std::vector<TypePtr>{BIGINT()} : keyTypes_;
  auto backendKeyChannels =
      global_ ? std::vector<column_index_t>{0} : keyChannels_;
  KeyedStateSpec spec(backendKeyChannels, backendKeyTypes);
  backend_ = createKeyedStateBackend<RowContainerStateKey>(
      *stateBackend, spec, memoryPool());
  if (global_) {
    auto constantKey =
        BaseVector::createConstant(BIGINT(), int64_t(0), 1, memoryPool());
    globalKey_.emplace(backend_->keyIndex().probeKey({constantKey}));
  } else {
    keySelector_ =
        std::make_unique<KeySelector>(backend_->keyIndex(), keyChannels_);
  }
  // Open before any checkpoint restore: StatefulTask restores the
  // checkpoint records before initializeState(), and the backend can only
  // restore entries into states that are already registered.
  ranker_->open(backend_.get());
  StatefulOperator::initializeStateBackend(stateBackend);
}

void StreamDeduplicateOperator::initializeState() {
  StatefulOperator::initializeState();
}

void StreamDeduplicateOperator::addInput(StreamElementPtr input) {
  VELOX_CHECK_NULL(input_);
  auto record = std::static_pointer_cast<StreamRecord>(input);
  // Deduplicate supports append-only (INSERT) input only, mirroring Flink's
  // checkInsertOnly. A record whose $row_kind is a flat per-row column (rather
  // than a constant) still counts as insert-only as long as every row is
  // INSERT; only retract/update kinds must be rejected.
  if (!record->appendOnly()) {
    auto* rowKind = record->rowKind().get();
    VELOX_USER_CHECK_NOT_NULL(rowKind);
    for (vector_size_t i = 0; i < rowKind->size(); ++i) {
      VELOX_USER_CHECK(
          rowKind->valueAt(i) == static_cast<int8_t>(RowKind::INSERT),
          "Deduplicate requires append-only (INSERT) input");
    }
  }
  input_ = record->record();
}

void StreamDeduplicateOperator::advance() {
  input_ = ranker_->normalizeInput(input_);
  if (global_) {
    auto result = ranker_->processKeyRows(*globalKey_, input_);
    if (result) {
      pushOutput(StreamRecord::create(getPlanNodeId(), std::move(result)));
    }
  } else {
    keySelector_->probe(input_);
    const auto distinctKeys = keySelector_->distinctKeys();
    const auto& groupRows = keySelector_->groupRows();
    VELOX_CHECK_EQ(groupRows.size(), distinctKeys.size());
    // rowToGroup[row] is the distinct-key group d with row in groupRows()[d].
    // Every input row must belong to exactly one group; a -1 would make the
    // ranker index out of bounds, so check instead of trusting the selector.
    std::vector<int32_t> rowToGroup(input_->size(), -1);
    for (size_t d = 0; d < groupRows.size(); ++d) {
      groupRows[d].applyToSelected([&](vector_size_t row) {
        VELOX_CHECK_GE(row, 0);
        VELOX_CHECK_LT(row, input_->size());
        rowToGroup[row] = static_cast<int32_t>(d);
      });
    }
    for (vector_size_t i = 0; i < input_->size(); ++i) {
      VELOX_CHECK_GE(
          rowToGroup[i], 0, "KeySelector left input row {} unassigned", i);
    }
    auto result = ranker_->processBatch(distinctKeys, rowToGroup, input_);
    if (result) {
      pushOutput(StreamRecord::create(getPlanNodeId(), std::move(result)));
    }
  }
  input_.reset();
  backend_->advance();
  // The dedup state keeps its referenced rows alive, but the input batch and
  // the per-batch probe/output buffers are dead here. Return their unused
  // memory to the allocator instead of letting the pool high-water mark track
  // the whole run.
  memoryPool()->release();
}

void StreamDeduplicateOperator::close() {
  // The ranker's close() clears its state handle, which references the
  // backend's state table; reset the backend only after the wrapped operator
  // is closed (and its state handle released).
  StatefulOperator::close();
  backend_.reset();
  keySelector_.reset();
  input_.reset();
}

} // namespace facebook::velox::stateful
