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
#include "velox/experimental/stateful/rank/DeduplicateRanker.h"

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

namespace {

/// Column-wise value equality of two rows at the given indices. Used for the
/// proc-time keep-last same-value suppression (Flink's RecordEqualiser).
/// Delegates to BaseVector::equalValueAt (null-as-value semantics), which
/// supports all column types including complex ones (array / map / nested
/// row).
bool rowsEqual(
    const RowVectorPtr& lhs,
    vector_size_t lhsIndex,
    const RowVectorPtr& rhs,
    vector_size_t rhsIndex) {
  VELOX_DCHECK_EQ(lhs->childrenSize(), rhs->childrenSize());
  for (auto i = 0; i < lhs->childrenSize(); ++i) {
    if (!lhs->childAt(i)->equalValueAt(
            rhs->childAt(i).get(), lhsIndex, rhsIndex)) {
      return false;
    }
  }
  return true;
}

} // namespace

std::string DedupRowStateSerializer::serialize(char* const& row) {
  VELOX_CHECK_NOT_NULL(row, "Deduplicate state value must not be null");
  const auto& rowType = store_->type;
  // Only snapshot/restore pays this materialization; the hot path stores and
  // reads the RowContainer row directly.
  std::vector<VectorPtr> children;
  children.reserve(rowType->size());
  for (auto i = 0; i < rowType->size(); ++i) {
    children.push_back(BaseVector::create(rowType->childAt(i), 1, pool_));
  }
  auto vec = std::make_shared<RowVector>(
      pool_, rowType, nullptr, 1, std::move(children));
  const char* const rowPtrs[1] = {row};
  for (auto i = 0; i < rowType->size(); ++i) {
    store_->rows->extractColumn(rowPtrs, 1, i, vec->childAt(i));
  }
  return store_->rowSerializer->serialize(vec);
}

char* DedupRowStateSerializer::deserialize(std::string_view str) {
  const auto& rowType = store_->type;
  auto vec = store_->rowSerializer->deserialize(str);
  VELOX_CHECK_NOT_NULL(vec, "Deduplicate snapshot row must not be null");
  VELOX_CHECK_EQ(vec->size(), 1);
  VELOX_CHECK(
      vec->type()->equivalent(*rowType),
      "Deduplicate snapshot row type {} does not match state type {}",
      vec->type()->toString(),
      rowType->toString());
  auto* row = store_->rows->newRow();
  for (auto i = 0; i < rowType->size(); ++i) {
    DecodedVector decoded(*vec->childAt(i));
    store_->rows->store(decoded, 0, row, i);
  }
  return row;
}

DeduplicateRanker::DeduplicateRanker(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& dedupNode,
    RowTypePtr inputType,
    int64_t minRetentionTime,
    int rowtimeIndex,
    bool generateUpdateBefore,
    bool generateInsert,
    bool keepLastRow)
    : exec::Operator(
          driverCtx,
          dedupNode->outputType(),
          operatorId,
          dedupNode->id(),
          "DeduplicateRanker"),
      outputType_(dedupNode->outputType()),
      minRetentionTime_(minRetentionTime),
      rowtimeIndex_(rowtimeIndex),
      generateUpdateBefore_(generateUpdateBefore),
      generateInsert_(generateInsert),
      keepLastRow_(keepLastRow),
      isRowtime_(rowtimeIndex >= 0) {
  VELOX_CHECK_NOT_NULL(inputType, "Deduplicate input type must not be null");
}

void DeduplicateRanker::open(
    HeapKeyedStateBackend<RowContainerStateKey>* backend) {
  VELOX_CHECK_NOT_NULL(backend, "Deduplicate requires a keyed state backend");
  backend_ = backend;
  // TODO: support TTL (stateRetentionTime). State is currently permanent
  // (TTL off); TTL-based expiry needs timer wiring and is deferred.
  auto rowSerializer = std::dynamic_pointer_cast<TypeSerializer<RowVectorPtr>>(
      createSerializer(outputType_, false, pool()));
  VELOX_CHECK_NOT_NULL(
      rowSerializer, "Deduplicate state requires a row value serializer");
  store_ = std::make_shared<DedupRowStore>();
  store_->type = outputType_;
  store_->rows =
      std::make_shared<exec::RowContainer>(store_->type->children(), pool());
  store_->rowSerializer = std::move(rowSerializer);
  ValueStateDescriptor<char*> stateDesc(
      "deduplicate-state",
      std::make_shared<DedupRowStateSerializer>(store_, pool()));
  state_ = backend->getOrCreateValueState<VoidNamespace>(stateDesc);
}

std::vector<std::string> DeduplicateRanker::snapshotState(
    int64_t /*checkpointId*/) {
  VELOX_CHECK_NOT_NULL(
      backend_, "Deduplicate state backend is not initialized before snapshot");
  return {backend_->snapshot()};
}

void DeduplicateRanker::restoreState(
    const std::vector<std::string>& checkpointRecords) {
  VELOX_CHECK_NOT_NULL(
      backend_, "Deduplicate state backend is not initialized before restore");
  for (const auto& record : checkpointRecords) {
    backend_->restore(record);
  }
}

void DeduplicateRanker::ensureRowStore(const TypePtr& batchType) {
  auto rowType = asRowType(batchType);
  VELOX_CHECK(
      store_->type->equivalent(*rowType),
      "Deduplicate batch type {} does not match normalized state type {}; "
      "normalizeInput() must convert the batch before the state machine",
      batchType->toString(),
      store_->type->toString());
}

RowVectorPtr DeduplicateRanker::normalizeInput(const RowVectorPtr& input) {
  if (input->type()->equivalent(*store_->type)) {
    return input;
  }
  VELOX_CHECK_EQ(
      input->childrenSize(),
      store_->type->size(),
      "Deduplicate cannot normalize a batch with a different column count");

  std::vector<VectorPtr> children;
  children.reserve(input->childrenSize());
  for (auto i = 0; i < input->childrenSize(); ++i) {
    const auto& source = input->childAt(i);
    const auto& target = store_->type->childAt(i);
    if (source->type()->equivalent(*target)) {
      children.push_back(source);
      continue;
    }
    // Proc-time: the plan declares the proctime column as TIMESTAMP, while
    // the calc produces unix_timestamp() as BIGINT epoch seconds. Rowtime, if
    // it arrives as BIGINT, is epoch milliseconds (see WatermarkAssigner).
    if (source->type()->kind() == TypeKind::BIGINT &&
        target->kind() == TypeKind::TIMESTAMP) {
      auto converted = BaseVector::create(target, input->size(), pool());
      auto* convertedFlat = converted->asFlatVector<Timestamp>();
      DecodedVector decoded(*source);
      const bool millis = isRowtime_ && i == rowtimeIndex_;
      for (vector_size_t row = 0; row < input->size(); ++row) {
        if (decoded.isNullAt(row)) {
          convertedFlat->setNull(row, true);
        } else {
          const auto value = decoded.valueAt<int64_t>(row);
          convertedFlat->set(
              row, Timestamp::fromMillis(millis ? value : value * 1000));
        }
      }
      children.push_back(std::move(converted));
      continue;
    }
    VELOX_FAIL(
        "Deduplicate cannot normalize column {} from {} to {}",
        i,
        source->type()->toString(),
        target->toString());
  }
  return std::make_shared<RowVector>(
      pool(), store_->type, input->nulls(), input->size(), std::move(children));
}

RowVectorPtr DeduplicateRanker::processKeyRows(
    const RowContainerStateKey& key,
    RowVectorPtr input) {
  // Global deduplicate is just one key covering every row of the batch.
  std::vector<int32_t> rowToGroup(input->size(), 0);
  const RowContainerStateKey* begin = &key;
  return processBatch(
      folly::Range<const RowContainerStateKey*>(begin, begin + 1),
      rowToGroup,
      std::move(input));
}

RowVectorPtr DeduplicateRanker::processBatch(
    const folly::Range<const RowContainerStateKey*>& distinctKeys,
    const std::vector<int32_t>& rowToGroup,
    RowVectorPtr input) {
  VELOX_CHECK_EQ(rowToGroup.size(), input->size());
  ensureRowStore(input->type());
  VELOX_CHECK_EQ(input->childrenSize(), store_->type->size());
  VELOX_CHECK(
      input->type()->equivalent(*store_->type),
      "Deduplicate batch type {} does not match state type {}",
      input->type()->toString(),
      store_->type->toString());
  const auto& ns = VoidNamespace::instance();
  std::vector<OutputEntry> outputs;
  outputs.reserve(input->size() * 2);
  std::unique_ptr<DecodedVector> inputTimestamps;
  if (isRowtime_) {
    inputTimestamps =
        std::make_unique<DecodedVector>(*input->childAt(rowtimeIndex_));
  }
  std::vector<KeyState> keyStates;
  keyStates.reserve(distinctKeys.size());
  for (const auto& key : distinctKeys) {
    keyStates.emplace_back(key, state_->value(key, ns));
  }

  // Rowtime: only the rowtime of each existing state row is needed for
  // comparison; full state rows are materialized later, and only for the rows
  // actually retracted with -U. Proc-time keep-first/no-emit modes only need
  // the existence of a previous state row, not its payload; materialize the
  // proc-time state batch only for the modes that compare whole rows.
  const bool needsProcTimeStateBatch =
      !isRowtime_ && keepLastRow_ && (generateUpdateBefore_ || generateInsert_);
  RowVectorPtr stateBatch;
  if (isRowtime_) {
    std::vector<char*> stateRows;
    std::vector<size_t> stateRowKeys;
    for (size_t i = 0; i < keyStates.size(); ++i) {
      if (keyStates[i].stateRow != nullptr) {
        stateRows.push_back(keyStates[i].stateRow);
        stateRowKeys.push_back(i);
      }
    }
    if (!stateRows.empty()) {
      std::vector<const char*> constRows(stateRows.begin(), stateRows.end());
      auto timestamps =
          BaseVector::create(TIMESTAMP(), stateRows.size(), pool());
      store_->rows->extractColumn(
          constRows.data(),
          static_cast<int32_t>(stateRows.size()),
          rowtimeIndex_,
          timestamps);
      auto* flatTimestamps = timestamps->asFlatVector<Timestamp>();
      for (size_t i = 0; i < stateRowKeys.size(); ++i) {
        keyStates[stateRowKeys[i]].preTs = flatTimestamps->valueAt(i);
      }
    }
  } else if (needsProcTimeStateBatch) {
    std::vector<char*> stateRows;
    for (auto& keyState : keyStates) {
      if (keyState.stateRow != nullptr) {
        keyState.stateIndex = static_cast<vector_size_t>(stateRows.size());
        stateRows.push_back(keyState.stateRow);
      }
    }
    if (!stateRows.empty()) {
      stateBatch = materializeRows(stateRows);
    }
  }

  for (vector_size_t i = 0; i < input->size(); ++i) {
    const auto group = rowToGroup[i];
    VELOX_CHECK_GE(group, 0, "Every input row must belong to a key group");
    VELOX_CHECK_LT(
        static_cast<size_t>(group),
        keyStates.size(),
        "Key group index out of range");
    processRow(
        keyStates[group], input, i, inputTimestamps.get(), stateBatch, outputs);
  }

  RowVectorPtr stateOutput;
  if (isRowtime_) {
    // Materialize the old state rows referenced by -U entries. This must
    // happen before flushStates(), which overwrites those rows in place.
    std::unordered_map<char*, vector_size_t> rowToIndex;
    std::vector<char*> stateRows;
    for (const auto& entry : outputs) {
      if (entry.stateRow != nullptr) {
        auto [it, inserted] =
            rowToIndex.try_emplace(entry.stateRow, stateRows.size());
        if (inserted) {
          stateRows.push_back(entry.stateRow);
        }
      }
    }
    if (!stateRows.empty()) {
      stateOutput = materializeRows(stateRows);
      for (auto& entry : outputs) {
        if (entry.stateRow != nullptr) {
          entry.source = stateOutput.get();
          entry.index = rowToIndex.at(entry.stateRow);
          entry.stateRow = nullptr;
        }
      }
    }
  }

  const auto result = buildOutput(outputs);
  flushStates(keyStates, input);
  return result;
}

void DeduplicateRanker::flushStates(
    std::vector<KeyState>& keyStates,
    const RowVectorPtr& input) {
  std::vector<char*> rows;
  std::vector<vector_size_t> inputRows;
  std::vector<KeyState*> newKeys;
  rows.reserve(keyStates.size());
  inputRows.reserve(keyStates.size());
  newKeys.reserve(keyStates.size());

  for (auto& keyState : keyStates) {
    if (!keyState.dirty) {
      continue;
    }
    VELOX_CHECK_GE(keyState.preInputIndex, 0);
    VELOX_CHECK_LT(keyState.preInputIndex, input->size());
    const bool isNew = keyState.stateRow == nullptr;
    char* row = isNew
        ? store_->rows->newRow()
        : store_->rows->initializeRow(keyState.stateRow, /*reuse=*/true);
    if (isNew) {
      keyState.stateRow = row;
      newKeys.push_back(&keyState);
    }
    rows.push_back(row);
    inputRows.push_back(keyState.preInputIndex);
  }
  if (rows.empty()) {
    return;
  }

  const auto numRows = static_cast<vector_size_t>(rows.size());
  for (auto column = 0; column < store_->type->size(); ++column) {
    auto indices = AlignedBuffer::allocate<vector_size_t>(numRows, pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    for (vector_size_t i = 0; i < numRows; ++i) {
      rawIndices[i] = inputRows[i];
    }
    auto dictionary = BaseVector::wrapInDictionary(
        nullptr, indices, numRows, input->childAt(column));
    DecodedVector decoded(*dictionary);
    store_->rows->store(
        decoded, folly::Range<char**>(rows.data(), rows.size()), column);
  }

  const auto& ns = VoidNamespace::instance();
  for (auto* keyState : newKeys) {
    state_->update(keyState->key, ns, keyState->stateRow);
  }
}

void DeduplicateRanker::processRow(
    KeyState& keyState,
    const RowVectorPtr& input,
    vector_size_t row,
    const DecodedVector* inputTimestamps,
    const RowVectorPtr& stateBatch,
    std::vector<OutputEntry>& outputs) {
  const bool hasPrev =
      keyState.preInputIndex >= 0 || keyState.stateRow != nullptr;
  const bool prevFromState =
      keyState.preInputIndex < 0 && keyState.stateRow != nullptr;

  auto pushPrev = [&](int8_t kind) {
    if (!prevFromState) {
      outputs.push_back({input.get(), keyState.preInputIndex, kind, nullptr});
    } else if (isRowtime_) {
      // Materialized after the scan, before flushStates().
      outputs.push_back({nullptr, 0, kind, keyState.stateRow});
    } else {
      outputs.push_back({stateBatch.get(), keyState.stateIndex, kind, nullptr});
    }
  };
  auto markAccepted = [&]() {
    keyState.preInputIndex = row;
    keyState.dirty = true;
  };

  if (!isRowtime_) {
    // Processing time.
    if (!keepLastRow_) {
      // keep-first: emit only the first row per key, always INSERT.
      if (!hasPrev) {
        outputs.push_back(
            {input.get(), row, static_cast<int8_t>(RowKind::INSERT), nullptr});
        markAccepted();
      }
      return;
    }
    // keep-last: the latest row wins; equal rows are suppressed while TTL is
    // off. When neither UPDATE_BEFORE nor INSERT is generated, the operator is
    // stateless and always emits UPDATE_AFTER.
    if (!(generateUpdateBefore_ || generateInsert_)) {
      outputs.push_back(
          {input.get(),
           row,
           static_cast<int8_t>(RowKind::UPDATE_AFTER),
           nullptr});
      return;
    }
    if (!hasPrev) {
      outputs.push_back(
          {input.get(), row, static_cast<int8_t>(RowKind::INSERT), nullptr});
      markAccepted();
      return;
    }
    const auto& prevSource = prevFromState ? stateBatch : input;
    const auto prevIndex =
        prevFromState ? keyState.stateIndex : keyState.preInputIndex;
    if (rowsEqual(input, row, prevSource, prevIndex)) {
      // Same value, state cleaning disabled: no emission.
      return;
    }
    if (generateUpdateBefore_) {
      pushPrev(static_cast<int8_t>(RowKind::UPDATE_BEFORE));
    }
    outputs.push_back(
        {input.get(),
         row,
         static_cast<int8_t>(RowKind::UPDATE_AFTER),
         nullptr});
    markAccepted();
    return;
  }

  // Event time: compare on the rowtime column.
  VELOX_CHECK_NOT_NULL(inputTimestamps);
  const auto curTs = inputTimestamps->valueAt<Timestamp>(row);
  bool isDuplicate = true; // First row is always emitted.
  if (hasPrev) {
    const auto preTs = prevFromState
        ? keyState.preTs
        : inputTimestamps->valueAt<Timestamp>(keyState.preInputIndex);
    isDuplicate = keepLastRow_ ? curTs >= preTs : curTs < preTs;
  }
  if (!isDuplicate) {
    return;
  }
  if (!hasPrev) {
    outputs.push_back(
        {input.get(),
         row,
         static_cast<int8_t>(
             (generateUpdateBefore_ || generateInsert_)
                 ? RowKind::INSERT
                 : RowKind::UPDATE_AFTER),
         nullptr});
  } else {
    if (generateUpdateBefore_) {
      pushPrev(static_cast<int8_t>(RowKind::UPDATE_BEFORE));
    }
    outputs.push_back(
        {input.get(),
         row,
         static_cast<int8_t>(RowKind::UPDATE_AFTER),
         nullptr});
  }
  markAccepted();
}

RowVectorPtr DeduplicateRanker::materializeRows(
    const std::vector<char*>& rows) {
  VELOX_CHECK_GT(rows.size(), 0);
  const auto size = static_cast<vector_size_t>(rows.size());
  std::vector<const char*> constRows(rows.begin(), rows.end());
  std::vector<VectorPtr> children;
  children.reserve(store_->type->size());
  for (auto i = 0; i < store_->type->size(); ++i) {
    auto child = BaseVector::create(store_->type->childAt(i), size, pool());
    store_->rows->extractColumn(constRows.data(), size, i, child);
    children.push_back(std::move(child));
  }
  return std::make_shared<RowVector>(
      pool(), store_->type, nullptr, size, std::move(children));
}

RowVectorPtr DeduplicateRanker::buildOutput(
    const std::vector<OutputEntry>& outputs) {
  if (outputs.empty()) {
    return nullptr;
  }
  const auto size = static_cast<vector_size_t>(outputs.size());
  // The source rows carry the dedup input type (which may include a trailing
  // PROCTIME column for proc-time dedup), exactly like the previous
  // merge-based implementation. Use that type rather than outputType_, whose
  // column layout can differ for proc-time.
  const auto sourceType = asRowType(outputs.front().source->type());
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> children;
  names.reserve(sourceType->size() + 1);
  types.reserve(sourceType->size() + 1);
  children.reserve(sourceType->size() + 1);
  for (auto i = 0; i < sourceType->size(); ++i) {
    names.emplace_back(sourceType->nameOf(i));
    types.emplace_back(sourceType->childAt(i));
  }

  // Fast path: when every emitted row comes from the same source batch (for
  // example a keep-first batch where all keys are new), reference the source
  // rows through one dictionary per column instead of copying every value.
  const auto* firstSource = outputs.front().source;
  VELOX_CHECK_NOT_NULL(firstSource);
  bool uniformSource = true;
  for (const auto& entry : outputs) {
    if (entry.source != firstSource) {
      uniformSource = false;
      break;
    }
  }
  if (uniformSource) {
    auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    for (vector_size_t j = 0; j < size; ++j) {
      const auto& entry = outputs[j];
      VELOX_CHECK_GE(entry.index, 0);
      VELOX_CHECK_LT(entry.index, entry.source->size());
      rawIndices[j] = entry.index;
    }
    for (auto i = 0; i < sourceType->size(); ++i) {
      auto child = BaseVector::wrapInDictionary(
          nullptr, indices, size, firstSource->childAt(i));
      // Keep the operator's external representation flat: downstream and the
      // existing tests read output children as flat vectors. The gather is
      // still done in bulk rather than one copy() call per output row.
      BaseVector::flattenVector(child);
      children.emplace_back(std::move(child));
    }
  } else {
    for (auto i = 0; i < sourceType->size(); ++i) {
      auto child = BaseVector::create(sourceType->childAt(i), size, pool());
      for (vector_size_t j = 0; j < size; ++j) {
        const auto& entry = outputs[j];
        VELOX_CHECK(
            sourceType->equivalent(*entry.source->type()),
            "Deduplicate output sources have mixed types: {} vs {}",
            sourceType->toString(),
            entry.source->type()->toString());
        VELOX_CHECK_GE(entry.index, 0);
        VELOX_CHECK_LT(entry.index, entry.source->size());
        child->copy(entry.source->childAt(i).get(), j, entry.index, 1);
      }
      children.emplace_back(std::move(child));
    }
  }
  names.emplace_back(std::string(kRowKindColumnName));
  types.emplace_back(TINYINT());
  auto kindVec =
      BaseVector::create<FlatVector<int8_t>>(TINYINT(), size, pool());
  for (vector_size_t j = 0; j < size; ++j) {
    kindVec->set(j, outputs[j].kind);
  }
  children.emplace_back(std::move(kindVec));
  auto mergedType = ROW(std::move(names), std::move(types));
  return std::make_shared<RowVector>(
      pool(), mergedType, nullptr, size, std::move(children));
}

void DeduplicateRanker::close() {
  exec::Operator::close();
  // state_ is created in open(), which no longer runs when the wrapping
  // operator's initializeState fails fast; it can be null at close time.
  if (state_) {
    state_->clear();
    state_.reset();
  }
  // The RowContainer owns all state rows (including variable-width payloads);
  // the serializer may still hold a reference until the backend is reset.
  store_.reset();
  backend_ = nullptr;
}

} // namespace facebook::velox::stateful
