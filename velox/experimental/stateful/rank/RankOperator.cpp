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
#include "velox/experimental/stateful/rank/RankOperator.h"

#include <algorithm>

#include "velox/buffer/Buffer.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

RankCheckpointOperator::RankCheckpointOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const RankNode>& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "RankCheckpointOperator") {}

std::vector<std::string> RankCheckpointOperator::snapshotState(
    int64_t checkpointId) {
  VELOX_CHECK_NOT_NULL(
      rankOperator_, "RankCheckpointOperator is not wired to its RankOperator");
  return rankOperator_->snapshotRankState(checkpointId);
}

void RankCheckpointOperator::restoreState(
    const std::vector<std::string>& checkpointRecords) {
  VELOX_CHECK_NOT_NULL(
      rankOperator_, "RankCheckpointOperator is not wired to its RankOperator");
  rankOperator_->restoreRankState(checkpointRecords);
}

RankOperator::RankOperator(
    std::unique_ptr<exec::Operator> wrappedOp,
    std::vector<StatefulOperatorPtr> targets,
    std::shared_ptr<const RankNode> node)
    : StatefulOperator(std::move(wrappedOp), std::move(targets)),
      node_(std::move(node)),
      rankStart_(node_->rankStart()),
      rankEnd_(node_->rankEnd()),
      firstN_(node_->firstN()),
      isTop1_(node_->rankStart() == 1 && node_->rankEnd() == 1),
      hasOffset_(node_->rankStart() > 1),
      outputRankNumber_(node_->outputRankNumber()),
      generateUpdateBefore_(node_->generateUpdateBefore()),
      sentinelKey_(
          node_->keyedStateSpec()->keyChannels().size() == 1 &&
          node_->keyedStateSpec()->keyChannels()[0] ==
              node_->inputType()->size()),
      // LRU key bound: max(1, cacheSize / rankEnd), the
      // AppendOnlyTopNFunction#open fold - the constant rank range's
      // default top N size equals rankEnd (design alignment #61; the
      // constructor CHECK pins rankEnd >= rankStart >= 1).
      lruCapacity_(static_cast<size_t>(
          std::max<int64_t>(1, node_->cacheSize() / node_->rankEnd()))) {
  // Plan constraints pinned at construction (implementation contract).
  VELOX_CHECK_EQ(
      node_->sortKeyChannels().size(),
      node_->sortAscending().size(),
      "RankNode needs one ascending flag per sort key channel");
  VELOX_CHECK_EQ(
      node_->sortKeyChannels().size(),
      node_->sortNullsFirst().size(),
      "RankNode needs one nullsFirst flag per sort key channel");
  VELOX_CHECK_GE(
      node_->rankStart(),
      1,
      "Rank rank range is 1-based: got start {}",
      node_->rankStart());
  VELOX_CHECK_GE(
      node_->rankEnd(),
      node_->rankStart(),
      "Rank rank range end {} is before its start {}",
      node_->rankEnd(),
      node_->rankStart());
  for (const auto channel : node_->sortKeyChannels()) {
    VELOX_CHECK_LT(
        channel,
        node_->inputType()->size(),
        "Rank sort key channel {} is outside the input schema",
        channel);
  }
  const auto numInputCols = node_->inputType()->size();
  for (const auto channel : node_->keyedStateSpec()->keyChannels()) {
    // The sentinel key channel of the global shape (single channel, one
    // past the schema, design D3) is the one out-of-schema channel;
    // anything else fails loud here instead of inside the first probe.
    VELOX_CHECK(
        channel < numInputCols || (sentinelKey_ && channel == numInputCols),
        "Rank key channel {} is outside the input schema ({} columns)",
        channel,
        numInputCols);
  }
  if (outputRankNumber_) {
    VELOX_CHECK_EQ(
        node_->outputType()->size(),
        node_->inputType()->size() + 1,
        "outputRankNumber requires exactly one trailing rank column");
  } else {
    VELOX_CHECK_EQ(
        node_->outputType()->size(),
        node_->inputType()->size(),
        "without outputRankNumber the output schema must equal the input");
  }
  // Wire the checkpoint closures (design gate-8: the placeholder operator
  // overrides the exec::Operator virtual snapshotState/restoreState).
  auto* checkpointOperator = dynamic_cast<RankCheckpointOperator*>(op().get());
  VELOX_CHECK_NOT_NULL(
      checkpointOperator,
      "RankOperator requires a RankCheckpointOperator as its wrapped operator");
  checkpointOperator->setRankOperator(this);
}

void RankOperator::initialize() {
  StatefulOperator::initialize();
  // Stateless facilities: the sort specification lives on the node; the
  // global shape additionally probes through a sentinel BIGINT tail column
  // appended to each input batch (design D3), so the augmented row type is
  // prepared here. The compact layout's slot offset table is also per input
  // row type (design S3.1) - built once, before the first addInput.
  if (sentinelKey_) {
    auto names = node_->inputType()->names();
    auto types = node_->inputType()->children();
    names.emplace_back("$rank_sentinel_key");
    types.emplace_back(BIGINT());
    augmentedInputType_ = ROW(std::move(names), std::move(types));
  }
  compactLayout_ = std::make_unique<CompactRowLayout>(node_->inputType());
}

void RankOperator::initializeStateBackend(StateBackend* stateBackend) {
  const auto& spec = *node_->keyedStateSpec();
  // Step 1: the one creation entry. ROCKSDB fails loud inside it
  // (design limit L1).
  keyedBackend_ = createKeyedStateBackend<RowContainerStateKey>(
      *stateBackend, spec, memoryPool());
  // Step 2: bind the key selector to the backend-owned key table.
  keySelector_ = std::make_unique<KeySelector>(
      keyedBackend_->keyIndex(), spec.keyChannels());
  // Step 3: descriptors + typed handles. The three shapes are mutually
  // exclusive, exactly one handle is registered per operator: FirstN counts
  // with a single-element int64 list (rank2 design D1 - ValueState<int64_t>
  // cannot express a miss, int64_t is not a nullable pointer), Top1 stores
  // the best row in a ValueState, TopN stores the ordered rows in a
  // ListState. Both row-storing shapes serialize the compact form
  // (CompactRowSerializer, design S3.1) - the checkpoint value schema
  // becomes "compact-row:v1:<inputType>" (design S6, the pre-cache
  // "presto:ROW(...)" format retires with the fail-loud schema CHECK).
  if (firstN_) {
    ListStateDescriptor<int64_t> descriptor(
        "rank-first-n-counter",
        std::make_shared<ValueSerializer<int64_t>>(),
        memoryPool());
    counterState_ =
        keyedBackend_->getOrCreateListState<VoidNamespace>(descriptor);
  } else if (isTop1_) {
    ValueStateDescriptor<CompactRowPtr> descriptor(
        "rank-top-1",
        makeCompactRowSerializer(node_->inputType()),
        memoryPool());
    valueState_ =
        keyedBackend_->getOrCreateValueState<VoidNamespace>(descriptor);
  } else {
    ListStateDescriptor<CompactRowPtr> descriptor(
        "rank-top-n",
        makeCompactRowSerializer(node_->inputType()),
        memoryPool());
    listState_ = keyedBackend_->getOrCreateListState<VoidNamespace>(descriptor);
  }
  // The TopN cache is created after the handles, before the first addInput
  // (design S3.2): a write-through bounded LRU over the list state - Top1
  // needs no cache layer (typed ValueState direct reads carry no
  // deserialization tax, design alignment #66), FirstN stores no rows.
  if (!firstN_ && !isTop1_) {
    cache_ = std::make_unique<TopNCache>(lruCapacity_);
  }
  // Step 4: AggregatingState wiring does not apply (Value/List handles have
  // no accumulator layout).
  // Step 5: forward to the base class (targets).
  StatefulOperator::initializeStateBackend(stateBackend);
}

void RankOperator::addInput(StreamElementPtr input) {
  // 1. Cast and validate the row kinds: insert-only, appendOnly counts as
  // all-INSERT; anything else fails loud.
  auto record = std::static_pointer_cast<StreamRecord>(input);
  checkInputRowKinds(*record);
  const auto& inputVector = record->record();
  const auto numRows = inputVector->size();
  if (numRows == 0) {
    return;
  }
  emissions_.clear();

  // 2. Probe: the global shape carries the sentinel tail column (design D3).
  const RowVectorPtr probeInput =
      sentinelKey_ ? withSentinelKeyColumn(inputVector) : inputVector;
  keySelector_->probe(probeInput);
  const auto keys = keySelector_->keys();

  // 3/4/5. Row-granular semantics inside the batch (design alignment #5):
  // state resolution and business logic run per row, changelog rows keep
  // the batch's row order. FirstN runs as a batch-level zero-copy branch
  // (the counter path stores no rows, so it needs none of the per-row
  // encodes); the row-storing shapes batch-decode every input column once
  // (encodeRow and the input-side comparator read through the same decoded
  // columns - the per-compare DecodedVector construction of the stored-
  // vector shape is gone, design S3.2) and keep the per-row dispatch order
  // mirroring the native routing (StreamExecRank): Top1, then TopN - where
  // an offset rank range (hasOffset_) forces the with-row-number algorithm
  // because the without-number eviction cannot filter inner ranks
  // (alignment #35).
  if (firstN_) {
    processFirstNRows(inputVector, keys, numRows);
  } else {
    decodeInputBatch(inputVector);
    for (vector_size_t i = 0; i < numRows; ++i) {
      if (isTop1_) {
        processTop1Row(keys[i], i);
      } else if (outputRankNumber_ || hasOffset_) {
        processTopNWithRowNumber(keys[i], i);
      } else {
        processTopNWithoutRowNumber(keys[i], i);
      }
    }
  }

  // 6. Assemble one changelog record for the whole batch and emit it.
  if (!emissions_.empty()) {
    auto output = std::make_shared<StreamRecord>(
        getPlanNodeId(),
        buildEmitRowVector(emissions_),
        buildEmitRowKinds(emissions_));
    pushOutput(std::move(output));
  }

  // 7. Batch boundary: drop cache entries whose state is empty (their key
  // rows would dangle after reclamation), then let the backend reclaim dead
  // key rows (design alignments #73/#76; the order is unchanged).
  if (cache_) {
    cache_->eraseEmpty();
  }
  keyedBackend_->advance();
}

void RankOperator::close() {
  // Reverse order (implementation contract): the derived cache and state
  // handles first, then the backend and the selector, then the base class.
  cache_.reset();
  counterState_.reset();
  valueState_.reset();
  listState_.reset();
  keySelector_.reset();
  keyedBackend_.reset();
  StatefulOperator::close();
}

std::vector<std::string> RankOperator::snapshotRankState(
    int64_t /*checkpointId*/) {
  VELOX_CHECK_NOT_NULL(
      keyedBackend_,
      "Rank keyed state backend is not initialized before snapshot");
  // One record: the whole typed backend snapshot (design S6).
  return {keyedBackend_->snapshot()};
}

void RankOperator::restoreRankState(
    const std::vector<std::string>& checkpointRecords) {
  VELOX_CHECK_NOT_NULL(
      keyedBackend_,
      "Rank keyed state backend is not initialized before restore");
  for (const auto& record : checkpointRecords) {
    keyedBackend_->restore(record);
  }
}

void RankOperator::checkInputRowKinds(const StreamRecord& record) const {
  const auto& rowKind = record.rowKind();
  if (!rowKind) {
    // appendOnly: every row is an INSERT.
    return;
  }
  for (vector_size_t i = 0; i < rowKind->size(); ++i) {
    const auto kind = rowKind->valueAt(i);
    if (kind != static_cast<int8_t>(RowKind::INSERT)) {
      VELOX_UNSUPPORTED(
          "Rank operator only supports insert-only input (got row kind {} at row {})",
          static_cast<int>(kind),
          i);
    }
  }
}

RowVectorPtr RankOperator::withSentinelKeyColumn(const RowVectorPtr& input) {
  const auto numRows = input->size();
  auto values = AlignedBuffer::allocate<int64_t>(numRows, memoryPool());
  auto* raw = values->asMutable<int64_t>();
  std::fill(raw, raw + numRows, 0);
  std::vector<VectorPtr> children = input->children();
  children.push_back(std::make_shared<FlatVector<int64_t>>(
      memoryPool(),
      BIGINT(),
      nullptr,
      numRows,
      std::move(values),
      std::vector<BufferPtr>{}));
  return std::make_shared<RowVector>(
      memoryPool(), augmentedInputType_, nullptr, numRows, std::move(children));
}

void RankOperator::decodeInputBatch(const RowVectorPtr& input) {
  // One DecodedVector per input column, decoded once for the whole batch
  // (decode transparently loads lazy children); encodeRow and the input
  // side of every comparison read these (design S3.2 step 2).
  const auto numCols = node_->inputType()->size();
  decodedInput_.resize(numCols);
  for (column_index_t c = 0; c < numCols; ++c) {
    decodedInput_[c].decode(*input->childAt(c));
  }
}

CompactRowPtr RankOperator::encodeRow(vector_size_t row) {
  // The inputRowSer.copy mirror (design alignment #59): one contiguous
  // compact blob, exactly one heap allocation, never borrowing from the
  // input batch.
  return makeCompactRow(compactLayout_->encodeRow(decodedInput_, row));
}

RankOperator::TopNBuffer& RankOperator::bufferFor(
    const RowContainerStateKey& key) {
  // LRU touch on hit; lazy rebuild on miss (design alignments #62-#64):
  // first touch of the key in this cache lifetime replays the stored list
  // through comparator insertion - sort-key order is restored by the
  // comparator, arrival order of ties by the list order (design D1: state
  // stores, buffer orders). Eviction never loses the facts: the list state
  // holds every row (write-through), so any evicted key replays the same.
  return cache_->touchOrReplay(key.row(), [this, &key] {
    TopNBuffer buffer;
    for (const auto& stored : listState_->get(key, ns_)) {
      const auto position = insertionPosition(stored, buffer);
      buffer.insert(buffer.begin() + position, stored);
    }
    return buffer;
  });
}

void RankOperator::processFirstNRows(
    const RowVectorPtr& input,
    folly::Range<const RowContainerStateKey*> keys,
    vector_size_t numRows) {
  // Mirrors AppendOnlyFirstNFunction.processElement with the row-granular
  // work restructured to the batch form: the counter counts the rows
  // already admitted for the key (an empty list is a fresh key, rank 0);
  // a full counter drops the row before any state write or row work,
  // otherwise the counter increments through the remove+add single-element
  // maintenance and the row is emitted as +I with its rank (counter value,
  // 1-based) when the number column or the offset window carries a rank -
  // plain +I otherwise (alignment #28-32). The offset gate of emit()
  // (isInRankRange: rank <= rankEnd && rank >= rankStart) applies here:
  // the state still counts a swallowed row, its output is dropped
  // (alignment #33/#34/#38).
  std::vector<vector_size_t> admitted;
  std::vector<int64_t> admittedRanks;
  for (vector_size_t i = 0; i < numRows; ++i) {
    const auto& key = keys[i];
    const auto stored = counterState_->get(key, ns_);
    const int64_t counter = stored.empty() ? 0 : stored.front();
    if (counter >= rankEnd_) {
      continue;
    }
    const int64_t currentRank = counter + 1;
    counterState_->remove(key, ns_);
    counterState_->add(key, ns_, currentRank);
    const int64_t emitRank =
        (outputRankNumber_ || hasOffset_) ? currentRank : 0;
    if (emitRank != 0 && (emitRank < rankStart_ || emitRank > rankEnd_)) {
      continue;
    }
    admitted.push_back(i);
    admittedRanks.push_back(emitRank);
  }
  if (admitted.empty()) {
    return;
  }

  // Zero-copy emission (AppendOnlyFirstNFunction emits the incoming row by
  // reference): each input column is dictionary-wrapped over the admitted
  // indices - the same wrapping StreamPartition applies to its partition
  // slices - so the output shares the input's buffers instead of copying
  // them. The stateful chain consumes the output synchronously within this
  // addInput call, and the wrapped children keep the input batch alive
  // through shared_ptr ownership afterwards.
  const auto numAdmitted = static_cast<vector_size_t>(admitted.size());
  auto indices =
      AlignedBuffer::allocate<vector_size_t>(numAdmitted, memoryPool());
  std::copy(
      admitted.begin(), admitted.end(), indices->asMutable<vector_size_t>());

  const auto numInputCols = node_->inputType()->size();
  // Lazy children must be loaded before wrapping (the StreamPartition
  // pattern): the load triggers on the child itself, the wrap targets the
  // original child so the output keeps sharing its buffers.
  for (column_index_t c = 0; c < numInputCols; ++c) {
    input->childAt(c)->loadedVector();
  }
  std::vector<VectorPtr> children;
  children.reserve(numInputCols + (outputRankNumber_ ? 1 : 0));
  for (column_index_t c = 0; c < numInputCols; ++c) {
    children.push_back(BaseVector::wrapInDictionary(
        nullptr, indices, numAdmitted, input->childAt(c)));
  }
  if (outputRankNumber_) {
    auto ranks = AlignedBuffer::allocate<int64_t>(numAdmitted, memoryPool());
    std::copy(
        admittedRanks.begin(),
        admittedRanks.end(),
        ranks->asMutable<int64_t>());
    children.push_back(std::make_shared<FlatVector<int64_t>>(
        memoryPool(),
        BIGINT(),
        nullptr,
        numAdmitted,
        std::move(ranks),
        std::vector<BufferPtr>{}));
  }
  auto output = std::make_shared<RowVector>(
      memoryPool(),
      node_->outputType(),
      nullptr,
      numAdmitted,
      std::move(children));

  // All-INSERT changelog kinds (FirstN is insert-only), same shape the
  // per-row emission path builds through buildEmitRowKinds.
  auto kinds = AlignedBuffer::allocate<int8_t>(numAdmitted, memoryPool());
  std::fill(
      kinds->asMutable<int8_t>(),
      kinds->asMutable<int8_t>() + numAdmitted,
      static_cast<int8_t>(RowKind::INSERT));
  auto rowKinds = std::make_shared<FlatVector<int8_t>>(
      memoryPool(),
      TINYINT(),
      nullptr,
      numAdmitted,
      std::move(kinds),
      std::vector<BufferPtr>{});

  pushOutput(std::make_shared<StreamRecord>(
      getPlanNodeId(), std::move(output), std::move(rowKinds)));
}

void RankOperator::processTop1Row(
    const RowContainerStateKey& key,
    vector_size_t inputRow) {
  // Mirrors FastTop1Function.processElement: first row +I (rank 1 with the
  // number column, plain +I without), strictly better row -U/+U - the -U
  // gated by generateUpdateBefore, the +U/U pair kept even without the
  // number column (design alignment #12-14, #25) - worse or equal row: no
  // output. No cache layer (design alignment #66): the typed ValueState
  // read is a direct shared_ptr lookup, and the compact row encodes only
  // once the comparison said the row wins.
  auto current = valueState_->value(key, ns_);
  if (current == nullptr) {
    auto row = encodeRow(inputRow);
    valueState_->update(key, ns_, row);
    emit(std::move(row), RowKind::INSERT, 1);
    return;
  }
  if (compareSortKeys(inputRow, current) < 0) {
    auto row = encodeRow(inputRow);
    valueState_->update(key, ns_, row);
    if (generateUpdateBefore_) {
      emit(std::move(current), RowKind::UPDATE_BEFORE, 1);
    }
    emit(std::move(row), RowKind::UPDATE_AFTER, 1);
  }
}

void RankOperator::processTopNWithRowNumber(
    const RowContainerStateKey& key,
    vector_size_t inputRow) {
  auto& buffer = bufferFor(key);
  // checkSortKeyInBufferRange (TopNBuffer): empty buffer, strictly better
  // than the worst sort key, or buffer not full yet.
  if (!checkRange(inputRow, buffer)) {
    return;
  }
  // The compact row encodes only after the range check admitted the row -
  // Flink copies through inputRowSer.copy at the same position (design
  // alignment #59/#60). Entry is written to the state immediately and
  // shares the same compact row with the buffer entry (write-through,
  // design alignment #63).
  auto row = encodeRow(inputRow);
  listState_->add(key, ns_, row);
  const auto position = insertionPosition(inputRow, buffer);
  buffer.insert(buffer.begin() + position, row);

  // Displacement chain of AppendOnlyTopNFunction#processElementWithRowNumber:
  // each row after the new one is pushed one rank down, the pair
  // (-U old holder, +U new holder) emitted per rank while inside the range.
  // Every emitted row is the same shared compact row (state, buffer and
  // emissions hold one pointer, no copy).
  const auto size = buffer.size();
  int64_t currentRank = static_cast<int64_t>(position) + 1;
  CompactRowPtr currentRow = std::move(row);
  size_t j = position + 1;
  for (; j < size && currentRank <= rankEnd_; ++j) {
    auto prevRow = buffer[j];
    if (generateUpdateBefore_) {
      emit(prevRow, RowKind::UPDATE_BEFORE, currentRank);
    }
    emit(currentRow, RowKind::UPDATE_AFTER, currentRank);
    currentRow = prevRow;
    ++currentRank;
  }
  // Not enough rows yet: the last displaced row takes the open rank as +I.
  if (currentRank <= rankEnd_) {
    emit(std::move(currentRow), RowKind::INSERT, currentRank);
  }
  // Eager purge of out-of-range rows (design alignment #10/#22): keep the
  // first rankEnd_ rows only, state rows per key stay <= N. Flink keeps one
  // ghost row past the end of the walk, which is never emitted, so external
  // semantics are identical.
  if (buffer.size() > static_cast<size_t>(rankEnd_)) {
    buffer.resize(static_cast<size_t>(rankEnd_));
    rewriteListState(key, buffer);
  }
}

void RankOperator::processTopNWithoutRowNumber(
    const RowContainerStateKey& key,
    vector_size_t inputRow) {
  auto& buffer = bufferFor(key);
  if (!checkRange(inputRow, buffer)) {
    return;
  }
  auto row = encodeRow(inputRow);
  listState_->add(key, ns_, row);
  const auto position = insertionPosition(inputRow, buffer);
  buffer.insert(buffer.begin() + position, row);

  // AppendOnlyTopNFunction#processElementWithoutRowNumber: over capacity,
  // evict the last row (-D) and emit the new row (+I); when the evicted row
  // is the input row itself, nothing is emitted.
  if (buffer.size() > static_cast<size_t>(rankEnd_)) {
    auto last = buffer.back();
    buffer.pop_back();
    rewriteListState(key, buffer);
    if (sameStoredRow(row, last)) {
      return;
    }
    emit(std::move(last), RowKind::DELETE, 0);
  }
  emit(std::move(row), RowKind::INSERT, 0);
}

bool RankOperator::checkRange(vector_size_t inputRow, const TopNBuffer& buffer)
    const {
  if (buffer.empty()) {
    return true;
  }
  const int cmp = compareSortKeys(inputRow, buffer.back());
  if (cmp < 0) {
    return true;
  }
  return buffer.size() < static_cast<size_t>(rankEnd_);
}

size_t RankOperator::insertionPosition(
    vector_size_t inputRow,
    const TopNBuffer& buffer) const {
  size_t position = 0;
  while (position < buffer.size() &&
         compareSortKeys(inputRow, buffer[position]) >= 0) {
    ++position;
  }
  return position;
}

size_t RankOperator::insertionPosition(
    const CompactRowPtr& row,
    const TopNBuffer& buffer) const {
  // The lazy rebuild's form: both sides are stored compact rows.
  size_t position = 0;
  while (position < buffer.size() &&
         compareSortKeys(row, buffer[position]) >= 0) {
    ++position;
  }
  return position;
}

int RankOperator::compareSortKeys(
    vector_size_t inputRow,
    const CompactRowPtr& stored) const {
  // Input batch row against a stored row: the channel walk, null ordering
  // and typed compare are the stored-vector semantics (design alignment
  // #60/#72) - only the reads changed (batch-decoded input, slot loads on
  // the stored side).
  return compareSortKeysInputVsStored(
      *compactLayout_,
      decodedInput_,
      inputRow,
      node_->sortKeyChannels(),
      node_->sortAscending(),
      node_->sortNullsFirst(),
      *stored);
}

int RankOperator::compareSortKeys(
    const CompactRowPtr& lhs,
    const CompactRowPtr& rhs) const {
  return compareSortKeysStoredVsStored(
      *compactLayout_,
      node_->sortKeyChannels(),
      node_->sortAscending(),
      node_->sortNullsFirst(),
      *lhs,
      *rhs);
}

void RankOperator::rewriteListState(
    const RowContainerStateKey& key,
    const TopNBuffer& buffer) {
  listState_->remove(key, ns_);
  for (const auto& stored : buffer) {
    listState_->add(key, ns_, stored);
  }
}

void RankOperator::emit(CompactRowPtr row, RowKind kind, int64_t rank) {
  // Unified isInRankRange gate (Flink AbstractTopNFunction#isInRankRange:
  // rank <= rankEnd && rank >= rankStart): every rank-bearing emission must
  // land inside the window - offset-window displacement chains, their tail
  // inserts and the FirstN+offset inserts drop the rows below rankStart
  // here (alignment #33/#34/#38). rank 0 marks the no-rank-semantics paths
  // (Top1/WithoutRowNumber/FirstN without the number) which the native side
  // emits ungated, so they pass through.
  if (rank != 0 && (rank < rankStart_ || rank > rankEnd_)) {
    return;
  }
  emissions_.push_back(EmitRow{std::move(row), kind, rank});
}

template <typename T>
void RankOperator::fillEmitColumn(
    const std::vector<EmitRow>& emissions,
    column_index_t c,
    FlatVector<T>& column) const {
  // Columnar emission decode (design S3.2 step 6): fixed-width kinds read
  // one slot per row; VARCHAR/VARBINARY rows copy their strings into the
  // vector's own string buffers (FlatVector<StringView>::set), so nothing
  // points back into the compact rows after this batch leaves the operator.
  for (size_t k = 0; k < emissions.size(); ++k) {
    const auto idx = static_cast<vector_size_t>(k);
    if (compactLayout_->isNullAt(*emissions[k].row, c)) {
      column.setNull(idx, true);
    } else {
      column.set(idx, compactLayout_->valueAt<T>(*emissions[k].row, c));
    }
  }
}

VectorPtr RankOperator::decodeEmitColumn(
    const std::vector<EmitRow>& emissions,
    column_index_t c) {
  const auto& type = node_->inputType()->childAt(c);
  const auto numRows = static_cast<vector_size_t>(emissions.size());
  auto column = BaseVector::create(type, numRows, memoryPool());
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      fillEmitColumn<bool>(emissions, c, *column->as<FlatVector<bool>>());
      break;
    case TypeKind::TINYINT:
      fillEmitColumn<int8_t>(emissions, c, *column->as<FlatVector<int8_t>>());
      break;
    case TypeKind::SMALLINT:
      fillEmitColumn<int16_t>(emissions, c, *column->as<FlatVector<int16_t>>());
      break;
    case TypeKind::INTEGER:
      fillEmitColumn<int32_t>(emissions, c, *column->as<FlatVector<int32_t>>());
      break;
    case TypeKind::BIGINT:
      fillEmitColumn<int64_t>(emissions, c, *column->as<FlatVector<int64_t>>());
      break;
    case TypeKind::HUGEINT:
      fillEmitColumn<int128_t>(
          emissions, c, *column->as<FlatVector<int128_t>>());
      break;
    case TypeKind::REAL:
      fillEmitColumn<float>(emissions, c, *column->as<FlatVector<float>>());
      break;
    case TypeKind::DOUBLE:
      fillEmitColumn<double>(emissions, c, *column->as<FlatVector<double>>());
      break;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      fillEmitColumn<StringView>(
          emissions, c, *column->as<FlatVector<StringView>>());
      break;
    case TypeKind::TIMESTAMP:
      fillEmitColumn<Timestamp>(
          emissions, c, *column->as<FlatVector<Timestamp>>());
      break;
    default:
      // Fail loud: the compact layout built from the same input row type
      // already rejected this column, this is unreachable defense.
      VELOX_UNSUPPORTED(
          "Compact row emission does not support type {}", type->toString());
  }
  return column;
}

RowVectorPtr RankOperator::buildEmitRowVector(
    const std::vector<EmitRow>& emissions) {
  const auto numRows = static_cast<vector_size_t>(emissions.size());
  const auto numInputCols = node_->inputType()->size();
  std::vector<VectorPtr> children;
  children.reserve(numInputCols + (outputRankNumber_ ? 1 : 0));
  for (column_index_t c = 0; c < numInputCols; ++c) {
    children.push_back(decodeEmitColumn(emissions, c));
  }
  if (outputRankNumber_) {
    auto ranks = AlignedBuffer::allocate<int64_t>(numRows, memoryPool());
    auto* raw = ranks->asMutable<int64_t>();
    for (vector_size_t k = 0; k < numRows; ++k) {
      raw[k] = emissions[k].rank;
    }
    children.push_back(std::make_shared<FlatVector<int64_t>>(
        memoryPool(),
        BIGINT(),
        nullptr,
        numRows,
        std::move(ranks),
        std::vector<BufferPtr>{}));
  }
  return std::make_shared<RowVector>(
      memoryPool(), node_->outputType(), nullptr, numRows, std::move(children));
}

std::shared_ptr<FlatVector<int8_t>> RankOperator::buildEmitRowKinds(
    const std::vector<EmitRow>& emissions) {
  const auto numRows = static_cast<vector_size_t>(emissions.size());
  auto kinds = AlignedBuffer::allocate<int8_t>(numRows, memoryPool());
  auto* raw = kinds->asMutable<int8_t>();
  for (vector_size_t k = 0; k < numRows; ++k) {
    raw[k] = static_cast<int8_t>(emissions[k].kind);
  }
  return std::make_shared<FlatVector<int8_t>>(
      memoryPool(),
      TINYINT(),
      nullptr,
      numRows,
      std::move(kinds),
      std::vector<BufferPtr>{});
}

} // namespace facebook::velox::stateful
