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

#include <algorithm>
#include <map>

#include "velox/experimental/stateful/WindowJoin.h"

#include "velox/common/base/BitUtil.h"
#include "velox/experimental/stateful/KeyedStateSpec.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/KeyedStateBackendFactory.h"

namespace facebook::velox::stateful {

namespace {

// Window ends arrive as BIGINT millis or TIMESTAMP; normalize to millis.
std::vector<int64_t> windowEnds(const RowVectorPtr& input, int windowEndIndex) {
  const auto* child = input->childAt(windowEndIndex).get();
  const auto numRows = input->size();
  std::vector<int64_t> ends(numRows);
  if (const auto* bigint = child->asFlatVector<int64_t>()) {
    std::copy(bigint->rawValues(), bigint->rawValues() + numRows, ends.data());
  } else if (const auto* ts = child->asFlatVector<Timestamp>()) {
    for (vector_size_t i = 0; i < numRows; ++i) {
      ends[i] = ts->rawValues()[i].toMillis();
    }
  } else {
    VELOX_FAIL(
        "Window join needs a BIGINT or TIMESTAMP window-end column, got {}",
        child->type()->toString());
  }
  return ends;
}

// One dictionary-wrapped view of the rows at 'rows' over 'input'.
RowVectorPtr dictionaryView(
    const RowVectorPtr& input,
    const std::vector<vector_size_t>& rows,
    memory::MemoryPool* pool) {
  const auto m = static_cast<vector_size_t>(rows.size());
  auto indices = allocateIndices(m, pool);
  std::copy(rows.begin(), rows.end(), indices->asMutable<vector_size_t>());
  auto view = std::make_shared<RowVector>(
      pool,
      input->type(),
      BufferPtr(nullptr),
      m,
      std::vector<VectorPtr>(input->childrenSize()));
  for (vector_size_t c = 0; c < input->childrenSize(); ++c) {
    view->childAt(c) =
        BaseVector::wrapInDictionary(nullptr, indices, m, input->childAt(c));
  }
  view->updateContainsLazyNotLoaded();
  return view;
}

RowVectorPtr mergeSide(
    const std::vector<RowVectorPtr>& rows,
    memory::MemoryPool* pool) {
  if (rows.empty()) {
    return nullptr;
  }
  if (rows.size() == 1) {
    return rows[0];
  }
  return TimeWindowUtil::mergeVectors(
      std::list<RowVectorPtr>(rows.begin(), rows.end()), pool);
}

} // namespace

WindowJoin::WindowJoin(
    std::unique_ptr<exec::Operator> leftInput,
    std::unique_ptr<exec::Operator> rightInput,
    std::unique_ptr<exec::Operator> probe,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::shared_ptr<const StreamWindowJoinNode> node)
    : StatefulOperator(std::move(probe), std::move(targets)),
      leftInput_(std::move(leftInput)),
      rightInput_(std::move(rightInput)),
      node_(std::move(node)),
      probe_(static_cast<exec::NestedLoopJoinProbe*>(op().get())) {}

void WindowJoin::initialize() {
  StatefulOperator::initialize();
  leftInput_->initialize();
  rightInput_->initialize();
}

bool WindowJoin::isFinished() {
  return leftInput_->isFinished() && rightInput_->isFinished();
}

void WindowJoin::initializeStateBackend(StateBackend* stateBackend) {
  StatefulOperator::initializeStateBackend(stateBackend);
  const auto& leftChannels = node_->leftKeyChannels();
  const auto& rightChannels = node_->rightKeyChannels();
  const auto leftInputType = node_->sources()[0]->outputType();
  const auto rightInputType = node_->sources()[1]->outputType();

  std::vector<TypePtr> keyTypes;
  keyTypes.reserve(leftChannels.size());
  for (auto channel : leftChannels) {
    keyTypes.push_back(leftInputType->childAt(channel));
  }
  const KeyedStateSpec spec(leftChannels, std::move(keyTypes));
  keyedBackend_ = createKeyedStateBackend<RowContainerStateKey>(
      *stateBackend, spec, op()->pool());
  // The factory's HEAP branch constructs exactly this engine type.
  backend_ = static_cast<HeapKeyedStateBackend<RowContainerStateKey>*>(
      keyedBackend_.get());

  leftKeySelector_ =
      std::make_unique<KeySelector>(backend_->keyIndex(), leftChannels);
  rightKeySelector_ =
      std::make_unique<KeySelector>(backend_->keyIndex(), rightChannels);

  leftState_ = backend_->getOrCreateListState<VoidNamespace, RowVectorPtr>(
      ListStateDescriptor<RowVectorPtr>(
          "left-records",
          std::make_shared<ComplexVectorSerializer<RowVectorPtr>>(
              leftInputType, op()->pool()),
          op()->pool()));
  rightState_ = backend_->getOrCreateListState<VoidNamespace, RowVectorPtr>(
      ListStateDescriptor<RowVectorPtr>(
          "right-records",
          std::make_shared<ComplexVectorSerializer<RowVectorPtr>>(
              rightInputType, op()->pool()),
          op()->pool()));
  timerService_ =
      std::make_unique<InternalTimerService<int64_t, int64_t>>(this);
  // The gluten planner gates the offload on joins with no residual
  // condition (isOffloadable); the C++ deserialization materializes a
  // default expression for a missing joinCondition field.
  fastPath_ = true;
}

void WindowJoin::addInput(StreamElementPtr input) {
  const auto record = std::dynamic_pointer_cast<StreamRecord>(input);
  VELOX_CHECK(record, "WindowJoin expects StreamRecord input");
  if (record->nodeId() == leftInput_->planNodeId()) {
    fileRecord(
        *record,
        leftKeySelector_.get(),
        node_->leftWindowEndIndex(),
        leftState_.get(),
        &leftLateRecordsDropped_);
  } else {
    fileRecord(
        *record,
        rightKeySelector_.get(),
        node_->rightWindowEndIndex(),
        rightState_.get(),
        &rightLateRecordsDropped_);
  }
}

void WindowJoin::advance() {
  auto leftResult = leftInput_->getOutput();
  if (leftResult) {
    const auto record =
        StreamRecord::create(leftInput_->planNodeId(), std::move(leftResult));
    fileRecord(
        *record,
        leftKeySelector_.get(),
        node_->leftWindowEndIndex(),
        leftState_.get(),
        &leftLateRecordsDropped_);
  }
  auto rightResult = rightInput_->getOutput();
  if (rightResult) {
    const auto record =
        StreamRecord::create(rightInput_->planNodeId(), std::move(rightResult));
    fileRecord(
        *record,
        rightKeySelector_.get(),
        node_->rightWindowEndIndex(),
        rightState_.get(),
        &rightLateRecordsDropped_);
  }
  if (backend_ != nullptr) {
    backend_->reclaim();
  }
}

void WindowJoin::fileRecord(
    const StreamRecord& record,
    KeySelector* keySelector,
    int windowEndIndex,
    JoinListState* state,
    uint64_t* lateRecordsDropped) {
  const auto& rows = record.record();
  for (const auto& child : rows->children()) {
    child->loadedVector();
  }
  const auto numRows = rows->size();
  keySelector->probe(rows);
  const auto keys = keySelector->keys();
  const auto ends = windowEnds(rows, windowEndIndex);

  // The composite key (join key..., window end) already carries the window,
  // so rows of one key row form one state entry.
  std::map<const void*, std::vector<vector_size_t>> byKey;
  for (vector_size_t i = 0; i < numRows; ++i) {
    byKey[keys[i].row()].push_back(i);
  }
  for (const auto& entry : byKey) {
    const auto& rowIdxs = entry.second;
    const auto& key = keys[rowIdxs.front()];
    const int64_t we = ends[rowIdxs.front()];
    // The timer service's currentWatermark() is the earliest pending timer,
    // not the progressed watermark; the lateness criterion follows the
    // operator's combined watermark.
    if (TimeWindowUtil::isWindowFired(
            we, combinedWatermark_, node_->shiftTimeZone())) {
      *lateRecordsDropped += rowIdxs.size();
      continue;
    }
    state->add(
        key, VoidNamespace{}, dictionaryView(rows, rowIdxs, op()->pool()));
    // Extract actual key values for debug
    std::string keyVals;
    for (auto ch : node_->leftKeyChannels()) {
      if (ch < rows->childrenSize()) {
        auto child = rows->childAt(ch).get();
        if (const auto* big = child->asFlatVector<int64_t>()) {
          keyVals += std::to_string(big->valueAt(rowIdxs.front())) + ",";
        } else if (const auto* ts = child->asFlatVector<Timestamp>()) {
          keyVals +=
              std::to_string(ts->valueAt(rowIdxs.front()).toMillis()) + ",";
        } else {
          keyVals += "type:" + child->type()->toString() + ",";
        }
      } else {
        keyVals += "OOR(" + std::to_string(ch) + "/" +
            std::to_string(rows->childrenSize()) + "),";
      }
    }
    const auto timerKey = reinterpret_cast<intptr_t>(key.row());
    timerKeys_.emplace(timerKey, key);
    timerService_->registerEventTimeTimer(
        timerKey,
        we,
        TimeWindowUtil::toEpochMillsForTimer(we - 1, node_->shiftTimeZone()));
  }
}

void WindowJoin::processWatermark(int64_t timestamp, int32_t index) {
  auto& side = index == 0 ? leftMinWatermark_ : rightMinWatermark_;
  side = std::max(side, timestamp);
  const int64_t combined = std::min(leftMinWatermark_, rightMinWatermark_);
  if (combined > combinedWatermark_) {
    combinedWatermark_ = combined;
    // Fires onEventTime synchronously: fired records are pushed before the
    // base propagates the watermark below.
    timerService_->advanceWatermark(combined);
    flushCrossOutputs();
  }
  StatefulOperator::processWatermark(timestamp, index);
}

void WindowJoin::onEventTime(
    std::shared_ptr<TimerHeapInternalTimer<int64_t, int64_t>> timer) {
  const auto it = timerKeys_.find(timer->key());
  if (it == timerKeys_.end()) {
    return; // Stale duplicate registration; the state is already gone.
  }
  const auto key = it->second;
  timerKeys_.erase(it);
  fireWindow(key);
}

void WindowJoin::fireWindow(const RowContainerStateKey& key) {
  auto leftRows = leftState_->get(key, VoidNamespace{});
  auto rightRows = rightState_->get(key, VoidNamespace{});
  const auto joinType = static_cast<core::JoinType>(node_->joinType());

  // A null key never satisfies an equality (SQL NULL = NULL -> UNKNOWN),
  // so a null-key bucket has no matched pairs regardless of right side.
  const bool nullKey = !leftRows.empty() && bucketHasNullKey(leftRows[0]);
  const bool hasMatch = !nullKey && !leftRows.empty() && !rightRows.empty();

  switch (joinType) {
    case core::JoinType::kInner:
      if (hasMatch) {
        emitCrossProduct(leftRows, rightRows);
      }
      break;
    case core::JoinType::kLeft:
      emitLeftOuter(leftRows, rightRows);
      break;
    case core::JoinType::kRight:
      emitRightOuter(leftRows, rightRows);
      break;
    case core::JoinType::kFull:
      emitFullOuter(leftRows, rightRows);
      break;
    case core::JoinType::kLeftSemiFilter:
      emitSemi(leftRows, rightRows);
      break;
    case core::JoinType::kAnti:
      emitAnti(leftRows, rightRows);
      break;
    default:
      VELOX_UNSUPPORTED(
          "Window join does not support join type {}", joinTypeName(joinType));
  }

  leftState_->remove(key, VoidNamespace{});
  rightState_->remove(key, VoidNamespace{});
}

// ---------------------------------------------------------------------------
// Six-semantics emit helpers.
// ---------------------------------------------------------------------------

bool WindowJoin::bucketHasNullKey(const RowVectorPtr& rows) const {
  for (auto channel : node_->leftKeyChannels()) {
    if (channel < rows->childrenSize() && rows->childAt(channel)->isNullAt(0)) {
      return true;
    }
  }
  return false;
}

void WindowJoin::emitNullPadded(
    const RowVectorPtr& rows,
    int rowWidth,
    bool padLeft,
    int leftWidth,
    int rightWidth) {
  const auto n = rows->size();
  if (n == 0) {
    return;
  }
  const auto outWidth = leftWidth + rightWidth;
  std::vector<VectorPtr> children(outWidth);
  if (padLeft) {
    // (null..., right...): null left, right from rows.
    for (int c = 0; c < leftWidth; c++) {
      children[c] = BaseVector::createNullConstant(
          node_->outputType()->childAt(c), n, op()->pool());
    }
    for (int c = 0; c < rightWidth; c++) {
      children[leftWidth + c] = rows->childAt(c);
    }
  } else {
    // (left..., null...): left from rows, null right.
    for (int c = 0; c < leftWidth; c++) {
      children[c] = rows->childAt(c);
    }
    for (int c = 0; c < rightWidth; c++) {
      children[leftWidth + c] = BaseVector::createNullConstant(
          node_->outputType()->childAt(leftWidth + c), n, op()->pool());
    }
  }
  auto out = std::make_shared<RowVector>(
      op()->pool(),
      node_->outputType(),
      BufferPtr(nullptr),
      n,
      std::move(children));
  out->updateContainsLazyNotLoaded();
  crossOutputs_.push_back(std::move(out));
  crossOutputRows_ += n;
  if (crossOutputs_.size() >= 256 || crossOutputRows_ >= 16384) {
    flushCrossOutputs();
  }
}

void WindowJoin::emitLeftProjection(const RowVectorPtr& leftRows) {
  const auto n = leftRows->size();
  if (n == 0) {
    return;
  }
  // The plan's outputType for SEMI/ANTI carries the left columns only.
  const auto outWidth = node_->outputType()->size();
  std::vector<VectorPtr> children(outWidth);
  for (int c = 0; c < outWidth; c++) {
    children[c] = leftRows->childAt(c);
  }
  auto out = std::make_shared<RowVector>(
      op()->pool(),
      node_->outputType(),
      BufferPtr(nullptr),
      n,
      std::move(children));
  out->updateContainsLazyNotLoaded();
  crossOutputs_.push_back(std::move(out));
  crossOutputRows_ += n;
  if (crossOutputs_.size() >= 256 || crossOutputRows_ >= 16384) {
    flushCrossOutputs();
  }
}

void WindowJoin::emitLeftOuter(
    const std::vector<RowVectorPtr>& leftRows,
    const std::vector<RowVectorPtr>& rightRows) {
  const auto lw = node_->probe()->sources()[0]->outputType()->size();
  const auto outWidth = node_->outputType()->size();
  const auto rw = outWidth - lw;
  const bool nullKey = !leftRows.empty() && bucketHasNullKey(leftRows[0]);
  const bool hasMatch = !nullKey && !leftRows.empty() && !rightRows.empty();

  if (hasMatch) {
    emitCrossProduct(leftRows, rightRows);
  } else if (!leftRows.empty()) {
    emitNullPadded(
        mergeSide(leftRows, op()->pool()), lw, /*padLeft=*/false, lw, rw);
  }
}

void WindowJoin::emitRightOuter(
    const std::vector<RowVectorPtr>& leftRows,
    const std::vector<RowVectorPtr>& rightRows) {
  const auto lw = node_->probe()->sources()[0]->outputType()->size();
  const auto outWidth = node_->outputType()->size();
  const auto rw = outWidth - lw;
  const bool nullKey = !rightRows.empty() && bucketHasNullKey(rightRows[0]);
  const bool hasMatch = !nullKey && !leftRows.empty() && !rightRows.empty();

  if (hasMatch) {
    emitCrossProduct(leftRows, rightRows);
  } else if (!rightRows.empty()) {
    emitNullPadded(
        mergeSide(rightRows, op()->pool()), rw, /*padLeft=*/true, lw, rw);
  }
}

void WindowJoin::emitFullOuter(
    const std::vector<RowVectorPtr>& leftRows,
    const std::vector<RowVectorPtr>& rightRows) {
  const auto lw = node_->probe()->sources()[0]->outputType()->size();
  const auto outWidth = node_->outputType()->size();
  const auto rw = outWidth - lw;
  const bool nullKey = (!leftRows.empty() && bucketHasNullKey(leftRows[0])) ||
      (!rightRows.empty() && bucketHasNullKey(rightRows[0]));
  const bool hasMatch = !nullKey && !leftRows.empty() && !rightRows.empty();

  if (hasMatch) {
    emitCrossProduct(leftRows, rightRows);
    return;
  }
  if (!leftRows.empty()) {
    emitNullPadded(
        mergeSide(leftRows, op()->pool()), lw, /*padLeft=*/false, lw, rw);
  }
  if (!rightRows.empty()) {
    emitNullPadded(
        mergeSide(rightRows, op()->pool()), rw, /*padLeft=*/true, lw, rw);
  }
}

void WindowJoin::emitSemi(
    const std::vector<RowVectorPtr>& leftRows,
    const std::vector<RowVectorPtr>& rightRows) {
  const bool nullKey = !leftRows.empty() && bucketHasNullKey(leftRows[0]);
  const bool hasMatch = !nullKey && !leftRows.empty() && !rightRows.empty();
  if (hasMatch && !leftRows.empty()) {
    auto L = mergeSide(leftRows, op()->pool());
    if (L != nullptr) {
      emitLeftProjection(L);
    }
  }
}

void WindowJoin::emitAnti(
    const std::vector<RowVectorPtr>& leftRows,
    const std::vector<RowVectorPtr>& rightRows) {
  const bool nullKey = !leftRows.empty() && bucketHasNullKey(leftRows[0]);
  const bool hasMatch = !nullKey && !leftRows.empty() && !rightRows.empty();
  if (!hasMatch && !leftRows.empty()) {
    auto L = mergeSide(leftRows, op()->pool());
    if (L != nullptr) {
      emitLeftProjection(L);
    }
  }
}

void WindowJoin::emitCrossProduct(
    const std::vector<RowVectorPtr>& leftRows,
    const std::vector<RowVectorPtr>& rightRows) {
  const auto L = mergeSide(leftRows, op()->pool());
  const auto R = mergeSide(rightRows, op()->pool());
  // INNER semantics: a NULL in any key column makes the equality
  // unsatisfiable; the fast path skips the probe condition, so drop
  // null-key rows here instead.
  bool leftNulls = false;
  for (auto channel : node_->leftKeyChannels()) {
    if (L->childAt(channel)->mayHaveNulls()) {
      leftNulls = true;
      break;
    }
  }
  bool rightNulls = false;
  if (!leftNulls) {
    for (auto channel : node_->rightKeyChannels()) {
      if (R->childAt(channel)->mayHaveNulls()) {
        rightNulls = true;
        break;
      }
    }
  }
  if (leftNulls || rightNulls) {
    return;
  }
  const auto nl = L->size();
  const auto nr = R->size();
  if (nl == 0 || nr == 0) {
    return;
  }
  const auto total = nl * nr;
  const auto lw = node_->probe()->sources()[0]->outputType()->size();
  const auto rw = node_->probe()->sources()[1]->outputType()->size();
  auto leftIdx = allocateIndices(total, op()->pool());
  auto rightIdx = allocateIndices(total, op()->pool());
  auto* li = leftIdx->asMutable<vector_size_t>();
  auto* ri = rightIdx->asMutable<vector_size_t>();
  for (vector_size_t i = 0, k = 0; i < nl; ++i) {
    for (vector_size_t j = 0; j < nr; ++j, ++k) {
      li[k] = i;
      ri[k] = j;
    }
  }
  std::vector<VectorPtr> children(lw + rw);
  for (vector_size_t c = 0; c < lw; ++c) {
    children[c] =
        BaseVector::wrapInDictionary(nullptr, leftIdx, total, L->childAt(c));
  }
  for (vector_size_t c = 0; c < rw; ++c) {
    children[lw + c] =
        BaseVector::wrapInDictionary(nullptr, rightIdx, total, R->childAt(c));
  }
  auto out = std::make_shared<RowVector>(
      op()->pool(),
      node_->outputType(),
      BufferPtr(nullptr),
      total,
      std::move(children));
  out->updateContainsLazyNotLoaded();
  crossOutputs_.push_back(std::move(out));
  crossOutputRows_ += total;
  if (crossOutputs_.size() >= 256 || crossOutputRows_ >= 16384) {
    flushCrossOutputs();
  }
}

void WindowJoin::emitProbe(
    const std::vector<RowVectorPtr>& leftRows,
    const std::vector<RowVectorPtr>& rightRows) {
  probe_->setBuildData(rightRows);
  for (const auto& leftVec : leftRows) {
    probe_->addInput(leftVec);
    while (auto output = probe_->getOutput()) {
      pushOutput(
          std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
    }
  }
}

void WindowJoin::flushCrossOutputs() {
  for (auto& output : crossOutputs_) {
    pushOutput(
        std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
  }
  crossOutputs_.clear();
  crossOutputRows_ = 0;
}

void WindowJoin::close() {
  if (timerService_ != nullptr) {
    timerService_->close();
  }
  StatefulOperator::close();
  leftInput_->close();
  rightInput_->close();
}

} // namespace facebook::velox::stateful
