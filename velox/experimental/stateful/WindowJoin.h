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

#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/experimental/stateful/InternalTimerService.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/KeyedStateBackend.h"
#include "velox/experimental/stateful/state/KeyedStateBackendFactory.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

// The velox counterpart of Flink\'s WindowJoinOperator: rows pair only
// inside their window; a window fires once when the combined watermark
// passes window end - 1, emits its matches and clears both sides\' state.
// The state key is the composite (equi-join key..., window end): the
// window end rides the key row, so the upstream exchange, the state
// bucketing and the timer key all agree on one key. The state lives in
// the operator-owned keyed backend (the framework checkpoint walk
// snapshots and restores it); timers run on InternalTimerService.
class WindowJoin : public StatefulOperator,
                   public Triggerable<int64_t, int64_t> {
 public:
  WindowJoin(
      std::unique_ptr<exec::Operator> leftInput,
      std::unique_ptr<exec::Operator> rightInput,
      std::unique_ptr<exec::Operator> probe,
      std::vector<std::unique_ptr<StatefulOperator>> targets,
      std::shared_ptr<const StreamWindowJoinNode> node);

  void initialize() override;

  bool isFinished() override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  // Creates the operator-owned keyed backend, binds both key selectors to
  // its key table and takes the two list states and the timer service.
  void initializeStateBackend(StateBackend* stateBackend) override;

  std::string name() const override {
    return "WindowJoin";
  }

  void onEventTime(
      std::shared_ptr<TimerHeapInternalTimer<int64_t, int64_t>> timer) override;

  uint64_t leftLateRecordsDropped() const {
    return leftLateRecordsDropped_;
  }

  uint64_t rightLateRecordsDropped() const {
    return rightLateRecordsDropped_;
  }

  // Fire due windows before the base propagates the combined watermark:
  // fired records must reach downstream ahead of the watermark.
  void processWatermark(int64_t timestamp, int32_t index) override;

 protected:
  int numInputs() const override {
    return 2;
  }

 private:
  using JoinListState =
      ListState<RowContainerStateKey, VoidNamespace, RowVectorPtr>;

  void fileRecord(
      const StreamRecord& record,
      KeySelector* keySelector,
      int windowEndIndex,
      JoinListState* state,
      uint64_t* lateRecordsDropped);

  void fireWindow(const RowContainerStateKey& key);

  // Fast path: INNER join whose probe carries no residual condition emits
  // the window\'s cross product as dictionary-wrapped vectors, zero copy.
  void emitCrossProduct(
      const std::vector<RowVectorPtr>& leftRows,
      const std::vector<RowVectorPtr>& rightRows);

  // Outer joins: matched pairs (cross product) plus unmatched rows with
  // null padding on the other side.
  void emitLeftOuter(
      const std::vector<RowVectorPtr>& leftRows,
      const std::vector<RowVectorPtr>& rightRows);
  void emitRightOuter(
      const std::vector<RowVectorPtr>& leftRows,
      const std::vector<RowVectorPtr>& rightRows);
  void emitFullOuter(
      const std::vector<RowVectorPtr>& leftRows,
      const std::vector<RowVectorPtr>& rightRows);

  // Semi/anti: project the left rows only.
  void emitSemi(
      const std::vector<RowVectorPtr>& leftRows,
      const std::vector<RowVectorPtr>& rightRows);
  void emitAnti(
      const std::vector<RowVectorPtr>& leftRows,
      const std::vector<RowVectorPtr>& rightRows);

  // Shared helpers for the six-semantics dispatch.
  void emitNullPadded(
      const RowVectorPtr& rows,
      int rowWidth,
      bool padLeft,
      int leftWidth,
      int rightWidth);
  void emitLeftProjection(const RowVectorPtr& leftRows);
  bool bucketHasNullKey(const RowVectorPtr& rows) const;

  void emitProbe(
      const std::vector<RowVectorPtr>& leftRows,
      const std::vector<RowVectorPtr>& rightRows);

  void flushCrossOutputs();

  const std::unique_ptr<exec::Operator> leftInput_;
  const std::unique_ptr<exec::Operator> rightInput_;
  const std::shared_ptr<const StreamWindowJoinNode> node_;
  std::unique_ptr<KeySelector> leftKeySelector_;
  std::unique_ptr<KeySelector> rightKeySelector_;
  exec::NestedLoopJoinProbe* probe_;
  // The engine face: the list-state template lives on the concrete heap
  // backend, not on the typed base (only the aggregating factory does).
  HeapKeyedStateBackend<RowContainerStateKey>* backend_ = nullptr;
  std::shared_ptr<JoinListState> leftState_;
  std::shared_ptr<JoinListState> rightState_;
  std::unique_ptr<InternalTimerService<int64_t, int64_t>> timerService_;
  // Timer key -> state key: the timer key is the key row address, the
  // framework timer contract's int64 shape; this map is the operator-side
  // view of the pending timers (both are runtime objects, rebuilt together
  // from the restored state when timer persistence lands).
  std::unordered_map<int64_t, RowContainerStateKey> timerKeys_;
  bool fastPath_ = false;
  int64_t leftMinWatermark_ = INT64_MIN;
  int64_t rightMinWatermark_ = INT64_MIN;
  int64_t combinedWatermark_ = INT64_MIN;
  uint64_t leftLateRecordsDropped_ = 0;
  uint64_t rightLateRecordsDropped_ = 0;
  std::vector<RowVectorPtr> crossOutputs_;
  vector_size_t crossOutputRows_ = 0;
};

} // namespace facebook::velox::stateful
