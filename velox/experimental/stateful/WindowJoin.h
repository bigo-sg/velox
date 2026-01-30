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

#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/experimental/stateful/InternalTimerService.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
<<<<<<< HEAD
=======
#include "velox/experimental/stateful/StreamElement.h"
>>>>>>> 72bb1ba5f (refactor(stateful): route addInput through StreamElementPtr)
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"
#include "velox/experimental/stateful/join/JoinRecordStateView.h"

namespace facebook::velox::stateful {

class WindowJoin : public StatefulOperator,
                   public Triggerable<uint32_t, int64_t> {
 public:
  WindowJoin(
      std::unique_ptr<exec::Operator> leftInput,
      std::unique_ptr<exec::Operator> rightInput,
      std::unique_ptr<KeySelector> leftKeySelector,
      std::unique_ptr<KeySelector> rightKeySelector,
      std::unique_ptr<exec::Operator> probe,
      std::vector<std::unique_ptr<StatefulOperator>> targets,
      int leftWindowEndIndex,
      int rightWindowEndIndex);

  void initialize() override;

  bool isFinished() override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  std::string name() const override {
    return "WindowJoin";
  }

  void onEventTime(std::shared_ptr<TimerHeapInternalTimer<uint32_t, int64_t>>
                       timer) override;

 protected:
  int numInputs() const override {
    return 2;
  }

 private:
  void join(uint32_t key, int64_t windowEnd);

  void processWatermarkInternal(int64_t timestamp);

  void processData(
      exec::Operator* input,
      KeySelector* keySelector,
      int windowEndIndex,
      ListState<uint32_t, int64_t, RowVectorPtr>* state);

  RowVectorPtr filterWindowFiredRows(RowVectorPtr& input);

<<<<<<< HEAD
  std::map<int64_t, RowVectorPtr> partitionWindowData(
=======
  std::map<long, RowVectorPtr> partitionWindowData(
>>>>>>> 72bb1ba5f (refactor(stateful): route addInput through StreamElementPtr)
      RowVectorPtr& input,
      int windowEndIndex);

  const std::unique_ptr<exec::Operator> leftInput_;
  const std::unique_ptr<exec::Operator> rightInput_;
  const std::unique_ptr<KeySelector> leftKeySelector_;
  const std::unique_ptr<KeySelector> rightKeySelector_;
  exec::NestedLoopJoinProbe* probe_;
  std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>> leftWindowState_;
  std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>> rightWindowState_;
  const int leftWindowEndIndex_;
  const int rightWindowEndIndex_;
  std::shared_ptr<InternalTimerService<uint32_t, int64_t>> timerService_;
};

} // namespace facebook::velox::stateful
