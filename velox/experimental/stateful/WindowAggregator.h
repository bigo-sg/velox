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

#include "velox/experimental/stateful/InternalTimerService.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/WindowBuffer.h"

namespace facebook::velox::stateful {

/// This class is related to XXXWindowAggProcessor in Flink.
/// It's work includes both WindowAggOperator and XXXWindowAggOperator.
class WindowAggregator : public StatefulOperator, public Triggerable<uint32_t, long> {
 public:
  WindowAggregator(
    std::unique_ptr<exec::Operator> localAggerator,
    std::unique_ptr<exec::Operator> globalAggerator,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<KeySelector> keySelector,
    std::unique_ptr<SliceAssigner> sliceAssigner,
    const long windowInterval,
    const bool useDayLightSaving);

  void initialize() override;

  void addInput(RowVectorPtr input) override;

  void getOutput() override;

  void close() override;

  std::string name() const override {
    return "WindowAggregator";
  }

  void onEventTime(std::shared_ptr<TimerHeapInternalTimer<uint32_t, long>> timer) override;

 private:
  void processWatermarkInternal(long timestamp) override;

  long sliceStateMergeTarget(long sliceToMerge);

  std::unique_ptr<exec::Operator> localAggerator_;
  std::unique_ptr<KeySelector> keySelector_;
  std::unique_ptr<SliceAssigner> sliceAssigner_;
  WindowBufferPtr windowBuffer_;
  const long windowInterval_;
  const bool useDayLightSaving_;
  const int shiftTimeZone_ = 0; // TODO: support time zone shift
  const bool isEventTime = true; // TODO: support processing time

  RowVectorPtr input_;
  long currentProgress_ = 0;
  long nextTriggerWatermark_ = 0;
  std::shared_ptr<ValueState<uint32_t, long, RowVectorPtr>> windowState_;
  std::shared_ptr<InternalTimerService<uint32_t, long>> windowTimerService_;
};
} // namespace facebook::velox::stateful
