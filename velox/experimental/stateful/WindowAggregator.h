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
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/InternalTimerService.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/WindowBuffer.h"

namespace facebook::velox::stateful {

/// This class is related to XXXWindowAggProcessor in Flink.
/// Its work includes both WindowAggOperator and XXXWindowAggOperator.
class WindowAggregator : public StatefulOperator,
                         public Triggerable<int64_t, int64_t> {
 public:
  WindowAggregator(
    std::unique_ptr<exec::Operator> localAggregator,
    std::unique_ptr<exec::Operator> globalAggregator,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<KeySelector> keySelector,
    std::unique_ptr<SliceAssigner> sliceAssigner,
    const long windowInterval,
    const bool useDayLightSaving,
    const bool isEventTime,
    const int windowStartIndex,
    const int windowEndIndex);

  void initialize() override;

  void initializeState() override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  std::string name() const override {
    return "WindowAggregator";
  }

  void onEventTime(std::shared_ptr<TimerHeapInternalTimer<int64_t, int64_t>>
                       timer) override;

  void onProcessingTime(std::shared_ptr<TimerHeapInternalTimer<int64_t, long>> timer) override;

 private:
  void processWatermarkInternal(int64_t timestamp);

  int64_t sliceStateMergeTarget(int64_t sliceToMerge);

  void onTimer(std::shared_ptr<TimerHeapInternalTimer<int64_t, long>> timer);

  template<typename K>
  void fireWindow(K key, long timerTImestamp, long windowEnd);

  template<typename K>
  void clearWindow(K key, long timerTimestamp, long windowEnd);

  std::unique_ptr<exec::Operator> localAggregator_;
  std::unique_ptr<KeySelector> keySelector_;
  std::unique_ptr<SliceAssigner> sliceAssigner_;
  WindowBufferPtr windowBuffer_;
  const int64_t windowInterval_;
  const bool useDayLightSaving_;
  const int shiftTimeZone_ = 0; // TODO: support time zone shift
  const bool isEventTime_ = true;
  const int windowStartIndex_ = -1;
  const int windowEndIndex_ = -1;

  RowVectorPtr input_;
  int64_t currentProgress_ = 0;
  int64_t nextTriggerWatermark_ = 0;
  int64_t lastTriggeredProcessingTime_ = 0;
  std::shared_ptr<ValueState<uint32_t, int64_t, RowVectorPtr>> windowState_;
  std::shared_ptr<InternalTimerService<int64_t, int64_t>> windowTimerService_;
};

} // namespace facebook::velox::stateful
