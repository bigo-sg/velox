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

#include "velox/experimental/stateful/InternalTimerService.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/WindowBuffer.h"
#include "velox/experimental/stateful/window/WindowProcessFunction.h"
#include "velox/experimental/stateful/window/WindowTrigger.h"

namespace facebook::velox::stateful {

class WindowContext;

/// This class is related to AggregateWindowOperator in Flink.
/// It's for group window aggregator. Rename it to GroupWindowAggregator.
class GroupWindowAggregator : public StatefulOperator,
                              public Triggerable<uint32_t, TimeWindow> {
 public:
  GroupWindowAggregator(
      std::unique_ptr<GroupWindowAggsHandler> windowAggerator,
      // TODO: maybe need to make GroupWindowAssigner a template parameter.
      std::shared_ptr<GroupWindowAssigner<TimeWindow>> windowAssigner,
      std::vector<std::unique_ptr<StatefulOperator>> targets,
      std::unique_ptr<KeySelector> keySelector,
      std::unique_ptr<SliceAssigner> sliceAssigner,
      int64_t allowedLateness,
      bool produceUpdates,
      int rowtimeIndex,
      bool isEventTime,
      int shiftTimeZone = 0);

  void initialize() override;

  void initializeState() override;

  void addInput(StreamElementPtr input) override;
  void advance() override;

  void close() override;

  std::string name() const override {
    return "GroupWindowAggregator";
  }

  void onEventTime(std::shared_ptr<TimerHeapInternalTimer<uint32_t, TimeWindow>>
                       timer) override;

 private:
  // This class is related to TriggerContext in Flink WindowOperator.
  // TODO: may need to make it implement an interface like Flink.
  class WindowTriggerContext : public TriggerContext {
   public:
    WindowTriggerContext(
        std::shared_ptr<WindowTrigger> trigger,
        std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>>
            internalTimerService,
        int shiftTimeZone);

    void open() override;

    bool onElement(
        uint32_t key,
        RowVectorPtr row,
        int64_t timestamp,
        TimeWindow window) override;

    bool onProcessingTime(TimeWindow window, int64_t time) override;

    bool onEventTime(TimeWindow window, int64_t time) override;

    void onMerge(uint32_t key, TimeWindow window) override;

    int64_t getCurrentProcessingTime() override;

    int64_t getCurrentWatermark() override;

    // TODO: support it
    // MetricGroup getMetricGroup()；

    void registerProcessingTimeTimer(
        uint32_t key,
        TimeWindow window,
        int64_t time) override;

    void registerEventTimeTimer(uint32_t key, TimeWindow window, int64_t time)
        override;

    void deleteProcessingTimeTimer(
        uint32_t key,
        TimeWindow window,
        int64_t time) override;

    void deleteEventTimeTimer(uint32_t key, TimeWindow window, int64_t time)
        override;

    int getShiftTimeZone();

    void clear(uint32_t key, TimeWindow window) override;

    StatePtr getPartitionedState(StateDescriptor& stateDescriptor) override;

    void mergePartitionedState(StateDescriptor& stateDescriptor) override;

   private:
    std::shared_ptr<WindowTrigger> trigger_;
    std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>>
        internalTimerService_;
    int shiftTimeZone_;
    TimeWindow window;
    std::vector<TimeWindow> mergedWindows;
  };

  void registerCleanupTimer(uint32_t key, TimeWindow window);
  void emitWindowResult(uint32_t key, TimeWindow window);

  std::shared_ptr<GroupWindowAssigner<TimeWindow>> windowAssigner_;
  std::unique_ptr<WindowProcessFunction<TimeWindow>> windowFunction_;
  std::unique_ptr<KeySelector> keySelector_;
  std::unique_ptr<SliceAssigner> sliceAssigner_;
  const int64_t allowedLateness_;
  const bool produceUpdates_;
  const int rowtimeIndex_ = 0;
  const bool isEventTime_;
  const int shiftTimeZone_ = 0; // TODO: support time zone shift
  GroupWindowAggsHandler* windowAggregator_;

  RowVectorPtr input_;
  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>> windowState_;
  std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>>
      windowTimerService_;
  std::shared_ptr<TriggerContext> triggerContext_;
  std::shared_ptr<WindowContext> windowContext_;
  std::shared_ptr<WindowTrigger> trigger_;
};

class WindowContext : public FunctionContext<TimeWindow> {
 public:
  WindowContext(
      GroupWindowAggsHandler* windowAggregator,
      std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
          windowState,
      std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>> timerService,
      std::shared_ptr<TriggerContext> triggerContext,
      std::shared_ptr<StreamOperatorStateHandler> stateHandler,
      int shiftTimeZone,
      bool isEventTime,
      int64_t allowedLateness);

  StatePtr getPartitionedState(StateDescriptor& stateDescriptor) override;

  uint32_t currentKey() override {
    return currentKey_;
  }

  int64_t currentProcessingTime() override;

  int64_t currentWatermark() override;

  int getShiftTimeZone() override;

  RowVectorPtr getWindowAccumulators(TimeWindow window) override;

  void setWindowAccumulators(TimeWindow window, RowVectorPtr acc) override;

  void clearWindowState(TimeWindow window) override;

  void clearPreviousState(TimeWindow window) override;

  void clearTrigger(TimeWindow window) override;

  void onMerge(TimeWindow newWindow, std::vector<TimeWindow>& mergedWindows)
      override;

  void deleteCleanupTimer(TimeWindow window) override;

  void setCurrentKey(uint32_t key) {
    currentKey_ = key;
  }

 private:
  GroupWindowAggsHandler* windowAggregator_;
  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>> windowState_;
  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
      previousWindowState_;
  std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>> timerService_;
  std::shared_ptr<TriggerContext> triggerContext_;
  std::shared_ptr<StreamOperatorStateHandler> stateHandler_;
  int shiftTimeZone_;
  bool isEventTime_;
  int64_t allowedLateness_;
  uint32_t currentKey_;
};
} // namespace facebook::velox::stateful
