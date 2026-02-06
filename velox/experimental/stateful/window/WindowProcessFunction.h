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

#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/window/GroupWindowAggsHandler.h"
#include "velox/experimental/stateful/window/GroupWindowAssigner.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/vector/ComplexVector.h"

#include <type_traits>
#include <vector>

namespace facebook::velox::stateful {

// This class is relevant to Flink InternalWindowProcessFunction::Context.
template <typename W, typename = std::enable_if_t<std::is_base_of_v<Window, W>>>
class FunctionContext {
 public:
  virtual StatePtr getPartitionedState(StateDescriptor& stateDescriptor) = 0;

  virtual uint32_t currentKey() = 0;

  virtual int64_t currentProcessingTime() = 0;

  virtual int64_t currentWatermark() = 0;

  virtual int getShiftTimeZone() = 0;

  virtual RowVectorPtr getWindowAccumulators(W window) = 0;

  virtual void setWindowAccumulators(W window, RowVectorPtr acc) = 0;

  virtual void clearWindowState(W window) = 0;

  virtual void clearPreviousState(W window) = 0;

  virtual void clearTrigger(W window) = 0;

  virtual void onMerge(W newWindow, std::vector<W>& mergedWindows) = 0;

  virtual void deleteCleanupTimer(W window) = 0;
};

// This class is relevant to Flink InternalWindowProcessFunction.
template <typename W, typename = std::enable_if_t<std::is_base_of_v<Window, W>>>
class WindowProcessFunction {
 public:
  WindowProcessFunction(
      GroupWindowAssignerPtr windowAssigner,
      exec::Operator* windowAggregator,
      int64_t allowedLateness)
      : windowAssigner_(windowAssigner),
        windowAggregator_(
            dynamic_cast<GroupWindowAggsHandler*>(windowAggregator)),
        allowedLateness_(allowedLateness) {}

  virtual void open(std::shared_ptr<FunctionContext<W>> ctx) {
    ctx_ = std::move(ctx);
  }

  virtual std::vector<W> assignStateNamespace(
      uint32_t key,
      RowVectorPtr inputRow,
      int64_t timestamp) = 0;

  virtual std::vector<W> assignActualWindows(
      RowVectorPtr inputRow,
      int64_t timestamp) = 0;

  // TODO: implement it when necessary.
  virtual void prepareAggregateAccumulatorForEmit(uint32_t key, W window) = 0;

  virtual void cleanWindowIfNeeded(W window, int64_t currentTime) = 0;

  // TODO: implement it when necessary.
  virtual void close() {}

 protected:
  bool isWindowLate(W window) {
    return (
        windowAssigner_->isEventTime() &&
        (TimeWindowUtil::toEpochMillsForTimer(
             TimeWindowUtil::cleanupTime(
                 window.maxTimestamp(),
                 allowedLateness_,
                 windowAssigner_->isEventTime()),
             ctx_->getShiftTimeZone()) <= ctx_->currentWatermark()));
  }

  bool isCleanupTime(W window, int64_t time) {
    return time ==
        TimeWindowUtil::toEpochMillsForTimer(
               TimeWindowUtil::cleanupTime(
                   window.maxTimestamp(),
                   allowedLateness_,
                   windowAssigner_->isEventTime()),
               ctx_->getShiftTimeZone());
  }

  GroupWindowAssignerPtr windowAssigner_;
  // TODO: windowAggregator may need state
  GroupWindowAggsHandler* windowAggregator_;
  int64_t allowedLateness_;
  std::shared_ptr<FunctionContext<W>> ctx_;
};

using WindowProcessFunctionPtr =
    std::shared_ptr<WindowProcessFunction<TimeWindow>>;

class MergingWindowSet;
class MergingFunction;

class MergingWindowProcessFunction : public WindowProcessFunction<TimeWindow> {
 public:
  MergingWindowProcessFunction(
      std::shared_ptr<MergingWindowAssigner> windowAssigner,
      exec::Operator* windowAggregator,
      int64_t allowedLateness);

  void open(std::shared_ptr<FunctionContext<TimeWindow>> ctx) override;

  std::vector<TimeWindow> assignStateNamespace(
      uint32_t key,
      RowVectorPtr inputRow,
      int64_t timestamp) override;

  std::vector<TimeWindow> assignActualWindows(
      RowVectorPtr inputRow,
      int64_t timestamp) override;

  void cleanWindowIfNeeded(TimeWindow window, int64_t currentTime) override;

  void prepareAggregateAccumulatorForEmit(uint32_t key, TimeWindow window)
      override;

  void close() override;

 private:
  using WindowProcessFunction<TimeWindow>::windowAssigner_;
  using WindowProcessFunction<TimeWindow>::windowAggregator_;
  std::shared_ptr<MergingWindowSet> mergingWindows_;
  std::shared_ptr<MergingFunction> mergingFunction_;
  std::vector<TimeWindow> reuseActualWindows_;
};

class DefaultAccMergingConsumer {
 public:
  DefaultAccMergingConsumer(
      std::shared_ptr<FunctionContext<TimeWindow>> ctx,
      GroupWindowAggsHandler* windowAggregator);

  void accept(
      TimeWindow stateWindowResult,
      std::vector<TimeWindow> stateWindowsToBeMerged);

 private:
  std::shared_ptr<FunctionContext<TimeWindow>> ctx_;
  GroupWindowAggsHandler* windowAggregator_;
};
} // namespace facebook::velox::stateful
