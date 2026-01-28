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
#include "velox/experimental/stateful/GroupWindowAggregator.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

GroupWindowAggregator::GroupWindowAggregator(
    std::unique_ptr<GroupWindowAggsHandler> windowAggerator,
    std::shared_ptr<GroupWindowAssigner<TimeWindow>> windowAssigner,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<KeySelector> keySelector,
    std::unique_ptr<SliceAssigner> sliceAssigner,
    long allowedLateness,
    bool produceUpdates,
    int rowtimeIndex,
    bool isEventTime,
    int shiftTimeZone)
    : StatefulOperator(std::move(windowAggerator), std::move(targets)),
      windowAssigner_(std::move(windowAssigner)),
      keySelector_(std::move(keySelector)),
      sliceAssigner_(std::move(sliceAssigner)),
      allowedLateness_(allowedLateness),
      produceUpdates_(produceUpdates),
      rowtimeIndex_(rowtimeIndex),
      isEventTime_(isEventTime),
      shiftTimeZone_(shiftTimeZone) {
  windowAggregator_ = dynamic_cast<GroupWindowAggsHandler*>(op().get());
}

void GroupWindowAggregator::initialize() {
  StatefulOperator::initialize();

  StateDescriptor stateDesc("window-aggs");
  windowState_ = stateHandler()->getGroupValueState(stateDesc);
  windowTimerService_ = stateHandler()->createGroupWindowAggTimerService(this);
  trigger_ = std::make_shared<AfterEndOfWindow>();
  triggerContext_ = std::make_shared<WindowTriggerContext>(
      trigger_,
      windowTimerService_,
      shiftTimeZone_);
  triggerContext_->open();
  // TODO: create windowFunction_ based on windowAssigner type
  windowFunction_ =
      std::make_unique<MergingWindowProcessFunction>(
          std::dynamic_pointer_cast<MergingWindowAssigner>(windowAssigner_),
          op().get(),
          allowedLateness_);
  windowContext_ = std::make_shared<WindowContext>(
      windowAggregator_,
      windowState_,
      windowTimerService_,
      triggerContext_,
      stateHandler(),
      shiftTimeZone_,
      isEventTime_,
      allowedLateness_);
  windowFunction_->open(windowContext_);
}

void GroupWindowAggregator::addInput(RowVectorPtr input) {
  VELOX_CHECK(!input_, "Last input has not been processed");
  input_ = input;
}

void GroupWindowAggregator::getOutput() {
  if (!input_) {
    return;
  }

  // 1. Partition input by key
  std::map<uint32_t, RowVectorPtr> keyToData = keySelector_->partition(input_);
  for (const auto& [key, keyedData] : keyToData) {
    // 2. Set the current key in the context
    windowContext_->setCurrentKey(key);
    // 3. Partition the keyed data by rowtime or processing time
    std::map<uint32_t, RowVectorPtr> timestampToData = sliceAssigner_->assignSliceEnd(keyedData);
    for (const auto& [timestamp, data] : timestampToData) {
      // 4. Assign data to window
      std::vector<TimeWindow> windows = 
          windowFunction_->assignStateNamespace(key, data, timestamp);
      for (const auto& window : windows) {
        auto acc = windowState_->value(key, window);
        if (!acc) {
          // If there is no accumulator for this window, create a new one
          acc = windowAggregator_->createAccumulators();;
        }
        windowAggregator_->setAccumulators(window, acc);
        windowAggregator_->accumulate(data);
        acc = windowAggregator_->getAccumulators();
        // windowAggregator->setAccumulators(window, acc);
        windowState_->update(key, window, acc);
      }
      std::vector<TimeWindow> actualWindows =
          windowFunction_->assignActualWindows(data, timestamp);
      for (const auto& window : actualWindows) {
        bool triggerResult = triggerContext_->onElement(key, data, timestamp, window);
        if (triggerResult) {
          emitWindowResult(key, window);
        }
        // register a clean up timer for the window
        registerCleanupTimer(key, window);
      }
    }
  }

  input_.reset();
}

void GroupWindowAggregator::onEventTime(std::shared_ptr<TimerHeapInternalTimer<uint32_t, TimeWindow>> timer) {
  windowContext_->setCurrentKey(timer->key());
  if (triggerContext_->onEventTime(timer->ns(), timer->timestamp())) {
    // fire
    emitWindowResult(timer->key(), timer->ns());
  }

  if (windowAssigner_->isEventTime()) {
    windowFunction_->cleanWindowIfNeeded(timer->ns(), timer->timestamp());
  }
  auto output = windowState_->value(timer->key(), timer->ns());
  windowState_->remove(timer->key(), timer->ns());
  pushOutput(output);
}

void GroupWindowAggregator::close() {
  StatefulOperator::close();
  input_.reset();
  windowFunction_->close();
  windowState_->clear();
}

void GroupWindowAggregator::registerCleanupTimer(uint32_t key, TimeWindow window) {
  long cleanupTime =
      TimeWindowUtil::toEpochMillsForTimer(
          TimeWindowUtil::cleanupTime(window.maxTimestamp(), allowedLateness_, isEventTime_),
          shiftTimeZone_);
  if (cleanupTime == LONG_MAX) {
    // don't set a GC timer for "end of time"
    return;
  }

  if (windowAssigner_->isEventTime()) {
    triggerContext_->registerEventTimeTimer(key, window, cleanupTime);
  } else {
    triggerContext_->registerProcessingTimeTimer(key, window, cleanupTime);
  }
}

void GroupWindowAggregator::emitWindowResult(uint32_t key, TimeWindow window) {
  windowFunction_->prepareAggregateAccumulatorForEmit(key, window);
  RowVectorPtr acc = windowAggregator_->getAccumulators();
  RowVectorPtr aggResult = windowAggregator_->getValue(window);
  if (produceUpdates_) {
    // TODO: suppport it.
  } else {
    // TODO: use recordCounter_ 
    //if (!recordCounter_.recordCountIsZero(acc)) {
    if (acc) {
      // send INSERT
      pushOutput(aggResult);
    }
    // if the counter is zero, no need to send accumulate
    // there is no possible skip `if` branch when `produceUpdates` is false
  }
}

GroupWindowAggregator::WindowTriggerContext::WindowTriggerContext(
    std::shared_ptr<WindowTrigger> trigger,
    std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>> internalTimerService,
    int shiftTimeZone)
    : trigger_(std::move(trigger)),
      internalTimerService_(std::move(internalTimerService)),
      shiftTimeZone_(shiftTimeZone) {}

void GroupWindowAggregator::WindowTriggerContext::open() {
  trigger_->open(shared_from_this());
}

bool GroupWindowAggregator::WindowTriggerContext::onElement(
    uint32_t key, RowVectorPtr row, long timestamp, TimeWindow window) {
  return trigger_->onElement(key, row, timestamp, window);
}

bool GroupWindowAggregator::WindowTriggerContext::onProcessingTime(
    TimeWindow window, long time) {
  return trigger_->onProcessingTime(window, time);
}

bool GroupWindowAggregator::WindowTriggerContext::onEventTime(
    TimeWindow window, long time) {
  return trigger_->onEventTime(window, time);
}

void GroupWindowAggregator::WindowTriggerContext::onMerge(
    uint32_t key, TimeWindow window) {
  trigger_->onMerge(key, window, nullptr);
}

long GroupWindowAggregator::WindowTriggerContext::getCurrentProcessingTime() {
  return internalTimerService_->currentProcessingTime();
}

int64_t GroupWindowAggregator::WindowTriggerContext::getCurrentWatermark() {
  return internalTimerService_->currentWatermark();
}

void GroupWindowAggregator::WindowTriggerContext::registerProcessingTimeTimer(
    uint32_t key, TimeWindow window, long time) {
  internalTimerService_->registerProcessingTimeTimer(key, window, time);
}

void GroupWindowAggregator::WindowTriggerContext::registerEventTimeTimer(
    uint32_t key, TimeWindow window, long time) {
  internalTimerService_->registerEventTimeTimer(key, window, time);
}

void GroupWindowAggregator::WindowTriggerContext::deleteProcessingTimeTimer(
    uint32_t key, TimeWindow window, long time) {
  internalTimerService_->deleteProcessingTimeTimer(key, window, time);
}

void GroupWindowAggregator::WindowTriggerContext::deleteEventTimeTimer(
    uint32_t key, TimeWindow window, long time) {
  internalTimerService_->deleteEventTimeTimer(key, window, time);
}

int GroupWindowAggregator::WindowTriggerContext::getShiftTimeZone() {
  return shiftTimeZone_;
}

void GroupWindowAggregator::WindowTriggerContext::clear(uint32_t key, TimeWindow window) {
  trigger_->clear(key, window);
}

StatePtr GroupWindowAggregator::WindowTriggerContext::getPartitionedState(
    StateDescriptor& stateDescriptor) {
  // TODO: implement this method when necessary.
  VELOX_NYI();
}

void GroupWindowAggregator::WindowTriggerContext::mergePartitionedState(
      StateDescriptor& stateDescriptor) {
  // TODO: implement this method when necessary.
  VELOX_NYI();
}

WindowContext::WindowContext(
    GroupWindowAggsHandler* windowAggregator,
    std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>> windowState,
    std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>> timerService,
    std::shared_ptr<TriggerContext> triggerContext,
    std::shared_ptr<StreamOperatorStateHandler> stateHandler,
    int shiftTimeZone,
    bool isEventTime,
    long allowedLateness)
    : windowAggregator_(windowAggregator),
      windowState_(std::move(windowState)),
      timerService_(std::move(timerService)),
      triggerContext_(std::move(triggerContext)),
      stateHandler_(std::move(stateHandler)),
      shiftTimeZone_(shiftTimeZone),
      isEventTime_(isEventTime),
      allowedLateness_(allowedLateness) {
}

StatePtr WindowContext::getPartitionedState(StateDescriptor& stateDescriptor) {
  return stateHandler_->getGroupMapState(stateDescriptor);
}

long WindowContext::currentProcessingTime() {
  return timerService_->currentProcessingTime();
}

int64_t WindowContext::currentWatermark() {
  return timerService_->currentWatermark();
}

int WindowContext::getShiftTimeZone() {
  return shiftTimeZone_;
}

RowVectorPtr WindowContext::getWindowAccumulators(TimeWindow window) {
  return windowState_->value(currentKey_, window);
}

void WindowContext::setWindowAccumulators(TimeWindow window, RowVectorPtr acc) {
  windowState_->update(currentKey_, window, acc);
}

void WindowContext::clearWindowState(TimeWindow window) {
  windowState_->remove(currentKey_, window);
  windowAggregator_->cleanup(window);
}

void WindowContext::clearPreviousState(TimeWindow window) {
  if (previousWindowState_ != nullptr) {
    previousWindowState_->remove(currentKey_, window);
  }
}

void WindowContext::clearTrigger(TimeWindow window) {
  triggerContext_->clear(currentKey_, window);
}

void WindowContext::deleteCleanupTimer(TimeWindow window) {
  long cleanupTime =
      TimeWindowUtil::toEpochMillsForTimer(
          TimeWindowUtil::cleanupTime(window.maxTimestamp(), allowedLateness_, isEventTime_),
          shiftTimeZone_);
  if (cleanupTime == LONG_MAX) {
      // no need to clean up because we didn't set one
      return;
  }
  if (isEventTime_) {
      triggerContext_->deleteEventTimeTimer(currentKey_, window, cleanupTime);
  } else {
      triggerContext_->deleteProcessingTimeTimer(currentKey_, window, cleanupTime);
  }
}

void WindowContext::onMerge(TimeWindow newWindow, std::vector<TimeWindow>& mergedWindows) {
  triggerContext_->onMerge(currentKey_, newWindow);
}
} // namespace facebook::velox::stateful
