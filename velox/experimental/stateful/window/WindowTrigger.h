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

#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

class TriggerContext;

/// This class is relevant to Flink WindowTrigger.
class WindowTrigger {
 public:
  virtual void open(std::shared_ptr<TriggerContext> ctx) = 0;

  virtual bool onElement(
      uint32_t key,
      RowVectorPtr element,
      int64_t timestamp,
      TimeWindow window) = 0;

  virtual bool onProcessingTime(TimeWindow window, int64_t time) = 0;

  virtual bool onEventTime(TimeWindow window, int64_t time) = 0;

  virtual bool canMerge() {
    return false;
  }

  // Use TriggerContext instead of OnMergeContext.
  virtual void onMerge(
      uint32_t key,
      TimeWindow window,
      std::shared_ptr<TriggerContext> mergeContext) = 0;

  virtual void clear(uint32_t key, TimeWindow window) = 0;

  virtual int64_t triggerTime(TimeWindow window);

 protected:
  std::shared_ptr<TriggerContext> ctx_;
};

class AfterEndOfWindow : public WindowTrigger {
 public:
  void open(std::shared_ptr<TriggerContext> ctx);

  bool onElement(
      uint32_t key,
      RowVectorPtr element,
      int64_t timestamp,
      TimeWindow window) override;

  bool onProcessingTime(TimeWindow window, int64_t time) override;

  bool onEventTime(TimeWindow window, int64_t time) override;

  void clear(uint32_t key, TimeWindow window) override;

  bool canMerge() override;

  void onMerge(
      uint32_t key,
      TimeWindow window,
      std::shared_ptr<TriggerContext> mergeContext) override;
};

class TriggerContext : public std::enable_shared_from_this<TriggerContext> {
 public:
  virtual void open() = 0;

  virtual bool onElement(
      uint32_t key,
      RowVectorPtr row,
      int64_t timestamp,
      TimeWindow window) = 0;

  virtual bool onProcessingTime(TimeWindow window, int64_t time) = 0;

  virtual bool onEventTime(TimeWindow window, int64_t time) = 0;

  virtual void onMerge(uint32_t key, TimeWindow window) = 0;

  virtual int64_t getCurrentProcessingTime() = 0;

  virtual int64_t getCurrentWatermark() = 0;

  // TODO: support it
  // MetricGroup getMetricGroup()；

  virtual void registerProcessingTimeTimer(
      uint32_t key,
      TimeWindow window,
      int64_t time) = 0;

  virtual void
  registerEventTimeTimer(uint32_t key, TimeWindow window, int64_t time) = 0;

  virtual void
  deleteProcessingTimeTimer(uint32_t key, TimeWindow window, int64_t time) = 0;

  virtual void
  deleteEventTimeTimer(uint32_t key, TimeWindow window, int64_t time) = 0;

  virtual int getShiftTimeZone() = 0;

  virtual void clear(uint32_t key, TimeWindow window) = 0;

  virtual StatePtr getPartitionedState(StateDescriptor& stateDescriptor) = 0;

  virtual void mergePartitionedState(StateDescriptor& stateDescriptor) = 0;
};
} // namespace facebook::velox::stateful
