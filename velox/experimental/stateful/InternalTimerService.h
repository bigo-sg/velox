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

#include <velox/experimental/stateful/window/TimeWindowUtil.h>
#include <velox/experimental/stateful/ProcessingTimeService.h>
#include <velox/experimental/stateful/InternalPriorityQueue.h>
#include <velox/experimental/stateful/TimerHeapInternalTimer.h>
#include <velox/experimental/stateful/Triggerable.h>
#include <limits>
#include <optional>

namespace facebook::velox::stateful {

// This class is relevent to flink InternalTimerServiceImpl.
template<typename K, typename N>
class InternalTimerService {
 public:
  InternalTimerService(Triggerable<K, N>* triggerable)
      : triggerable_(triggerable), processingTimeService_(std::make_shared<SystemProcessingTimeService>()) {}

  void registerEventTimeTimer(K key, N ns, long time) {
    eventTimeTimersQueue_.add(std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  void deleteEventTimeTimer(K key, N ns, long time) {
    eventTimeTimersQueue_.remove(std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  void registerProcessingTimeTimer(K key, N ns, long time) {
    std::shared_ptr<TimerHeapInternalTimer<K, N>> oldHead = processingTimeTimersQueue_.peek();
    processingTimeTimersQueue_.add(std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
    long nextTriggerTime = oldHead != nullptr ? oldHead->timestamp() :  std::numeric_limits<long>::max() ;
    if (time < nextTriggerTime) {
      if (nextTimer_.has_value()) {
        processingTimeService_->cancel(nextTimer_.value());
      }
      nextTimer_ = processingTimeService_->registerTimer(time, ProcessingTimerTask(time, [&](long processingTime) {
        onProcessingTime(processingTime);
      }));
    }
  }

  void deleteProcessingTimeTimer(K key, N ns, long time) {
    processingTimeTimersQueue_.remove(std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  long currentWatermark() {
    // TODO: Implement watermark logic if needed.
    if (eventTimeTimersQueue_.peek() != nullptr) {
      return eventTimeTimersQueue_.peek()->timestamp();
    }
    return 0; // or some other default value
  }

  long currentProcessingTime() {
    // TODO: Implement processing time logic if needed.
    return 0; // or some other default value
  }

  void advanceWatermark(long time) {
    while (eventTimeTimersQueue_.peek() != nullptr &&
           eventTimeTimersQueue_.peek()->timestamp() <= time) {
      auto timer = eventTimeTimersQueue_.poll();
      triggerable_->onEventTime(timer);
    }
  }

  void close() {
    eventTimeTimersQueue_.clear();
    processingTimeTimersQueue_.clear();
    processingTimeService_->close();
  }

 private:
  void onProcessingTime(long time) {
    std::string taskName = "";
    if (nextTimer_.has_value()) {
      taskName = nextTimer_.value();
    }
    nextTimer_ = std::nullopt;
    std::shared_ptr<TimerHeapInternalTimer<K, N>> timer = nullptr;
    bool triggerOnProcessingTime = true;
    while (triggerOnProcessingTime && !processingTimeTimersQueue_.empty()) {
      timer = processingTimeTimersQueue_.peek();
      if (!timer || timer->timestamp() > time) {
        triggerOnProcessingTime = false;
        continue;
      }
      processingTimeTimersQueue_.poll();
      triggerable_->onProcessingTime(timer);
      timer = nullptr;
    }

    if (!taskName.empty()) {
      processingTimeService_->finish(taskName);
    }

    if (timer != nullptr && !nextTimer_.has_value()) {
      nextTimer_ = processingTimeService_->registerTimer(timer->timestamp(), ProcessingTimerTask(timer->timestamp(), [&](long processingTime) {
        onProcessingTime(processingTime);
      }));
    }
  }

  Triggerable<K, N>* triggerable_;
  std::optional<std::string> nextTimer_;
  std::shared_ptr<ProcessingTimeSerivice> processingTimeService_;
  HeapPriorityQueue<std::shared_ptr<TimerHeapInternalTimer<K, N>>> eventTimeTimersQueue_;
  HeapPriorityQueue<std::shared_ptr<TimerHeapInternalTimer<K, N>>> processingTimeTimersQueue_;
};

} // namespace facebook::velox::stateful
