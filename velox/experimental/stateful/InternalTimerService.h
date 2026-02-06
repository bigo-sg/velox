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

#include <velox/experimental/stateful/InternalPriorityQueue.h>
#include <velox/experimental/stateful/TimerHeapInternalTimer.h>
#include <velox/experimental/stateful/Triggerable.h>

namespace facebook::velox::stateful {

// This class is relevant to Flink InternalTimerServiceImpl.
template <typename K, typename N>
class InternalTimerService {
 public:
  InternalTimerService(Triggerable<K, N>* triggerable)
      : triggerable_(triggerable) {}

  void registerEventTimeTimer(K key, N ns, int64_t time) {
    eventTimeTimersQueue_.add(
        std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  void deleteEventTimeTimer(K key, N ns, int64_t time) {
    eventTimeTimersQueue_.remove(
        std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  void registerProcessingTimeTimer(K key, N ns, int64_t time) {}

  void deleteProcessingTimeTimer(K key, N ns, int64_t time) {
    eventTimeTimersQueue_.remove(
        std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  int64_t currentWatermark() {
    // TODO: Implement watermark logic if needed.
    if (eventTimeTimersQueue_.peek() != nullptr) {
      return eventTimeTimersQueue_.peek()->timestamp();
    }
    return 0; // or some other default value
  }

  int64_t currentProcessingTime() {
    // TODO: Implement processing time logic if needed.
    return 0; // or some other default value
  }

  void advanceWatermark(int64_t time) {
    while (eventTimeTimersQueue_.peek() != nullptr &&
           eventTimeTimersQueue_.peek()->timestamp() <= time) {
      auto timer = eventTimeTimersQueue_.poll();
      triggerable_->onEventTime(timer);
    }
  }

  void close() {
    eventTimeTimersQueue_.clear();
  }

 private:
  Triggerable<K, N>* triggerable_;
  HeapPriorityQueue<std::shared_ptr<TimerHeapInternalTimer<K, N>>>
      eventTimeTimersQueue_;
};

} // namespace facebook::velox::stateful
