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

#include "velox/experimental/stateful/EventTimeTimerService.h"
#include "velox/experimental/stateful/ProcessingTimeScheduler.h"
#include "velox/experimental/stateful/ProcessingTimeTimerService.h"
#include "velox/experimental/stateful/Triggerable.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink InternalTimerServiceImpl.
//
// External facade kept for backward compatibility: one instance per operator
// composes an EventTimeTimerService and a ProcessingTimeTimerService and
// delegates the corresponding calls.
//
// Layout:
//   InternalTimerService           <- facade
//   ├── EventTimeTimerService      <- event-time queue + advanceWatermark
//   └── ProcessingTimeTimerService <- processing-time queue + scheduling
template <typename K, typename N>
class InternalTimerService {
 public:
  explicit InternalTimerService(Triggerable<K, N>* triggerable)
      : eventTimeService_(triggerable),
        processingTimeService_(
            triggerable,
            std::make_unique<SystemProcessingTimeScheduler>()) {}

  InternalTimerService(
      Triggerable<K, N>* triggerable,
      std::unique_ptr<ProcessingTimeScheduler> scheduler)
      : eventTimeService_(triggerable),
        processingTimeService_(triggerable, std::move(scheduler)) {}

  void registerEventTimeTimer(const K& key, const N& ns, int64_t time) {
    eventTimeService_.registerTimer(key, ns, time);
  }

  void deleteEventTimeTimer(const K& key, const N& ns, int64_t time) {
    eventTimeService_.deleteTimer(key, ns, time);
  }

  void registerProcessingTimeTimer(const K& key, const N& ns, int64_t time) {
    processingTimeService_.registerTimer(key, ns, time);
  }

  void deleteProcessingTimeTimer(const K& key, const N& ns, int64_t time) {
    processingTimeService_.deleteTimer(key, ns, time);
  }

  int64_t currentWatermark() {
    return eventTimeService_.currentWatermark();
  }

  int64_t currentProcessingTime() {
    return processingTimeService_.currentProcessingTime();
  }

  void advanceWatermark(int64_t time) {
    eventTimeService_.advanceWatermark(time);
  }

  void close() {
    eventTimeService_.close();
    processingTimeService_.close();
  }

  EventTimeTimerService<K, N>& eventTimeTimerService() {
    return eventTimeService_;
  }

  ProcessingTimeTimerService<K, N>& processingTimeTimerService() {
    return processingTimeService_;
  }

 private:
  EventTimeTimerService<K, N> eventTimeService_;
  ProcessingTimeTimerService<K, N> processingTimeService_;
};

} // namespace facebook::velox::stateful
