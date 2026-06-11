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
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/InternalPriorityQueue.h"
#include "velox/experimental/stateful/ProcessingTimeScheduler.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"

namespace facebook::velox::stateful {

// Manages processing-time timers for a single operator. Pending timers are
// kept in a priority queue ordered by timestamp; the head of the queue is
// scheduled with the underlying ProcessingTimeScheduler so it fires at its
// wall-clock time. When the timer fires, all timers due at that moment are
// drained and forwarded to the Triggerable.
//
// All queue and nextTimer_ access is serialized with Triggerable::mtx_.
// Do not hold mtx_ across scheduler_->registerTimer/cancel/unregister calls
// (delay==0 may invoke callbacks inline and deadlock on std::mutex).
template <typename K, typename N>
class ProcessingTimeTimerService {
 public:
  ProcessingTimeTimerService(
      Triggerable<K, N>* triggerable,
      std::unique_ptr<ProcessingTimeScheduler> scheduler)
      : triggerable_(triggerable), scheduler_(std::move(scheduler)) {}

  void registerTimer(const K& key, const N& ns, int64_t time) {
    std::lock_guard<std::mutex> lock(*mutex());
    bool reschedule = false;
    int64_t scheduleAt = time;
    const int64_t oldHeadTimestamp = timersQueue_.empty()
        ? std::numeric_limits<int64_t>::max()
        : timersQueue_.peek().timestamp();
    if (timersQueue_.add(TimerHeapInternalTimer<K, N>(time, key, ns))) {
      if (time < oldHeadTimestamp) {
        reschedule = true;
        scheduleAt = time;
      }
    }
    if (!reschedule) {
      return;
    }

    std::optional<std::string> timerToCancel;
    if (nextTimer_.has_value()) {
      timerToCancel = nextTimer_.value();
      nextTimer_ = std::nullopt;
    }
    if (timerToCancel.has_value()) {
      scheduler_->cancel(timerToCancel.value());
    }

    auto registered = scheduler_->registerTimer(
        scheduleAt, ProcessingTimerTask(scheduleAt, [this](int64_t processingTime) {
          onProcessingTime(processingTime);
        }));
    if (!registered.has_value()) {
      return;
    }
    if (!nextTimer_.has_value()) {
      nextTimer_ = std::move(registered);
    } else {
      scheduler_->cancel(registered.value());
    }
  }

  void deleteTimer(const K& key, const N& ns, int64_t time) {
    std::lock_guard<std::mutex> lock(*mutex());
    timersQueue_.remove(TimerHeapInternalTimer<K, N>(time, key, ns));
  }

  int64_t currentProcessingTime() {
    if (scheduler_) {
      return scheduler_->getCurrentProcessingTime();
    }
    return 0;
  }

  void close() {
    {
      std::lock_guard<std::mutex> lock(*mutex());
      timersQueue_.clear();
      nextTimer_ = std::nullopt;
    }
    if (scheduler_) {
      scheduler_->close();
    }
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(*mutex());
    return timersQueue_.empty();
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(*mutex());
    return timersQueue_.size();
  }

 private:
  std::shared_ptr<std::mutex> mutex() const {
    auto mtx = triggerable_->getMutex();
    VELOX_CHECK(mtx, "Triggerable mutex must be initialized");
    return mtx;
  }

  // Invoked by the ProcessingTimeScheduler when the scheduled wall-clock
  // timestamp is reached. Drains all due timers under the Triggerable's
  // mutex and re-arms the scheduler for the next head if any.
  void onProcessingTime(int64_t time) {
    std::string taskName;
    std::optional<int64_t> nextHeadTimestamp;

    std::lock_guard<std::mutex> lock(*mutex());
    if (nextTimer_.has_value()) {
      taskName = nextTimer_.value();
      nextTimer_ = std::nullopt;
    }

    while (!timersQueue_.empty()) {
      const auto& head = timersQueue_.peek();
      if (head.timestamp() > time) {
        nextHeadTimestamp = head.timestamp();
        break;
      }
      TimerHeapInternalTimer<K, N> popped = timersQueue_.poll();
      triggerable_->onProcessingTime(
          std::make_shared<TimerHeapInternalTimer<K, N>>(std::move(popped)));
    }
  
    triggerable_->onProcessingTime(time);
    if (!taskName.empty()) {
      scheduler_->unregister(taskName);
    }

    std::optional<int64_t> scheduleTs;
    if (nextHeadTimestamp.has_value() && !nextTimer_.has_value()) {
      scheduleTs = nextHeadTimestamp;
    }
    if (!scheduleTs.has_value()) {
      return;
    }
  
    auto registered = scheduler_->registerTimer(
        scheduleTs.value(),
        ProcessingTimerTask(scheduleTs.value(), [this](int64_t processingTime) {
          onProcessingTime(processingTime);
        }));
    if (!registered.has_value()) {
      return;
    }

    if (!nextTimer_.has_value()) {
      nextTimer_ = std::move(registered);
    } else {
      scheduler_->cancel(registered.value());
    }
  }

  Triggerable<K, N>* triggerable_;
  std::unique_ptr<ProcessingTimeScheduler> scheduler_;
  std::optional<std::string> nextTimer_;
  HeapPriorityQueue<
      TimerHeapInternalTimer<K, N>,
      HeapTimerPriorityFn<K, N>,
      /*kMaxQueue=*/false,
      std::allocator<TimerHeapInternalTimer<K, N>>,
      HeapTimerHasher<K, N>,
      HeapTimerEquals<K, N>>
      timersQueue_;
};

} // namespace facebook::velox::stateful
