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
template <typename K, typename N>
class ProcessingTimeTimerService {
 public:
  ProcessingTimeTimerService(
      Triggerable<K, N>* triggerable,
      std::unique_ptr<ProcessingTimeScheduler> scheduler)
      : triggerable_(triggerable), scheduler_(std::move(scheduler)) {}

  void registerTimer(K key, N ns, int64_t time) {
    // Capture the timestamp of the current head before insertion. If the
    // queue is empty, treat as +inf so the new timer is unconditionally the
    // head.
    const int64_t oldHeadTimestamp = timersQueue_.empty()
        ? std::numeric_limits<int64_t>::max()
        : timersQueue_.peek().timestamp();
    if (timersQueue_.add(TimerHeapInternalTimer<K, N>(time, key, ns))) {
      if (time < oldHeadTimestamp) {
        if (nextTimer_.has_value()) {
          scheduler_->cancel(nextTimer_.value());
        }
        nextTimer_ = scheduler_->registerTimer(
            time, ProcessingTimerTask(time, [this](int64_t processingTime) {
              onProcessingTime(processingTime);
            }));
      }
    }
  }

  void deleteTimer(K key, N ns, int64_t time) {
    timersQueue_.remove(TimerHeapInternalTimer<K, N>(time, key, ns));
  }

  int64_t currentProcessingTime() {
    if (scheduler_) {
      return scheduler_->getCurrentProcessingTime();
    }
    return 0;
  }

  void close() {
    timersQueue_.clear();
    if (scheduler_) {
      scheduler_->close();
    }
  }

  bool empty() const {
    return timersQueue_.empty();
  }

  size_t size() const {
    return timersQueue_.size();
  }

 private:
  // Invoked by the ProcessingTimeScheduler when the scheduled wall-clock
  // timestamp is reached. Drains all due timers under the Triggerable's
  // mutex and re-arms the scheduler for the next head if any.
  void onProcessingTime(int64_t time) {
    std::string taskName;
    if (nextTimer_.has_value()) {
      taskName = nextTimer_.value();
    }
    nextTimer_ = std::nullopt;

    // Timestamp of the next pending timer that was NOT due (head after the
    // drain). std::nullopt means the queue is empty after draining.
    std::optional<int64_t> nextHeadTimestamp;
    bool keepDraining = true;
    const std::shared_ptr<std::mutex> mtx = triggerable_->getMutex();
    if (mtx) {
      std::lock_guard<std::mutex> lock(*mtx);
      while (keepDraining && !timersQueue_.empty()) {
        const auto& head = timersQueue_.peek();
        if (head.timestamp() > time) {
          nextHeadTimestamp = head.timestamp();
          keepDraining = false;
          continue;
        }
        TimerHeapInternalTimer<K, N> popped = timersQueue_.poll();
        // Triggerable callbacks still take a shared_ptr; wrap the popped value.
        triggerable_->onProcessingTime(
            std::make_shared<TimerHeapInternalTimer<K, N>>(std::move(popped)));
      }
      if (!taskName.empty()) {
        scheduler_->unregister(taskName);
      }
    }

    if (nextHeadTimestamp.has_value() && !nextTimer_.has_value()) {
      const int64_t ts = *nextHeadTimestamp;
      nextTimer_ = scheduler_->registerTimer(
          ts, ProcessingTimerTask(ts, [this](int64_t processingTime) {
            onProcessingTime(processingTime);
          }));
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
