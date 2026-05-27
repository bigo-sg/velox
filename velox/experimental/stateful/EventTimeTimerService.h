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
#include <utility>

#include "velox/experimental/stateful/InternalPriorityQueue.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"

namespace facebook::velox::stateful {

// Manages event-time timers for a single operator. Timers are kept in a
// priority queue ordered by timestamp. Triggering is driven externally by
// watermark advancement via advanceWatermark().
template <typename K, typename N>
class EventTimeTimerService {
 public:
  explicit EventTimeTimerService(Triggerable<K, N>* triggerable)
      : triggerable_(triggerable) {}

  void registerTimer(const K& key, const N& ns, int64_t time) {
    timersQueue_.add(TimerHeapInternalTimer<K, N>(time, key, ns));
  }

  void deleteTimer(const K& key, const N& ns, int64_t time) {
    timersQueue_.remove(TimerHeapInternalTimer<K, N>(time, key, ns));
  }

  // Returns the timestamp of the earliest pending event-time timer, or 0 if
  // no timer is registered. Preserves the original InternalTimerService
  // semantics used by existing callers.
  int64_t currentWatermark() {
    if (timersQueue_.empty()) {
      return 0;
    }
    return timersQueue_.peek().timestamp();
  }

  // Polls all event-time timers whose timestamp is <= the given watermark
  // and dispatches them to the Triggerable.
  void advanceWatermark(int64_t time) {
    while (!timersQueue_.empty() && timersQueue_.peek().timestamp() <= time) {
      TimerHeapInternalTimer<K, N> timer = timersQueue_.poll();
      // Triggerable callbacks still take a shared_ptr; wrap the popped value.
      triggerable_->onEventTime(
          std::make_shared<TimerHeapInternalTimer<K, N>>(std::move(timer)));
    }
  }

  void close() {
    timersQueue_.clear();
  }

  bool empty() const {
    return timersQueue_.empty();
  }

  size_t size() const {
    return timersQueue_.size();
  }

 private:
  Triggerable<K, N>* triggerable_;
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
