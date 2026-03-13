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

#include "velox/experimental/stateful/ProcessingTimeService.h"
#include "velox/experimental/stateful/InternalPriorityQueue.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/Triggerable.h"
#include "velox/common/base/Exceptions.h"
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace facebook::velox::stateful {

template <typename K, typename N>
class InternalTimerService {
 public:
  virtual void registerEventTimeTimer(K key, N ns, int64_t time) {}
  virtual void deleteEventTimeTimer(K key, N ns, int64_t time) {}
  virtual void registerProcessingTimeTimer(K key, N ns, int64_t time) {}
  virtual void deleteProcessingTimeTimer(K key, N ns, int64_t time) {}
  virtual int64_t currentWatermark() { return 0;}
  virtual int64_t currentProcessingTime() { return 0; }
  virtual void advanceWatermark(int64_t time) {}
  virtual void close() {}
};

// This class is relevant to Flink InternalTimerServiceImpl.
template <typename K, typename N>
class InternalTimerServiceImpl : public InternalTimerService<K, N> {
 public:
  InternalTimerServiceImpl(Triggerable<K, N>* triggerable)
      : triggerable_(triggerable), processingTimeService_(std::make_shared<SystemProcessingTimeService>()) {}

  void registerEventTimeTimer(K key, N ns, int64_t time) override {
    eventTimeTimersQueue_.add(std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  void deleteEventTimeTimer(K key, N ns, int64_t time) override {
    eventTimeTimersQueue_.remove(std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  void registerProcessingTimeTimer(K key, N ns, int64_t time) override {
    const std::shared_ptr<TimerHeapInternalTimer<K, N>>& oldHead = processingTimeTimersQueue_.empty() ? nullptr : processingTimeTimersQueue_.peek();
    if (processingTimeTimersQueue_.add(std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns))) {
      int64_t nextTriggerTime = oldHead != nullptr ? oldHead->timestamp() :  std::numeric_limits<int64_t>::max() ;
      if (time < nextTriggerTime) {
        if (nextTimer_.has_value()) {
          processingTimeService_->cancel(nextTimer_.value());
        }
        nextTimer_ = processingTimeService_->registerTimer(time, ProcessingTimerTask(time, [&](int64_t processingTime) {
          onProcessingTime(processingTime);
        }));
      }
    }
  }

  void deleteProcessingTimeTimer(K key, N ns, int64_t time) override {
    processingTimeTimersQueue_.remove(std::make_shared<TimerHeapInternalTimer<K, N>>(time, key, ns));
  }

  int64_t currentWatermark() override {
    // TODO: Implement watermark logic if needed.
    const std::shared_ptr<TimerHeapInternalTimer<K, N>>& timer = eventTimeTimersQueue_.empty() ? nullptr : eventTimeTimersQueue_.peek();
    if (timer != nullptr) {
      return timer->timestamp();
    }
    return 0; // or some other default value
  }

  int64_t currentProcessingTime() override {
    // TODO: Implement processing time logic if needed.
    return 0; // or some other default value
  }

  void advanceWatermark(int64_t time) override {
    const std::shared_ptr<TimerHeapInternalTimer<K, N>>& timer = eventTimeTimersQueue_.empty() ? nullptr : eventTimeTimersQueue_.peek();
    while (timer != nullptr && timer->timestamp() <= time) {
      auto timer = eventTimeTimersQueue_.poll();
      triggerable_->onEventTime(timer);
    }
  }

  void close() override {
    eventTimeTimersQueue_.clear();
    processingTimeTimersQueue_.clear();
    processingTimeService_->close();
  }

 private:
  void onProcessingTime(int64_t time) {
    std::string taskName = "";
    if (nextTimer_.has_value()) {
      taskName = nextTimer_.value();
    }
    nextTimer_ = std::nullopt;
    std::shared_ptr<TimerHeapInternalTimer<K, N>> timer = nullptr;
    bool triggerOnProcessingTime = true;
    const std::shared_ptr<std::mutex> mtx = triggerable_->getMutex();
    if (mtx) {
      std::lock_guard<std::mutex> lock(*mtx);
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
        processingTimeService_->unregister(taskName);
      }
    }

    if (timer != nullptr && !nextTimer_.has_value()) {
      nextTimer_ = processingTimeService_->registerTimer(timer->timestamp(), ProcessingTimerTask(timer->timestamp(), [&](int64_t processingTime) {
        onProcessingTime(processingTime);
      }));
    }
  }

  Triggerable<K, N>* triggerable_;
  std::optional<std::string> nextTimer_;
  std::shared_ptr<ProcessingTimeService> processingTimeService_;
  HeapPriorityQueue<std::shared_ptr<TimerHeapInternalTimer<K, N>>, HeapTimerComparator<K, N>, HeapTimerHasher<K, N>, HeapTimerEquals<K, N>> eventTimeTimersQueue_;
  HeapPriorityQueue<std::shared_ptr<TimerHeapInternalTimer<K, N>>, HeapTimerComparator<K, N>, HeapTimerHasher<K, N>, HeapTimerEquals<K, N>> processingTimeTimersQueue_;
};

namespace {
template <typename K, typename N>
std::unordered_map<std::string, std::shared_ptr<InternalTimerService<K, N>>>&
timerServices() {
  static std::unordered_map<std::string, std::shared_ptr<InternalTimerService<K, N>>>
      services;
  return services;
}
} // namespace

/// Adds timer service instance to the registry using service ID as the key.
/// Throws if timer service with the same ID is already present. Always returns
/// true. The return value makes it easy to use with FB_ANONYMOUS_VARIABLE.
template <typename K, typename N>
bool registerTimerService(
    const std::string& serviceId,
    std::shared_ptr<InternalTimerService<K, N>> service) {
  bool ok = timerServices<K, N>().insert({serviceId, service}).second;
  VELOX_CHECK(
      ok,
      "InternalTimerService with ID '{}' is already registered",
      serviceId);
  return true;
}

/// Removes the timer service with specified ID from the registry. Returns true
/// if timer service was removed and false if timer service didn't exist.
template <typename K, typename N>
bool unregisterTimerService(const std::string& serviceId) {
  auto count = timerServices<K, N>().erase(serviceId);
  return count == 1;
}

/// Returns a timer service with specified ID. Throws if timer service doesn't
/// exist.
template <typename K, typename N>
std::shared_ptr<InternalTimerService<K, N>> getTimerService(
    const std::string& serviceId) {
  auto& services = timerServices<K, N>();
  auto it = services.find(serviceId);
  VELOX_CHECK(
      it != services.end(),
      "InternalTimerService with ID '{}' not registered",
      serviceId);
  return it->second;
}

/// Returns a map of all (serviceId -> timerService) pairs currently
/// registered.
template <typename K, typename N>
const std::unordered_map<std::string, std::shared_ptr<InternalTimerService<K, N>>>&
getAllTimerServices() {
  return timerServices<K, N>();
}

} // namespace facebook::velox::stateful
