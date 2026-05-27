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

#include <folly/executors/FunctionScheduler.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <functional>
#include <optional>
#include <set>
#include <string>

#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

using ProcessingTimeCallback = std::function<void(int64_t)>;

// A task scheduled to run at a specific wall-clock timestamp.
class ProcessingTimerTask {
 public:
  ProcessingTimerTask(int64_t time, ProcessingTimeCallback callback)
      : time_(time), callback_(std::move(callback)) {}

  void operator()() const {
    callback_(time_);
  }

 private:
  int64_t time_;
  ProcessingTimeCallback callback_;
};

// Low-level scheduler abstraction: arranges for a callback to fire at a
// given wall-clock timestamp. This is the underlying timing primitive used
// by the higher-level ProcessingTimeTimerService and corresponds to Flink's
// org.apache.flink.streaming.runtime.tasks.ProcessingTimeService interface.
class ProcessingTimeScheduler {
 public:
  virtual ~ProcessingTimeScheduler() = default;

  int64_t getCurrentProcessingTime() {
    return TimeWindowUtil::getCurrentProcessingTime();
  }

  virtual std::optional<std::string> registerTimer(
      int64_t timestamp,
      ProcessingTimerTask target) = 0;
  virtual void cancel(const std::string& /*task*/) {}
  virtual void close() {}

  void unregister(const std::string& task) {
    auto it = std::find(registry_.begin(), registry_.end(), task);
    if (it != registry_.end()) {
      registry_.erase(it);
    }
  }

  std::string generateTimerTaskName(int64_t timestamp) {
    // boost::uuids::random_generator's constructor is expensive (it opens
    // /dev/urandom to seed its underlying RNG) and the generator itself is
    // not thread-safe. A function-local static thread_local instance gives
    // each thread its own generator, constructed lazily once.
    static thread_local boost::uuids::random_generator generator;
    boost::uuids::uuid uuid = generator();
    return "proc_time_task_" + std::to_string(timestamp) + "-" +
        to_string(uuid);
  }

 protected:
  std::set<std::string> registry_;
};

// Concrete scheduler backed by a folly::FunctionScheduler running tasks on a
// background thread when the wall-clock deadline is reached.
class SystemProcessingTimeScheduler : public ProcessingTimeScheduler {
 public:
  SystemProcessingTimeScheduler() : ProcessingTimeScheduler() {
    executor_ = std::make_unique<folly::FunctionScheduler>();
    executor_->start();
  }

  std::optional<std::string> registerTimer(
      int64_t timestamp,
      ProcessingTimerTask task) override {
    int64_t currentTimestamp = getCurrentProcessingTime();
    int64_t delay = 0;
    if (timestamp >= currentTimestamp) {
      delay = timestamp - currentTimestamp;
    }
    std::string taskName = generateTimerTaskName(timestamp);
    if (delay > 0) {
      executor_->addFunctionOnce(
          std::move(task), taskName, std::chrono::microseconds(delay * 1000));
      registry_.emplace(taskName);
    } else {
      task();
    }
    return std::make_optional<std::string>(taskName);
  }

  void cancel(const std::string& task) override {
    auto it = std::find(registry_.begin(), registry_.end(), task);
    if (it != registry_.end()) {
      executor_->cancelFunction(task);
      registry_.erase(it);
    }
  }

  void close() override {
    if (executor_) {
      executor_->shutdown();
    }
    registry_.clear();
  }

 private:
  std::unique_ptr<folly::FunctionScheduler> executor_;
};

} // namespace facebook::velox::stateful
