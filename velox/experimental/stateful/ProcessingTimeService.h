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

#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include <folly/executors/FunctionScheduler.h>
#include <chrono>
#include <optional>

namespace facebook::velox::stateful {

using ProcessingTimeCallback = std::function<void(long)>;

class ProcessingTimerTask {
public:
    ProcessingTimerTask(
        long time,
        ProcessingTimeCallback callback)
        : time_(time), callback_(callback) {}

    void operator()() const {
        callback_(time_);
    }
private:
    long time_;
    ProcessingTimeCallback callback_;
};

class ProcessingTimeSerivice {
public:
    long getCurrentProcessingTime() {
        return TimeWindowUtil::getCurrentProcessingTime();
    }
    virtual std::optional<std::string> registerTimer(long timestamp, ProcessingTimerTask target) {
        std::optional<std::string> task;
        return task;
    }
    virtual void cancel(const std::string& task) {}
    virtual void close() {}

    void finish(const std::string& task) {
        auto it = std::find(registry.begin(), registry.end(), task);
        if (it != registry.end()) {
            registry.erase(it);
        }
    }

    std::string generateTimerTaskName(long timestamp) {
        return "task_" + std::to_string(timestamp);
    }
protected:
    std::vector<std::string> registry;
};

class SystemProcessingTimeService : public ProcessingTimeSerivice {
public:
    SystemProcessingTimeService() : ProcessingTimeSerivice() {
        executor_ = std::make_shared<folly::FunctionScheduler>();
        executor_->start();
    }

    std::optional<std::string> registerTimer(long timestamp, ProcessingTimerTask task) override {
        long currentTimestamp = getCurrentProcessingTime();
        long delay = 0;
        if (timestamp >= currentTimestamp) {
            delay = timestamp - currentTimestamp;
        }
        std::string taskName = generateTimerTaskName(timestamp);
        if (delay > 0) {
            executor_->addFunctionOnce(task, taskName, std::chrono::microseconds(delay * 1000));
            registry.emplace_back(taskName);
        } else {
            task();
        }
        return std::make_optional<std::string>(taskName);
    }

    void cancel(const std::string& task) override {
        auto it = std::find(registry.begin(), registry.end(), task);
        if (it != registry.end()) {
            executor_->cancelFunction(task);
            registry.erase(it);
        }
    }

    void close() override {
        if (executor_) {
            executor_->shutdown();
        }
    }
private:
    std::shared_ptr<folly::FunctionScheduler> executor_;
};
}
