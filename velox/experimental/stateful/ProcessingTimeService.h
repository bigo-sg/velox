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
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <optional>
#include <set>

namespace facebook::velox::stateful {

using ProcessingTimeCallback = std::function<void(int64_t)>;

class ProcessingTimerTask {
public:
    ProcessingTimerTask(
        int64_t time,
        ProcessingTimeCallback callback)
        : time_(time), callback_(callback) {}

    void operator()() const {
        callback_(time_);
    }
private:
    int64_t time_;
    ProcessingTimeCallback callback_;
};

class ProcessingTimeService {
public:
    int64_t getCurrentProcessingTime() {
        return TimeWindowUtil::getCurrentProcessingTime();
    }
    virtual std::optional<std::string> registerTimer(int64_t timestamp, ProcessingTimerTask target) = 0;
    virtual void cancel(const std::string& task) {}
    virtual void close() {}

    void unregister(const std::string& task) {
        auto it = std::find(registry.begin(), registry.end(), task);
        if (it != registry.end()) {
            registry.erase(it);
        }
    }

    std::string generateTimerTaskName(int64_t timestamp) {
        boost::uuids::random_generator generator;
        boost::uuids::uuid uuid = generator();
        return "proc_time_task_" + std::to_string(timestamp) + "-" + to_string(uuid);
    }
protected:
    std::set<std::string> registry;
};

class SystemProcessingTimeService : public ProcessingTimeService {
public:
    SystemProcessingTimeService() : ProcessingTimeService() {
        executor_ = std::make_shared<folly::FunctionScheduler>();
        executor_->start();
    }

    std::optional<std::string> registerTimer(int64_t timestamp, ProcessingTimerTask task) override {
        int64_t currentTimestamp = getCurrentProcessingTime();
        int64_t delay = 0;
        if (timestamp >= currentTimestamp) {
            delay = timestamp - currentTimestamp;
        }
        std::string taskName = generateTimerTaskName(timestamp);
        if (delay > 0) {
            executor_->addFunctionOnce(task, taskName, std::chrono::microseconds(delay * 1000));
            registry.emplace(taskName);
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
        registry.clear();
    }
private:
    std::shared_ptr<folly::FunctionScheduler> executor_;
};
}
