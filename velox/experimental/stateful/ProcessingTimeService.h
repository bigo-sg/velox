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

#include "velox/type/Timestamp.h"
#include <folly/futures/Future.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <chrono>

namespace facebook::velox::stateful {

using ProcessingTimeCallback = std::function<void(long)>;

class ProcessingTimeSerivice {
public:
    long getCurrentProcessingTime() {
        return Timestamp::now().toMillis();
    }

    virtual folly::Future<folly::Unit> registerTimer(long timestamp, ProcessingTimeCallback target) {
        return folly::Future<folly::Unit>::makeEmpty();
    }
};

class SystemProcessingTimeService : public ProcessingTimeSerivice {
public:
    SystemProcessingTimeService() : ProcessingTimeSerivice() {
        executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    }

    folly::Future<folly::Unit> registerTimer(long timestamp, ProcessingTimeCallback callback) override {
        long currentTimestamp = getCurrentProcessingTime();
        long delay = 0;
        if (timestamp >= currentTimestamp) {
            delay = timestamp - currentTimestamp + 1;
        }
        return folly::futures::sleep(std::chrono::microseconds(delay * 1000))
            .via(executor_.get())
            .thenValue([&](auto) {
                callback(timestamp);
            });
    }
private:
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;

};
}