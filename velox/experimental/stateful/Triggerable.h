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

#include "velox/experimental/stateful/TimerHeapInternalTimer.h"

#include <memory>
#include <mutex>

namespace facebook::velox::stateful {

// This class is relevant to Flink Triggerable.
template <typename K, typename N>
class Triggerable {
 public:
  Triggerable() {
    mtx_ = std::make_shared<std::mutex>();
  }

  virtual void onEventTime(
      std::shared_ptr<TimerHeapInternalTimer<K, N>> timer) = 0;

  virtual void onProcessingTime(
      std::shared_ptr<TimerHeapInternalTimer<K, N>> timer) {}

  // For Gluten/Flink, the processing time is triggered by JNI.
  virtual void processProcessingTimeByJni(int64_t timestamp) {}

  const std::shared_ptr<std::mutex> getMutex() const {
    return mtx_;
  }

 protected:
  std::shared_ptr<std::mutex> mtx_;
};

} // namespace facebook::velox::stateful
