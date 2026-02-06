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

namespace facebook::velox::stateful {

// This class is relevent to flink InternalTimerServiceImpl.
template <typename K, typename N>
class TimerHeapInternalTimer {
 public:
  TimerHeapInternalTimer(long timestamp, K key, N ns)
      : timestamp_(timestamp), key_(key), ns_(ns), keyGroupIndex_(0) {}

  long timestamp() {
    return timestamp_;
  }

  K key() {
    return key_;
  }

  N ns() {
    return ns_;
  }

  int keyGroupIndex() {
    return keyGroupIndex_;
  }

  bool operator==(const TimerHeapInternalTimer& other) const {
    return timestamp_ == other.timestamp_ && key_ == other.key_ &&
           ns_ == other.ns_;
  }

 private:
  long timestamp_;
  K key_;
  N ns_;
  int keyGroupIndex_;
};

} // namespace facebook::velox::stateful
