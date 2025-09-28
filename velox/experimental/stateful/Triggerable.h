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
#include "velox/vector/ComplexVector.h"

#include <memory>

namespace facebook::velox::stateful {

// This class is relevent to flink Triggerable.
template<typename K, typename N>
class Triggerable {
 public:
  virtual void onEventTime(
      std::shared_ptr<TimerHeapInternalTimer<K, N>> timer) = 0;
  
  virtual void onProcessingTime(
      std::shared_ptr<TimerHeapInternalTimer<K, N>> timer) {}
};

} // namespace facebook::velox::stateful
