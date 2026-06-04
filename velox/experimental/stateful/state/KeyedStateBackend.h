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

#include "velox/experimental/stateful/InternalTimerService.h"
#include "velox/experimental/stateful/state/CheckpointListener.h"
#include "velox/experimental/stateful/state/Snapshotable.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/vector/ComplexVector.h"
#include "velox/common/memory/MemoryPool.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink
// org.apache.flink.runtime.state.KeyedStateBackend.
class KeyedStateBackend : public Snapshotable, public CheckpointListener {
 public:
  virtual std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
  getOrCreateMapState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>>
  getOrCreateListState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<ValueState<int64_t, int64_t, RowVectorPtr>>
  getOrCreateValueState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
  getOrCreateGroupValueState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
  getOrCreateGroupMapState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
  getOrCreateRankMapState(StateDescriptor& stateDescriptor) = 0;

  // TODO: Flink create PriorityQueue.
  virtual std::shared_ptr<InternalTimerService<int64_t, int64_t>>
  createTimerService(Triggerable<int64_t, int64_t>* triggerable) = 0;

  virtual std::shared_ptr<InternalTimerService<int64_t, TimeWindow>>
      createGroupWindowAggTimerService(Triggerable<int64_t, TimeWindow>* triggerable) = 0;

  void setCurrentKey(const uint32_t key) {
    currentKey_ = key;
  }

  const uint32_t getCurrentKey() {
    return currentKey_;
  }

 private:
  uint32_t currentKey_{};
  velox::memory::MemoryPool* memoryPool;
};

using KeyedStateBackendPtr = std::shared_ptr<KeyedStateBackend>;

} // namespace facebook::velox::stateful
