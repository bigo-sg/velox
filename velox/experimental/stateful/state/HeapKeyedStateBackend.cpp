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
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include <cstdint>
#include "velox/experimental/stateful/state/HeapListState.h"
#include "velox/experimental/stateful/state/HeapMapState.h"
#include "velox/experimental/stateful/state/HeapValueState.h"

namespace facebook::velox::stateful {

std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
HeapKeyedStateBackend::getOrCreateMapState(StateDescriptor& stateDescriptor) {
  auto stateIte = keyValueStatesByName_.find(stateDescriptor.name());
  if (stateIte != keyValueStatesByName_.end()) {
    return std::dynamic_pointer_cast<
        MapState<uint32_t, int, RowVectorPtr, int>>(stateIte->second);
  }
  auto state = std::make_shared<HeapMapState<uint32_t, int, RowVectorPtr, int>>(
      stateDescriptor.keyGroupNumber());
  keyValueStatesByName_.insert({stateDescriptor.name(), state});
  return state;
}

std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>>
HeapKeyedStateBackend::getOrCreateListState(StateDescriptor& stateDescriptor) {
  auto stateIte = keyValueStatesByName_.find(stateDescriptor.name());
  if (stateIte != keyValueStatesByName_.end()) {
    return std::dynamic_pointer_cast<
        ListState<uint32_t, int64_t, RowVectorPtr>>(stateIte->second);
  }
  auto state = std::make_shared<HeapListState<uint32_t, int64_t, RowVectorPtr>>(
      stateDescriptor.keyGroupNumber());
  keyValueStatesByName_.insert({stateDescriptor.name(), state});
  return state;
}

std::shared_ptr<ValueState<uint32_t, int64_t, RowVectorPtr>>
HeapKeyedStateBackend::getOrCreateValueState(StateDescriptor& stateDescriptor) {
  auto stateIte = keyValueStatesByName_.find(stateDescriptor.name());
  if (stateIte != keyValueStatesByName_.end()) {
    return std::dynamic_pointer_cast<
        ValueState<uint32_t, int64_t, RowVectorPtr>>(stateIte->second);
  }
  auto state =
      std::make_shared<HeapValueState<uint32_t, int64_t, RowVectorPtr>>(
          stateDescriptor.keyGroupNumber());
  keyValueStatesByName_.insert({stateDescriptor.name(), state});
  return state;
}

std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
HeapKeyedStateBackend::getOrCreateGroupValueState(
    StateDescriptor& stateDescriptor) {
  auto stateIte = keyValueStatesByName_.find(stateDescriptor.name());
  if (stateIte != keyValueStatesByName_.end()) {
    return std::dynamic_pointer_cast<
        ValueState<uint32_t, TimeWindow, RowVectorPtr>>(stateIte->second);
  }
  auto state =
      std::make_shared<HeapValueState<uint32_t, TimeWindow, RowVectorPtr>>(
          stateDescriptor.keyGroupNumber());
  keyValueStatesByName_.insert({stateDescriptor.name(), state});
  return state;
}

std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
HeapKeyedStateBackend::getOrCreateGroupMapState(
    StateDescriptor& stateDescriptor) {
  auto stateIte = keyValueStatesByName_.find(stateDescriptor.name());
  if (stateIte != keyValueStatesByName_.end()) {
    return std::dynamic_pointer_cast<
        MapState<uint32_t, int, TimeWindow, TimeWindow>>(stateIte->second);
  }
  auto state =
      std::make_shared<HeapMapState<uint32_t, int, TimeWindow, TimeWindow>>(
          stateDescriptor.keyGroupNumber());
  keyValueStatesByName_.insert({stateDescriptor.name(), state});
  return state;
}

std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
HeapKeyedStateBackend::getOrCreateRankMapState(
    StateDescriptor& stateDescriptor) {
  auto stateIte = keyValueStatesByName_.find(stateDescriptor.name());
  if (stateIte != keyValueStatesByName_.end()) {
    return std::dynamic_pointer_cast<
        MapState<uint32_t, int, uint32_t, RowVectorPtr>>(stateIte->second);
  }
  auto state =
      std::make_shared<HeapMapState<uint32_t, int, uint32_t, RowVectorPtr>>(
          stateDescriptor.keyGroupNumber());
  keyValueStatesByName_.insert({stateDescriptor.name(), state});
  return state;
}

std::shared_ptr<InternalTimerService<int64_t, int64_t>>
HeapKeyedStateBackend::createTimerService(
    Triggerable<int64_t, int64_t>* triggerable) {
  return std::make_shared<InternalTimerService<int64_t, int64_t>>(triggerable);
}

std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>>
HeapKeyedStateBackend::createGroupWindowAggTimerService(
    Triggerable<uint32_t, TimeWindow>* triggerable) {
  return std::make_shared<InternalTimerService<uint32_t, TimeWindow>>(
      triggerable);
}

void HeapKeyedStateBackend::snapshot(
    int64_t checkpointId,
    int64_t timestamp,
    CheckpointOptions checkpointOptions) {
  // TODO: implement snapshot logic.
}

void HeapKeyedStateBackend::notifyCheckpointComplete(int64_t checkpointId) {
  // TODO: implement checkpoint complete logic.
}

void HeapKeyedStateBackend::notifyCheckpointAborted(int64_t checkpointId) {
  // TODO: implement checkpoint abort logic.
}
} // namespace facebook::velox::stateful
