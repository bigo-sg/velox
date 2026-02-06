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

#include "velox/experimental/stateful/state/KeyedStateBackend.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink HeapKeyedStateBackend.
class HeapKeyedStateBackend : public KeyedStateBackend {
 public:
  // TODO: use template to support different key type.
  std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
  getOrCreateMapState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<ListState<uint32_t, long, RowVectorPtr>> getOrCreateListState(
      StateDescriptor& stateDescriptor) override;

  std::shared_ptr<ValueState<uint32_t, long, RowVectorPtr>>
  getOrCreateValueState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<InternalTimerService<uint32_t, long>> createTimerService(
      Triggerable<uint32_t, long>* triggerable) override;

  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
  getOrCreateGroupValueState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
  getOrCreateGroupMapState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
  getOrCreateRankMapState(StateDescriptor& stateDescriptor) override;

  virtual std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>>
  createGroupWindowAggTimerService(
      Triggerable<uint32_t, TimeWindow>* triggerable) override;

  void snapshot(
      long checkpointId,
      long timestamp,
      CheckpointOptions checkpointOptions) override;

  void notifyCheckpointComplete(long checkpointId) override;

  void notifyCheckpointAborted(long checkpointId) override;

 private:
  std::map<std::string, StatePtr> keyValueStatesByName_;
};

} // namespace facebook::velox::stateful
