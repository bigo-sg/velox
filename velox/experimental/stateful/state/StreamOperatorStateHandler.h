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

/** 
 * This class is relevent to flink 
 * org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.
 */
class StreamOperatorStateHandler {
 public:
  StreamOperatorStateHandler(int operatorId, KeyedStateBackendPtr keyedStateBackend)
      : operatorId_(operatorId),
        keyedStateBackend_(std::move(keyedStateBackend)) {}

  KeyedStateBackend getKeyedStateBackend() const;

  State getOrCreateKeyedState() const;

  void snapshotState() {
    keyedStateBackend_->snapshot(
        operatorId_, 0, CheckpointOptions::defaultOptions());
  }

  void notifyCheckpointComplete(long checkpointId) {
    keyedStateBackend_->notifyCheckpointComplete(checkpointId);
  }

  void notifyCheckpointAborted(long checkpointId) {
    keyedStateBackend_->notifyCheckpointAborted(checkpointId);
  }

  // The type of state has to be specified as c++ not support template well.
  std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>> getMapState(
      StateDescriptor& stateDescriptor) {
    return keyedStateBackend_->getOrCreateMapState(stateDescriptor);
  }

  std::shared_ptr<ListState<uint32_t, long, RowVectorPtr>> getListState(
      StateDescriptor& stateDescriptor) {
    return keyedStateBackend_->getOrCreateListState(stateDescriptor);
  }

  std::shared_ptr<ValueState<uint32_t, long, RowVectorPtr>> getValueState(
      StateDescriptor& stateDescriptor) {
    return keyedStateBackend_->getOrCreateValueState(stateDescriptor);
  }

  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>> getGroupValueState(
      StateDescriptor& stateDescriptor) {
    return keyedStateBackend_->getOrCreateGroupValueState(stateDescriptor);
  }

  std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>> getGroupMapState(
      StateDescriptor& stateDescriptor) {
    return keyedStateBackend_->getOrCreateGroupMapState(stateDescriptor);
  }

  std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>> getRankMapState(
      StateDescriptor& stateDescriptor) {
    return keyedStateBackend_->getOrCreateRankMapState(stateDescriptor);
  }

  std::shared_ptr<InternalTimerService<uint32_t, long>> createTimerService(
      Triggerable<uint32_t, long>* triggerable) {
    return keyedStateBackend_->createTimerService(triggerable);
  }

  // TODO: should make it using template
  std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>> createGroupWindowAggTimerService(
    Triggerable<uint32_t, TimeWindow>* triggerable) {
  return keyedStateBackend_->createGroupWindowAggTimerService(triggerable);
}

 private:
  int operatorId_;
  KeyedStateBackendPtr keyedStateBackend_;
};

using StreamOperatorStateHandlerPtr =
    std::shared_ptr<StreamOperatorStateHandler>;

} // namespace facebook::velox::stateful
