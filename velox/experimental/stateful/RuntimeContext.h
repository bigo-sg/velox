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

// This class is relevent to flink org.apache.flink.api.common.functions.RuntimeContext.
class RuntimeContext {
 public:
  RuntimeContext(int operatorId, KeyedStateBackendPtr keyedStateBackend) 
      : operatorId_(operatorId),
        keyedStateBackend_(std::move(keyedStateBackend)) {};

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

  std::shared_ptr<InternalTimerService<uint32_t, long>> createTimerService(
      Triggerable* triggerable) {
    return keyedStateBackend_->createTimerService(triggerable);
  }

 private:
  int operatorId_;
  KeyedStateBackendPtr keyedStateBackend_;
};

using RuntimeContextPtr = std::unique_ptr<RuntimeContext>;

} // namespace facebook::velox::stateful
