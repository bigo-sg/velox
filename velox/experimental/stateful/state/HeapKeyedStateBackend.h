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

#include "velox/common/serialization/Serializable.h"
#include "velox/experimental/stateful/state/KeyedStateBackend.h"

namespace facebook::velox::stateful {

// This class is relevent to flink HeapKeyedStateBackend.
class HeapKeyedStateBackend: public KeyedStateBackend {
 public:
  std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
      getOrCreateMapState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<ListState<uint32_t, long, RowVectorPtr>>
      getOrCreateListState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<ValueState<uint32_t, long, RowVectorPtr>>
      getOrCreateValueState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<InternalTimerService<uint32_t, long>> 
      createTimerService(Triggerable* triggerable) override;

 private:
  std::map<std::string, StatePtr> keyValueStatesByName_;
};

} // namespace facebook::velox::stateful
