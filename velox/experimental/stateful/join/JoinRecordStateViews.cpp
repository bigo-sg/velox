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
#include "velox/experimental/stateful/join/JoinRecordStateViews.h"

namespace facebook::velox::stateful {

// static
JoinRecordStateViewPtr JoinRecordStateViews::create(
    StreamOperatorStateHandler* stateHandler,
    std::string stateName,
    //JoinInputSideSpec inputSideSpec,
    //InternalTypeInfo<RowData> recordType,
    long retentionTime) {
  StateTtlConfig ttlConfig(retentionTime);
  return std::make_unique<InputSideHasNoUniqueKey>(stateHandler, stateName, ttlConfig);
}

InputSideHasNoUniqueKey::InputSideHasNoUniqueKey(
  StreamOperatorStateHandler* stateHandler,
    std::string& stateName,
    StateTtlConfig ttlConfig) {
  StateDescriptor recordStateDesc(stateName);
  /**
  if (ttlConfig.isEnabled()) {
    recordStateDesc.enableTimeToLive(ttlConfig);
  }
  */
  recordState_ = stateHandler->getMapState(recordStateDesc);
}

void InputSideHasNoUniqueKey::addRecord(uint32_t key, RowVectorPtr record) {
  int cnt = recordState_->get(key, ns_, record);
  if (cnt != -1) {
      cnt += 1;
  } else {
      cnt = 1;
  }
  recordState_->put(key, ns_, std::move(record), cnt);
}

std::map<RowVectorPtr, int> InputSideHasNoUniqueKey::records(uint32_t key) {
  return recordState_->entries(key, ns_);
}

void InputSideHasNoUniqueKey::close() {
  recordState_->clear();
  recordState_.reset();
}

} // namespace facebook::velox::stateful
