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

#include "velox/experimental/stateful/join/JoinRecordStateView.h"
#include "velox/experimental/stateful/state/StateTtlConfig.h"
#include "velox/experimental/stateful/state/StreamOperatorStateHandler.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink JoinRecordStateViews.
class JoinRecordStateViews {
 public:
  static JoinRecordStateViewPtr create(
      StreamOperatorStateHandler* stateHandler,
      std::string stateName,
      // JoinInputSideSpec inputSideSpec,
      // InternalTypeInfo<RowData> recordType,
      long retentionTime);
};

// This class is relevant to Flink InputSideHasNoUniqueKey.
class InputSideHasNoUniqueKey : public JoinRecordStateView {
 public:
  InputSideHasNoUniqueKey(
      StreamOperatorStateHandler* stateHandler,
      std::string& stateName,
      StateTtlConfig ttlConfig);

  void addRecord(uint32_t key, RowVectorPtr record) override;

  std::map<RowVectorPtr, int> records(uint32_t key) override;

  void close() override;

 private:
  std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>> recordState_;
  const int ns_ = 0; // For join, namespace is all same, just use 0.
};

} // namespace facebook::velox::stateful
