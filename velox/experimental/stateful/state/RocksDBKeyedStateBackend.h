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

#include <set>
#include <unordered_map>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "velox/experimental/stateful/state/KeyedStateBackend.h"

namespace facebook::velox::stateful {

class RocksDBKeyedStateBackend : public KeyedStateBackend {
 public:
  RocksDBKeyedStateBackend(
      rocksdb::DB* db,
      const rocksdb::ReadOptions* readOptions,
      const rocksdb::WriteOptions* writeOptions,
      const std::set<std::string>& states,
      const std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*>&
          stateColumnFamilies,
      const std::unordered_map<std::string, std::string>& stateOperators,
      const std::unordered_map<std::string, TypePtr>& stateKeys,
      const std::unordered_map<std::string, TypePtr>& stateNamespaces,
      const std::unordered_map<std::string, TypePtr>& stateValues);

  void checkValidState(const std::string& stateName);

  std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
  getOrCreateMapState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>>
  getOrCreateListState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<ValueState<uint32_t, int64_t, RowVectorPtr>>
  getOrCreateValueState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
  getOrCreateGroupValueState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
  getOrCreateGroupMapState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
  getOrCreateRankMapState(StateDescriptor& stateDescriptor) override;

  std::shared_ptr<InternalTimerService<uint32_t, int64_t>> createTimerService(
      Triggerable<uint32_t, int64_t>* triggerable) override;

  std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>>
  createGroupWindowAggTimerService(
      Triggerable<uint32_t, TimeWindow>* triggerable) override;

  // Deprecated, this maybe removed later
  void snapshot(
      int64_t checkpointId,
      int64_t timestamp,
      CheckpointOptions checkpointOptions) override;

  // Deprecated, this maybe removed later
  void notifyCheckpointComplete(int64_t checkpointId) override;

  // Deprecated, this maybe removed later
  void notifyCheckpointAborted(int64_t checkpointId) override;

 private:
  rocksdb::DB* db_;
  const rocksdb::ReadOptions* readOptions_;
  const rocksdb::WriteOptions* writeOptions_;
  std::set<std::string> states_;
  std::unordered_map<std::string, TypePtr> stateKeys_;
  std::unordered_map<std::string, TypePtr> stateNamespaces_;
  std::unordered_map<std::string, TypePtr> stateValues_;
  std::unordered_map<std::string, std::string> stateOperators_;
  std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*>
      stateColumnFamilies_;
};

} // namespace facebook::velox::stateful
