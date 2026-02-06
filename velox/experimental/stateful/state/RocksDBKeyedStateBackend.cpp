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

#include "velox/experimental/stateful/state/RocksDBKeyedStateBackend.h"
#include <memory>
#include "velox/common/memory/MemoryPool.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/RocksDBState.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

RocksDBKeyedStateBackend::RocksDBKeyedStateBackend(
    rocksdb::DB* db,
    const rocksdb::ReadOptions* readOptions,
    const rocksdb::WriteOptions* writeOptions,
    const std::set<std::string>& states,
    const std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*>&
        stateColumnFamilies,
    const std::unordered_map<std::string, std::string>& stateOperators,
    const std::unordered_map<std::string, TypePtr>& stateKeys,
    const std::unordered_map<std::string, TypePtr>& stateNamespaces,
    const std::unordered_map<std::string, TypePtr>& stateValues)
    : KeyedStateBackend(),
      db_(db),
      readOptions_(readOptions),
      writeOptions_(writeOptions),
      states_(states),
      stateKeys_(stateKeys),
      stateNamespaces_(stateNamespaces),
      stateValues_(stateValues),
      stateOperators_(stateOperators),
      stateColumnFamilies_(stateColumnFamilies) {
  VELOX_CHECK(db_ != nullptr, "rocksdb can not be null");
  VELOX_CHECK(readOptions_ != nullptr, "rocksdb read options can not be null");
  VELOX_CHECK(
      writeOptions_ != nullptr, "rocksdb write options can not be null");
}

void RocksDBKeyedStateBackend::checkValidState(const std::string& stateName) {
  VELOX_CHECK(
      states_.count(stateName),
      "The rocksdb state {} is not registered",
      stateName);
  VELOX_CHECK(
      stateColumnFamilies_.count(stateName),
      "No column family related to rocksdb state {}",
      stateName);
  VELOX_CHECK(
      stateKeys_.count(stateName),
      "No state key related to rocksdb state {}",
      stateName);
  VELOX_CHECK(
      stateNamespaces_.count(stateName),
      "No state namespace related to rocksdb state {}",
      stateName);
  VELOX_CHECK(
      stateValues_.count(stateName),
      "No state value related to rocksdb state {}",
      stateName);
}

RowTypePtr combineToRowType(const TypePtr& keyType, const TypePtr& valueType) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  if (keyType->kind() == TypeKind::ROW) {
    auto keyRowType = std::dynamic_pointer_cast<const RowType>(keyType);
    names.reserve(keyRowType->size());
    types.reserve(keyRowType->size());
    for (size_t i = 0; i < keyRowType->size(); ++i) {
      names.push_back(keyRowType->nameOf(i));
      types.push_back(keyRowType->childAt(i));
    }
  } else {
    names.push_back("key");
    types.push_back(keyType);
  }

  if (valueType->kind() == TypeKind::ROW) {
    auto valueRowType = std::dynamic_pointer_cast<const RowType>(valueType);
    names.reserve(valueRowType->size() + names.size());
    types.reserve(valueRowType->size() + types.size());
    for (size_t i = 0; i < valueRowType->size(); ++i) {
      names.push_back(valueRowType->nameOf(i));
      types.push_back(valueRowType->childAt(i));
    }
  } else {
    names.push_back("value");
    types.push_back(valueType);
  }
  return std::make_shared<RowType>(std::move(names), std::move(types));
}

std::shared_ptr<ValueState<uint32_t, int64_t, RowVectorPtr>>
RocksDBKeyedStateBackend::getOrCreateValueState(
    StateDescriptor& stateDescriptor) {
  const std::string stateName = stateDescriptor.name();
  checkValidState(stateName);
  const std::string operatorId = stateDescriptor.operatorId();
  if (!operatorId.empty() && stateOperators_[stateName] != operatorId) {
    VELOX_FAIL(
        "The rocksdb state {} is not matched with the operatorId: {}",
        stateName,
        operatorId);
  }
  memory::MemoryPool* pool = stateDescriptor.memoryPool();
  std::shared_ptr<ValueSerializer<uint32_t>> keySerializer =
      std::dynamic_pointer_cast<ValueSerializer<uint32_t>>(createSerializer(
          std::make_shared<ScalarType<TypeKind::INTEGER>>(), true, pool));
  std::shared_ptr<ValueSerializer<int64_t>> namespaceSerializer =
      std::dynamic_pointer_cast<ValueSerializer<int64_t>>(
          createSerializer(stateNamespaces_[stateName], false, pool));
  const TypePtr& keyType = stateKeys_[stateName];
  const TypePtr& valueType = stateValues_[stateName];
  std::shared_ptr<ComplexVectorSerializer<RowVectorPtr>> valueSerializer =
      std::dynamic_pointer_cast<ComplexVectorSerializer<RowVectorPtr>>(
          createSerializer(combineToRowType(keyType, valueType), false, pool));
  return std::make_shared<RocksDBValueState<uint32_t, int64_t, RowVectorPtr>>(
      *db_,
      *readOptions_,
      *writeOptions_,
      *stateColumnFamilies_[stateName],
      keySerializer,
      namespaceSerializer,
      valueSerializer,
      nullptr,
      pool);
}

std::shared_ptr<ListState<uint32_t, long, RowVectorPtr>>
RocksDBKeyedStateBackend::getOrCreateListState(
    StateDescriptor& stateDescriptor) {
  // TODO: implement this
  return nullptr;
}

std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
RocksDBKeyedStateBackend::getOrCreateMapState(
    StateDescriptor& stateDescriptor) {
  // TODO: implement this
  return nullptr;
}

std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
RocksDBKeyedStateBackend::getOrCreateGroupValueState(
    StateDescriptor& stateDescriptor) {
  // TODO: implement this
  return nullptr;
}

std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
RocksDBKeyedStateBackend::getOrCreateGroupMapState(
    StateDescriptor& stateDescriptor) {
  // TODO: implement this
  return nullptr;
}

std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
RocksDBKeyedStateBackend::getOrCreateRankMapState(
    StateDescriptor& stateDescriptor) {
  // TODO: implement this
  return nullptr;
}

std::shared_ptr<InternalTimerService<uint32_t, long>>
RocksDBKeyedStateBackend::createTimerService(
    Triggerable<uint32_t, long>* triggerable) {
  return std::make_shared<InternalTimerService<uint32_t, long>>(triggerable);
}

std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>>
RocksDBKeyedStateBackend::createGroupWindowAggTimerService(
    Triggerable<uint32_t, TimeWindow>* triggerable) {
  // TODO: implement this
  return nullptr;
}

void RocksDBKeyedStateBackend::snapshot(
    long checkpointId,
    long timestamp,
    CheckpointOptions checkpointOptions) {}

void RocksDBKeyedStateBackend::notifyCheckpointComplete(long checkpointId) {}

void RocksDBKeyedStateBackend::notifyCheckpointAborted(long checkpointId) {}

} // namespace facebook::velox::stateful
