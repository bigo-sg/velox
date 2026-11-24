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

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "velox/experimental/stateful/state/KeyedStateBackend.h"
#include "velox/experimental/stateful/state/KeyTable.h"
#include "velox/experimental/stateful/state/RocksDBStateBackend.h"
#include "velox/experimental/stateful/state/RocksDBState.h"

namespace facebook::velox::stateful {

template <typename K>
class RocksDBKeyedStateBackend : public KeyedStateBackend {
 public:
  RocksDBKeyedStateBackend(
      std::vector<TypePtr> keyTypes,
      const RocksDBKeyedStateBackendParameters& parameters,
      memory::MemoryPool* pool)
      : keyTable_(
            std::make_unique<KeyTable>(
                std::move(keyTypes),
                parameters.getMaxParallelism(),
                pool)),
        keySerializer_(keyTable_->keySerializer()),
        db_(*parameters.getDB()),
        readOptions_(*parameters.getReadOptions()),
        writeOptions_(*parameters.getWriteOptions()),
        columnFamilies_(parameters.getColumnFamilies()),
        maxParallelism_(parameters.getMaxParallelism()),
        startKeyGroup_(parameters.getStartKeyGroup()),
        numKeyGroups_(parameters.getNumKeyGroups()) {
    VELOX_CHECK_GT(numKeyGroups_, 0);
    VELOX_CHECK(
        static_cast<uint64_t>(startKeyGroup_) + numKeyGroups_ <=
            maxParallelism_,
        "Invalid RocksDB key-group range");
  }

  KeyIndex& keyIndex() {
    return *keyTable_;
  }
  uint32_t maxParallelism() const {
    return maxParallelism_;
  }
  uint32_t startKeyGroup() const {
    return startKeyGroup_;
  }
  uint32_t numKeyGroups() const {
    return numKeyGroups_;
  }

  void advance() {
    for (const auto& flush : flushers_) {
      flush();
    }
    keyTable_->advance();
  }

  // The non-template state factories predate the typed state API and cannot
  // represent its key and namespace types.
  std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
  getOrCreateMapState(StateDescriptor& /*descriptor*/) override {
    VELOX_NYI("Use the typed RocksDB map state API");
  }

  std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>>
  getOrCreateListState(StateDescriptor& /*descriptor*/) override {
    VELOX_NYI("Use the typed RocksDB list state API");
  }

  std::shared_ptr<ValueState<int64_t, int64_t, RowVectorPtr>>
  getOrCreateValueState(StateDescriptor& /*descriptor*/) override {
    VELOX_NYI("Use the typed RocksDB value state API");
  }

  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
  getOrCreateGroupValueState(StateDescriptor& /*descriptor*/) override {
    VELOX_NYI("Use the typed RocksDB value state API");
  }

  std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
  getOrCreateGroupMapState(StateDescriptor& /*descriptor*/) override {
    VELOX_NYI("Use the typed RocksDB map state API");
  }

  std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
  getOrCreateRankMapState(StateDescriptor& /*descriptor*/) override {
    VELOX_NYI("Use the typed RocksDB map state API");
  }

  std::shared_ptr<InternalTimerService<int64_t, int64_t>> createTimerService(
      Triggerable<int64_t, int64_t>* /*triggerable*/) override {
    VELOX_NYI("RocksDB timer service is not implemented");
  }

  std::shared_ptr<InternalTimerService<int64_t, TimeWindow>>
  createGroupWindowAggTimerService(
      Triggerable<int64_t, TimeWindow>* /*triggerable*/) override {
    VELOX_NYI("RocksDB group window timer service is not implemented");
  }

  void snapshot(
      int64_t /*checkpointId*/,
      int64_t /*timestamp*/,
      CheckpointOptions /*checkpointOptions*/) override {
    VELOX_NYI("Use the typed RocksDB snapshot API");
  }

  void notifyCheckpointComplete(int64_t /*checkpointId*/) override {}

  void notifyCheckpointAborted(int64_t /*checkpointId*/) override {}

  template <typename N>
  std::shared_ptr<AggregatingState<K, N>> getOrCreateAggregatingState(
      const AggregatingStateDescriptor& descriptor) {
    if (auto state = findState<AggregatingState<K, N>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<RocksDBAggregatingState<K, N>>(
        db_,
        columnFamily(descriptor.name()),
        readOptions_,
        writeOptions_,
        descriptor,
        keySerializer_,
        keyTable_->keyLifecycle(),
        startKeyGroup_,
        numKeyGroups_);
    createdStates_.emplace(descriptor.name(), state);
    flushers_.push_back([state]() { state->flush(); });
    return state;
  }

  template <typename N, typename V>
  std::shared_ptr<ValueState<K, N, V>> getOrCreateValueState(
      const ValueStateDescriptor<V>& descriptor) {
    if (auto state = findState<ValueState<K, N, V>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<RocksDBValueState<K, N, V>>(
        db_,
        columnFamily(descriptor.name()),
        readOptions_,
        writeOptions_,
        descriptor,
        keySerializer_,
        startKeyGroup_,
        numKeyGroups_);
    createdStates_.emplace(descriptor.name(), state);
    return state;
  }

  template <typename N, typename T>
  std::shared_ptr<ListState<K, N, T>> getOrCreateListState(
      const ListStateDescriptor<T>& descriptor) {
    if (auto state = findState<ListState<K, N, T>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<RocksDBListState<K, N, T>>(
        db_,
        columnFamily(descriptor.name()),
        readOptions_,
        writeOptions_,
        descriptor,
        keySerializer_,
        startKeyGroup_,
        numKeyGroups_);
    createdStates_.emplace(descriptor.name(), state);
    return state;
  }

  template <typename N, typename UK, typename UV>
  std::shared_ptr<MapState<K, N, UK, UV>> getOrCreateMapState(
      const MapStateDescriptor<UK, UV>& descriptor) {
    if (auto state = findState<MapState<K, N, UK, UV>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<RocksDBMapState<K, N, UK, UV>>(
        db_,
        columnFamily(descriptor.name()),
        readOptions_,
        writeOptions_,
        descriptor,
        keySerializer_,
        startKeyGroup_,
        numKeyGroups_);
    createdStates_.emplace(descriptor.name(), state);
    return state;
  }

  std::string snapshot() {
    advance();
    const auto states = sortedStates();
    std::string bytes;
    CheckpointWriter writer(bytes);
    CheckpointHeader header;
    header.flags = kCheckpointFlagLittleEndian | kCheckpointFlagCrcTrailer;
    header.maxParallelism = maxParallelism_;
    header.startKeyGroup = startKeyGroup_;
    header.numKeyGroups = numKeyGroups_;
    writeCheckpointHeader(writer, header);
    writer.writeBytes(keySerializer_->schemaId());
    writer.writeUInt32(static_cast<uint32_t>(states.size()));
    for (const auto& [name, state] : states) {
      writer.writeBytes(name);
      writer.writeBytes(state->namespaceSchemaId());
      writer.writeBytes(state->valueSchemaId());
    }
    for (uint32_t group = startKeyGroup_;
         group < startKeyGroup_ + numKeyGroups_;
         ++group) {
      writer.writeUInt32(group);
      for (const auto& [name, state] : states) {
        writer.writeBytes(name);
        state->snapshotKeyGroup(static_cast<int32_t>(group), writer);
      }
    }
    writer.writeUInt32(checkpointCrc32(bytes));
    return bytes;
  }

  void restore(const std::string& bytes) {
    CheckpointReader headerReader(bytes.data(), bytes.size());
    const auto header = readCheckpointHeader(headerReader);
    size_t bodySize = bytes.size();
    if ((header.flags & kCheckpointFlagCrcTrailer) != 0) {
      VELOX_CHECK_GE(bytes.size(), kCheckpointHeaderSize + sizeof(uint32_t));
      bodySize -= sizeof(uint32_t);
      VELOX_CHECK_EQ(
          littleEndianValue<uint32_t>(
              std::string_view(bytes.data() + bodySize, sizeof(uint32_t))),
          checkpointCrc32(std::string_view(bytes.data(), bodySize)),
          "Corrupt checkpoint: CRC-32 mismatch");
    }
    CheckpointReader reader(bytes.data(), bodySize);
    readCheckpointHeader(reader);
    VELOX_CHECK_EQ(header.maxParallelism, maxParallelism_);
    VELOX_CHECK_GT(header.numKeyGroups, 0);
    VELOX_CHECK(
        static_cast<uint64_t>(header.startKeyGroup) + header.numKeyGroups <=
            header.maxParallelism,
        "Corrupt checkpoint key-group range");
    VELOX_CHECK_EQ(reader.readBytes(), keySerializer_->schemaId());
    const auto stateCount = reader.readUInt32();
    VELOX_CHECK_EQ(
        stateCount,
        createdStates_.size(),
        "Checkpoint state registry differs from the backend");
    std::vector<std::string> names;
    std::vector<StatePtr> states;
    for (uint32_t i = 0; i < stateCount; ++i) {
      const std::string name(reader.readBytes());
      const auto nsSchema = reader.readBytes();
      const auto valueSchema = reader.readBytes();
      auto it = createdStates_.find(name);
      VELOX_CHECK(
          it != createdStates_.end(),
          "Checkpoint state '{}' is not registered",
          name);
      VELOX_CHECK_EQ(it->second->namespaceSchemaId(), nsSchema);
      VELOX_CHECK_EQ(it->second->valueSchemaId(), valueSchema);
      names.push_back(name);
      states.push_back(it->second);
    }
    const uint64_t streamEnd =
        static_cast<uint64_t>(header.startKeyGroup) + header.numKeyGroups;
    const uint64_t myEnd =
        static_cast<uint64_t>(startKeyGroup_) + numKeyGroups_;
    VELOX_CHECK(
        header.startKeyGroup < myEnd && streamEnd > startKeyGroup_,
        "Checkpoint key-group range does not overlap this backend");
    for (uint64_t group = header.startKeyGroup; group < streamEnd; ++group) {
      VELOX_CHECK_EQ(
          reader.readUInt32(),
          group,
          "Corrupt checkpoint: key groups are not ascending");
      const bool mine = group >= startKeyGroup_ && group < myEnd;
      for (uint32_t id = 0; id < stateCount; ++id) {
        VELOX_CHECK_EQ(reader.readBytes(), names[id]);
        const auto count = reader.readInt32();
        VELOX_CHECK_GE(count, 0, "Corrupt checkpoint entry count");
        for (int32_t i = 0; i < count; ++i) {
          if (mine) {
            states[id]->restoreEntry(static_cast<int32_t>(group), reader);
          } else {
            readEntryFrames(reader);
          }
        }
      }
    }
    VELOX_CHECK(reader.atEnd(), "Corrupt checkpoint: trailing bytes");
  }

 private:
  rocksdb::ColumnFamilyHandle& columnFamily(const std::string& name) {
    auto it = columnFamilies_.find(name);
    VELOX_CHECK(
        it != columnFamilies_.end() && it->second != nullptr,
        "RocksDB state '{}' has no column family",
        name);
    return *it->second;
  }

  template <typename S>
  std::shared_ptr<S> findState(const std::string& name) const {
    auto it = createdStates_.find(name);
    if (it == createdStates_.end()) {
      return nullptr;
    }
    auto state = std::dynamic_pointer_cast<S>(it->second);
    VELOX_CHECK_NOT_NULL(
        state, "State '{}' was registered with another type", name);
    return state;
  }

  std::vector<std::pair<std::string, StatePtr>> sortedStates() const {
    std::vector<std::pair<std::string, StatePtr>> states(
        createdStates_.begin(), createdStates_.end());
    std::sort(states.begin(), states.end());
    return states;
  }

  std::unique_ptr<KeyTable> keyTable_;
  std::shared_ptr<TypeSerializer<K>> keySerializer_;
  rocksdb::DB& db_;
  const rocksdb::ReadOptions& readOptions_;
  const rocksdb::WriteOptions& writeOptions_;
  std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> columnFamilies_;
  const uint32_t maxParallelism_;
  const uint32_t startKeyGroup_;
  const uint32_t numKeyGroups_;
  std::unordered_map<std::string, StatePtr> createdStates_;
  std::vector<std::function<void()>> flushers_;
};

} // namespace facebook::velox::stateful
