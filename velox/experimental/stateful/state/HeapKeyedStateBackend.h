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
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/CheckpointStream.h"
#include "velox/experimental/stateful/state/HeapAggregatingState.h"
#include "velox/experimental/stateful/state/HeapListState.h"
#include "velox/experimental/stateful/state/HeapMapState.h"
#include "velox/experimental/stateful/state/HeapValueState.h"
#include "velox/experimental/stateful/state/KeyedStateBackend.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateTable.h"

namespace facebook::velox::stateful {

/// Heap-backed keyed state storage, generic on the key type K (a StateKey
/// subclass); the namespace type N is a template parameter of the getOrCreate
/// methods, as in Flink AbstractKeyedStateBackend<K>. Header-only because
/// every member of the generic API is a template (a virtual function cannot
/// be a template, so the generic API is non-virtual; it is called through
/// the concrete backend type held by the operator).
///
/// The backend keeps one by-name registry of state handles (createdStates_,
/// relevant to Flink createdKVStates); registration, snapshot and restore
/// all go through it, while the runtime hot path goes through the typed
/// handle. Each handle owns its state table: Flink's backend-owned table
/// registry exists because Flink restores a checkpoint before the states
/// are created and must hold the tables until then, whereas here the
/// operator registers its states first and restore is dispatched to the
/// registered handles, so the extra registry has no role.
///
/// The interface of the non-template base KeyedStateBackend predates the
/// typed state API: its state factory methods do not fit (K, N) and are
/// overridden as not-implemented until the stateful operators migrate.
template <typename K>
class HeapKeyedStateBackend : public KeyedStateBackend {
 public:
  /// 'keySerializer' serializes keys into checkpoints and cross-checks the
  /// checkpoint header, as in Flink's backend-held key serializer;
  /// 'startKeyGroup' and 'numKeyGroups' describe this backend's key-group
  /// sub-range (all state tables bucket over exactly this range);
  /// 'maxParallelism' defines the key-group space, hash % maxParallelism,
  /// and stays stable across rescale.
  HeapKeyedStateBackend(
      std::shared_ptr<TypeSerializer<K>> keySerializer,
      uint32_t maxParallelism,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups)
      : keySerializer_(std::move(keySerializer)),
        maxParallelism_(maxParallelism),
        startKeyGroup_(startKeyGroup),
        numKeyGroups_(numKeyGroups) {
    VELOX_CHECK(numKeyGroups > 0, "numKeyGroups must be greater than 0");
    VELOX_CHECK(
        static_cast<uint64_t>(startKeyGroup) + numKeyGroups <= maxParallelism,
        "Key-group range [{}, {}) exceeds maxParallelism {}",
        startKeyGroup,
        startKeyGroup + numKeyGroups,
        maxParallelism);
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

  const std::shared_ptr<TypeSerializer<K>>& keySerializer() const {
    return keySerializer_;
  }

  // The pre-generic interface of the base; the stateful operators migrate
  // to the typed API below.
  std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
  getOrCreateMapState(StateDescriptor& /*stateDescriptor*/) override {
    VELOX_NYI("getOrCreateMapState is not available on the heap backend");
  }

  std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>>
  getOrCreateListState(StateDescriptor& /*stateDescriptor*/) override {
    VELOX_NYI("getOrCreateListState is not available on the heap backend");
  }

  std::shared_ptr<ValueState<int64_t, int64_t, RowVectorPtr>>
  getOrCreateValueState(StateDescriptor& /*stateDescriptor*/) override {
    VELOX_NYI("getOrCreateValueState is not available on the heap backend");
  }

  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
  getOrCreateGroupValueState(StateDescriptor& /*stateDescriptor*/) override {
    VELOX_NYI(
        "getOrCreateGroupValueState is not available on the heap backend");
  }

  std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
  getOrCreateGroupMapState(StateDescriptor& /*stateDescriptor*/) override {
    VELOX_NYI("getOrCreateGroupMapState is not available on the heap backend");
  }

  std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
  getOrCreateRankMapState(StateDescriptor& /*stateDescriptor*/) override {
    VELOX_NYI("getOrCreateRankMapState is not available on the heap backend");
  }

  std::shared_ptr<InternalTimerService<int64_t, int64_t>> createTimerService(
      Triggerable<int64_t, int64_t>* /*triggerable*/) override {
    VELOX_NYI("createTimerService is not available on the heap backend");
  }

  std::shared_ptr<InternalTimerService<int64_t, TimeWindow>>
  createGroupWindowAggTimerService(
      Triggerable<int64_t, TimeWindow>* /*triggerable*/) override {
    VELOX_NYI(
        "createGroupWindowAggTimerService is not available on the heap backend");
  }

  // The pre-generic snapshot plane stays a no-op until the stateful
  // operators migrate; the typed plane is snapshot() / restore() below.
  void snapshot(
      int64_t /*checkpointId*/,
      int64_t /*timestamp*/,
      CheckpointOptions /*checkpointOptions*/) override {}

  void notifyCheckpointComplete(int64_t /*checkpointId*/) override {}

  void notifyCheckpointAborted(int64_t /*checkpointId*/) override {}

  // --- Typed state API ---

  /// Returns the AggregatingState handle for the descriptor, creating the state
  /// table and the handle on the first call; a second call with the same
  /// name returns the same handle.
  template <typename N>
  std::shared_ptr<AggregatingState<K, N>> getOrCreateAggregatingState(
      const AggregatingStateDescriptor& descriptor) {
    if (auto state = findState<AggregatingState<K, N>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<HeapAggregatingState<K, N>>(
        std::make_shared<StateTable<K, N, char*>>(
            startKeyGroup_, numKeyGroups_),
        descriptor,
        keySerializer_);
    createdStates_.emplace(descriptor.name(), state);
    return state;
  }

  template <typename N, typename V>
  std::shared_ptr<ValueState<K, N, V>> getOrCreateValueState(
      const ValueStateDescriptor<V>& descriptor) {
    if (auto state = findState<ValueState<K, N, V>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<HeapValueState<K, N, V>>(
        std::make_shared<StateTable<K, N, V>>(startKeyGroup_, numKeyGroups_),
        descriptor,
        keySerializer_);
    createdStates_.emplace(descriptor.name(), state);
    return state;
  }

  template <typename N, typename T>
  std::shared_ptr<ListState<K, N, T>> getOrCreateListState(
      const ListStateDescriptor<T>& descriptor) {
    if (auto state = findState<ListState<K, N, T>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<HeapListState<K, N, T>>(
        std::make_shared<StateTable<K, N, std::shared_ptr<std::vector<T>>>>(
            startKeyGroup_, numKeyGroups_),
        descriptor,
        keySerializer_);
    createdStates_.emplace(descriptor.name(), state);
    return state;
  }

  template <typename N, typename UK, typename UV>
  std::shared_ptr<MapState<K, N, UK, UV>> getOrCreateMapState(
      const MapStateDescriptor<UK, UV>& descriptor) {
    if (auto state = findState<MapState<K, N, UK, UV>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<HeapMapState<K, N, UK, UV>>(
        std::make_shared<StateTable<K, N, std::shared_ptr<std::map<UK, UV>>>>(
            startKeyGroup_, numKeyGroups_),
        descriptor,
        keySerializer_);
    createdStates_.emplace(descriptor.name(), state);
    return state;
  }

  // --- Typed snapshot / restore plane ---

  /// Serializes the entries of all registered states into one checkpoint
  /// stream. Layout: the fixed header (magic, version, flags, key-group
  /// labels), the schema table (the backend's key schema id, then per state
  /// its id with the namespace and value schema ids), the entry body — one
  /// block per key group in ascending order, within a group one block per
  /// state in schema-table order, each headed by the state id and the entry
  /// count — and a CRC-32 trailer over everything before it. States are
  /// visited in id order so the stream is reproducible. The schema table
  /// plays the role of Flink's per-state metadata snapshots: restore
  /// cross-checks it against the backend and its registered states instead
  /// of relying on registration order.
  std::string snapshot() {
    std::vector<std::pair<std::string, StatePtr>> states(
        createdStates_.begin(), createdStates_.end());
    std::sort(states.begin(), states.end());
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
    for (uint32_t keyGroup = startKeyGroup_;
         keyGroup < startKeyGroup_ + numKeyGroups_;
         ++keyGroup) {
      writer.writeUInt32(keyGroup);
      for (const auto& [name, state] : states) {
        writer.writeBytes(name);
        state->snapshotKeyGroup(static_cast<int32_t>(keyGroup), writer);
      }
    }
    writer.writeUInt32(checkpointCrc32(bytes));
    return bytes;
  }

  /// Restores the stream written by snapshot() into the states this backend
  /// has registered. The header must carry this backend's maxParallelism —
  /// it defines the key-group space and is stable across rescale, so
  /// equality is required — and the schema table must match the registered
  /// states one-for-one, same ids and same schema ids. The stream's
  /// key-group range must overlap this backend's; key groups of the stream
  /// outside the range are skipped, so a checkpoint written under a
  /// different parallelism restores into this backend — one covering
  /// stream at once, the disjoint-range pieces of a scale-down in
  /// sequence — and only a stream with no overlap at all is rejected. This
  /// is the reverse of Flink's order (restore before state creation) and
  /// is what makes handle-owned state tables possible; the byte layout of
  /// the entries is the same. When the flags announce a CRC-32 trailer it
  /// is verified over the whole stream before any state is touched.
  void restore(const std::string& bytes) {
    CheckpointReader headerReader(bytes.data(), bytes.size());
    const auto header = readCheckpointHeader(headerReader);
    size_t bodySize = bytes.size();
    if ((header.flags & kCheckpointFlagCrcTrailer) != 0) {
      VELOX_CHECK_GE(
          bytes.size(),
          kCheckpointHeaderSize + sizeof(uint32_t),
          "Corrupt checkpoint: too short for the announced CRC trailer");
      bodySize = bytes.size() - sizeof(uint32_t);
      VELOX_CHECK_EQ(
          littleEndianValue<uint32_t>(
              std::string_view(bytes.data() + bodySize, sizeof(uint32_t))),
          checkpointCrc32(std::string_view(bytes.data(), bodySize)),
          "Corrupt checkpoint: CRC-32 mismatch");
    }
    CheckpointReader reader(bytes.data(), bodySize);
    readCheckpointHeader(reader);
    VELOX_CHECK_EQ(
        header.maxParallelism,
        maxParallelism_,
        "Checkpoint maxParallelism {} does not match this backend's {}; maxParallelism defines the key-group space and must not change",
        header.maxParallelism,
        maxParallelism_);
    VELOX_CHECK_GT(
        header.numKeyGroups,
        0,
        "Corrupt checkpoint: non-positive key-group count");
    VELOX_CHECK(
        static_cast<uint64_t>(header.startKeyGroup) + header.numKeyGroups <=
            header.maxParallelism,
        "Corrupt checkpoint: key-group range [{},{}) exceeds maxParallelism {}",
        header.startKeyGroup,
        header.startKeyGroup + header.numKeyGroups,
        header.maxParallelism);
    const auto keySchemaId = reader.readBytes();
    VELOX_CHECK_EQ(
        keySchemaId,
        keySerializer_->schemaId(),
        "Checkpoint key schema does not match this backend's key serializer");
    const auto stateCount = reader.readUInt32();
    std::vector<std::string> stateIds;
    std::vector<StatePtr> statesById;
    stateIds.reserve(stateCount);
    statesById.reserve(stateCount);
    for (uint32_t id = 0; id < stateCount; ++id) {
      // One bounded copy per state (schema table only): the registry lookup
      // needs an owning key, and the group blocks re-match the ids below.
      const std::string stateId(reader.readBytes());
      const auto nsSchemaId = reader.readBytes();
      const auto valueSchemaId = reader.readBytes();
      auto it = createdStates_.find(stateId);
      VELOX_CHECK(
          it != createdStates_.end(),
          "Checkpoint contains state '{}' which is not registered; states must be registered before restore",
          stateId);
      VELOX_CHECK_EQ(
          it->second->namespaceSchemaId(),
          nsSchemaId,
          "Namespace schema of state '{}' changed since the checkpoint",
          stateId);
      VELOX_CHECK_EQ(
          it->second->valueSchemaId(),
          valueSchemaId,
          "Value schema of state '{}' changed since the checkpoint",
          stateId);
      stateIds.push_back(stateId);
      statesById.push_back(it->second);
    }
    // The stream must overlap this backend's range: this backend picks its
    // own key groups out of the stream and skips the rest (rescale
    // restore); no overlap at all is a mis-delivery.
    const uint64_t streamEndKeyGroup =
        static_cast<uint64_t>(header.startKeyGroup) + header.numKeyGroups;
    const uint64_t myStartKeyGroup = static_cast<uint64_t>(startKeyGroup_);
    const uint64_t myEndKeyGroup =
        static_cast<uint64_t>(startKeyGroup_) + numKeyGroups_;
    VELOX_CHECK(
        header.startKeyGroup < myEndKeyGroup &&
            streamEndKeyGroup > myStartKeyGroup,
        "Checkpoint key-group range [{},{}) does not overlap this backend's [{},{})",
        header.startKeyGroup,
        streamEndKeyGroup,
        myStartKeyGroup,
        myEndKeyGroup);
    for (uint64_t keyGroup = header.startKeyGroup; keyGroup < streamEndKeyGroup;
         ++keyGroup) {
      VELOX_CHECK_EQ(
          reader.readUInt32(),
          keyGroup,
          "Corrupt checkpoint: key groups are not ascending");
      const bool mine = keyGroup >= myStartKeyGroup && keyGroup < myEndKeyGroup;
      for (uint32_t id = 0; id < stateCount; ++id) {
        VELOX_CHECK_EQ(
            reader.readBytes(),
            stateIds[id],
            "Corrupt checkpoint: state blocks out of schema-table order");
        const auto entryCount = reader.readInt32();
        VELOX_CHECK_GE(
            entryCount, 0, "Corrupt checkpoint: negative entry count");
        for (int32_t i = 0; i < entryCount; ++i) {
          if (mine) {
            statesById[id]->restoreEntry(reader);
          } else {
            readEntryFrames(reader);
          }
        }
      }
    }
    VELOX_CHECK(reader.atEnd(), "Corrupt checkpoint: trailing bytes");
  }

 private:
  /// Returns the cached handle registered under 'name' when it is of the
  /// requested type, nullptr when the name is unknown; a type mismatch
  /// fails: one name is one state.
  template <typename S>
  std::shared_ptr<S> findState(const std::string& name) {
    auto it = createdStates_.find(name);
    if (it == createdStates_.end()) {
      return nullptr;
    }
    auto state = std::dynamic_pointer_cast<S>(it->second);
    VELOX_CHECK_NOT_NULL(
        state, "State '{}' is already registered with a different type", name);
    return state;
  }

  std::shared_ptr<TypeSerializer<K>> keySerializer_;
  const uint32_t maxParallelism_;
  const uint32_t startKeyGroup_;
  const uint32_t numKeyGroups_;
  // Handle registry, by state name; each handle owns its state table.
  std::unordered_map<std::string, StatePtr> createdStates_;
};

} // namespace facebook::velox::stateful
