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
#include "velox/experimental/stateful/state/HeapAccState.h"
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

  /// Returns the AccState handle for the descriptor, creating the state
  /// table and the handle on the first call; a second call with the same
  /// name returns the same handle.
  template <typename N>
  std::shared_ptr<AccState<K, N>> getOrCreateAccState(
      const AccStateDescriptor& descriptor) {
    if (auto state = findState<AccState<K, N>>(descriptor.name())) {
      return state;
    }
    auto state = std::make_shared<HeapAccState<K, N>>(
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
  /// stream: a header describing the states (name, schemas, key-group
  /// range), then the entries grouped by key group (ascending); within one
  /// key group each state writes its block in header order, addressed by
  /// its int16 name id. States are visited in name order so the stream is
  /// reproducible. The header plays the role of Flink's per-state metadata
  /// snapshots: restore cross-checks it against the backend and its
  /// registered states instead of relying on registration order.
  std::string snapshot() {
    std::vector<std::pair<std::string, StatePtr>> states(
        createdStates_.begin(), createdStates_.end());
    std::sort(states.begin(), states.end());
    std::string bytes;
    CheckpointWriter writer(bytes);
    writer.writeInt32(kCheckpointFormatVersion);
    writer.writeBytes(keySerializer_->schema());
    writer.writeInt32(static_cast<int32_t>(states.size()));
    for (const auto& [name, state] : states) {
      writer.writeBytes(name);
      writer.writeBytes(state->namespaceSchema());
      writer.writeBytes(state->valueSchema());
      writer.writeInt32(static_cast<int32_t>(startKeyGroup_));
      writer.writeInt32(static_cast<int32_t>(numKeyGroups_));
    }
    for (uint32_t keyGroup = startKeyGroup_;
         keyGroup < startKeyGroup_ + numKeyGroups_;
         ++keyGroup) {
      writer.writeInt32(static_cast<int32_t>(keyGroup));
      for (int32_t id = 0; id < static_cast<int32_t>(states.size()); ++id) {
        writer.writeInt16(static_cast<int16_t>(id));
        states[id].second->snapshotKeyGroup(
            static_cast<int32_t>(keyGroup), writer);
      }
    }
    return bytes;
  }

  /// Restores the stream written by snapshot() into the states this backend
  /// has registered: every state of the checkpoint must already be
  /// registered under the same name with a matching schema, and the
  /// checkpoint's key-group range must overlap this backend's. Key groups
  /// of the stream outside this backend's range are skipped, so a
  /// checkpoint written under a different parallelism restores into this
  /// backend — one covering stream at once, the disjoint-range pieces of a
  /// scale-down in sequence — and only a stream with no overlap at all is
  /// rejected. This is the reverse of Flink's order (restore before state
  /// creation) and is what makes handle-owned state tables possible; the
  /// byte layout of the entries is the same.
  void restore(const std::string& bytes) {
    CheckpointReader reader(bytes.data(), bytes.size());
    VELOX_CHECK_EQ(
        reader.readInt32(),
        kCheckpointFormatVersion,
        "Unsupported checkpoint format version");
    const auto keySchema = reader.readBytes();
    VELOX_CHECK_EQ(
        keySchema,
        keySerializer_->schema(),
        "Checkpoint key schema does not match this backend's key serializer");
    const auto stateCount = reader.readInt32();
    VELOX_CHECK_GE(stateCount, 0, "Corrupt checkpoint: negative state count");
    std::vector<StatePtr> statesById(stateCount);
    int32_t streamStartKeyGroup = 0;
    int32_t streamNumKeyGroups = 0;
    for (int32_t id = 0; id < stateCount; ++id) {
      // One bounded copy per state (header only): the registry lookup needs
      // an owning key.
      const std::string name(reader.readBytes());
      const auto nsSchema = reader.readBytes();
      const auto valueSchema = reader.readBytes();
      const auto startKeyGroup = reader.readInt32();
      const auto numKeyGroups = reader.readInt32();
      VELOX_CHECK_GE(
          startKeyGroup, 0, "Corrupt checkpoint: negative key-group start");
      VELOX_CHECK_GT(
          numKeyGroups, 0, "Corrupt checkpoint: non-positive key-group count");
      if (id == 0) {
        streamStartKeyGroup = startKeyGroup;
        streamNumKeyGroups = numKeyGroups;
      } else {
        VELOX_CHECK_EQ(
            startKeyGroup,
            streamStartKeyGroup,
            "Corrupt checkpoint: states disagree on the key-group range");
        VELOX_CHECK_EQ(
            numKeyGroups,
            streamNumKeyGroups,
            "Corrupt checkpoint: states disagree on the key-group range");
      }
      auto it = createdStates_.find(name);
      VELOX_CHECK(
          it != createdStates_.end(),
          "Checkpoint contains state '{}' which is not registered; states must be registered before restore",
          name);
      VELOX_CHECK_EQ(
          it->second->namespaceSchema(),
          nsSchema,
          "Namespace schema of state '{}' changed since the checkpoint",
          name);
      VELOX_CHECK_EQ(
          it->second->valueSchema(),
          valueSchema,
          "Value schema of state '{}' changed since the checkpoint",
          name);
      statesById[id] = it->second;
    }
    if (stateCount == 0) {
      // A checkpoint of no states carries no range fields; its body is only
      // the key-group ids, with an empty block per group.
      while (!reader.atEnd()) {
        reader.readInt32();
      }
      return;
    }
    // The stream must overlap this backend's range: this backend picks its
    // own key groups out of the stream and skips the rest (rescale
    // restore); no overlap at all is a mis-delivery.
    const int64_t streamEndKeyGroup =
        static_cast<int64_t>(streamStartKeyGroup) + streamNumKeyGroups;
    const int64_t myStartKeyGroup = static_cast<int64_t>(startKeyGroup_);
    const int64_t myEndKeyGroup =
        static_cast<int64_t>(startKeyGroup_) + numKeyGroups_;
    VELOX_CHECK(
        streamStartKeyGroup < myEndKeyGroup &&
            streamEndKeyGroup > myStartKeyGroup,
        "Checkpoint key-group range [{},{}) does not overlap this backend's [{},{})",
        streamStartKeyGroup,
        streamEndKeyGroup,
        myStartKeyGroup,
        myEndKeyGroup);
    for (int64_t keyGroup = streamStartKeyGroup; keyGroup < streamEndKeyGroup;
         ++keyGroup) {
      VELOX_CHECK_EQ(
          reader.readInt32(),
          keyGroup,
          "Corrupt checkpoint: key groups are not ascending");
      const bool mine = keyGroup >= myStartKeyGroup && keyGroup < myEndKeyGroup;
      for (int32_t id = 0; id < stateCount; ++id) {
        VELOX_CHECK_EQ(
            reader.readInt16(),
            static_cast<int16_t>(id),
            "Corrupt checkpoint: state blocks out of header order");
        const auto entryCount = reader.readInt32();
        VELOX_CHECK_GE(
            entryCount, 0, "Corrupt checkpoint: negative entry count");
        for (int32_t i = 0; i < entryCount; ++i) {
          if (mine) {
            statesById[id]->restoreEntry(reader);
          } else {
            // An entry is always three length-prefixed components
            // (namespace, key, payload) whatever the state kind, so skipping
            // one is three bounds-checked advances.
            reader.readBytes();
            reader.readBytes();
            reader.readBytes();
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
