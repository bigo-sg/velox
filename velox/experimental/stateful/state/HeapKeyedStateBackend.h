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
#include <memory>
#include <string>
#include <unordered_map>

#include "velox/common/base/Exceptions.h"
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
/// The state tables are registered by descriptor name and owned by this
/// backend (stateTables_, relevant to Flink registeredKVStates); the state
/// handles returned to operators are cached by name as well (createdStates_,
/// relevant to Flink createdKVStates). By-name access happens on
/// registration and later on the snapshot / restore plane; the runtime hot
/// path goes through the typed handle.
///
/// The interface of the non-template base KeyedStateBackend predates the
/// typed state API: its state factory methods do not fit (K, N) and are
/// overridden as not-implemented until the stateful operators migrate.
template <typename K>
class HeapKeyedStateBackend : public KeyedStateBackend {
 public:
  /// 'startKeyGroup' and 'numKeyGroups' describe this backend's key-group
  /// sub-range (all state tables bucket over exactly this range);
  /// 'maxParallelism' defines the key-group space, hash % maxParallelism,
  /// and stays stable across rescale.
  HeapKeyedStateBackend(
      uint32_t maxParallelism,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups)
      : maxParallelism_(maxParallelism),
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
    VELOX_NYI("getOrCreateGroupValueState is not available on the heap backend");
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

  // Snapshot / restore are implemented together with the snapshot stage.
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
        getOrCreateStateTable<N, char*>(descriptor.name()), descriptor);
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
        getOrCreateStateTable<N, V>(descriptor.name()));
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
        getOrCreateStateTable<N, std::shared_ptr<std::vector<T>>>(
            descriptor.name()));
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
        getOrCreateStateTable<N, std::shared_ptr<std::map<UK, UV>>>(
            descriptor.name()));
    createdStates_.emplace(descriptor.name(), state);
    return state;
  }

 private:
  /// Returns the state table registered under 'name', creating it on the
  /// first call. A name already registered with a different (N, S) fails:
  /// one name is one state.
  template <typename N, typename S>
  std::shared_ptr<StateTable<K, N, S>> getOrCreateStateTable(
      const std::string& name) {
    auto it = stateTables_.find(name);
    if (it != stateTables_.end()) {
      auto table = std::dynamic_pointer_cast<StateTable<K, N, S>>(it->second);
      VELOX_CHECK_NOT_NULL(
          table,
          "State '{}' is already registered with a different type",
          name);
      return table;
    }
    auto table =
        std::make_shared<StateTable<K, N, S>>(startKeyGroup_, numKeyGroups_);
    stateTables_.emplace(name, table);
    return table;
  }

  /// Returns the cached handle registered under 'name' when it is of the
  /// requested type, nullptr when the name is unknown; a type mismatch
  /// fails as in getOrCreateStateTable.
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

  const uint32_t maxParallelism_;
  const uint32_t startKeyGroup_;
  const uint32_t numKeyGroups_;
  // Storage ownership, by state name.
  std::unordered_map<std::string, std::shared_ptr<StateTableBase>> stateTables_;
  // Handle cache, by state name.
  std::unordered_map<std::string, StatePtr> createdStates_;
};

} // namespace facebook::velox::stateful
