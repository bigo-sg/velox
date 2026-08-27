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

#include <map>
#include <memory>
#include <utility>

#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateTable.h"

namespace facebook::velox::stateful {

/// MapState on the heap storage: the value is a shared std::map, created on
/// the first put, overwritten on put of an existing user key and read as an
/// empty map when absent, as in Flink HeapMapState. Removing from an absent
/// map does not materialize one.
/// @param <K> type of key, a StateKey subclass
/// @param <N> type of namespace, a Namespace subclass
/// @param <UK> type of the user keys of the map
/// @param <UV> type of the user values of the map
template <typename K, typename N, typename UK, typename UV>
class HeapMapState : public MapState<K, N, UK, UV> {
 public:
  HeapMapState(
      std::shared_ptr<StateTable<K, N, std::shared_ptr<std::map<UK, UV>>>>
          stateTable)
      : stateTable_(std::move(stateTable)) {}

  UV get(const K& key, const N& ns, const UK& userKey) override {
    auto currentMap = stateTable_->get(key, ns);
    if (currentMap == nullptr) {
      return UV();
    }
    auto it = currentMap->find(userKey);
    return it == currentMap->end() ? UV() : it->second;
  }

  void put(const K& key, const N& ns, const UK& userKey, const UV& value)
      override {
    (*getOrCreate(key, ns))[userKey] = value;
  }

  std::map<UK, UV> entries(const K& key, const N& ns) override {
    auto currentMap = stateTable_->get(key, ns);
    return currentMap == nullptr ? std::map<UK, UV>{} : *currentMap;
  }

  void remove(const K& key, const N& ns, const UK& userKey) override {
    if (auto currentMap = stateTable_->get(key, ns)) {
      currentMap->erase(userKey);
    }
  }

  void clear() override {
    stateTable_->clear();
  }

 private:
  std::shared_ptr<std::map<UK, UV>> getOrCreate(const K& key, const N& ns) {
    if (auto currentMap = stateTable_->get(key, ns)) {
      return currentMap;
    }
    auto freshMap = std::make_shared<std::map<UK, UV>>();
    stateTable_->put(key, ns, freshMap);
    return freshMap;
  }

  std::shared_ptr<StateTable<K, N, std::shared_ptr<std::map<UK, UV>>>>
      stateTable_;
};
} // namespace facebook::velox::stateful
