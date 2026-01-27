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

#include "velox/experimental/stateful/state/StateEntry.h"

#include <vector>

namespace facebook::velox::stateful {

/**
 * This class is relevent to flink org.apache.flink.runtime.state.heap.StateMap.
 * remove namespace first.
 * TODO: It is a simplified implementation, not equal to flink.
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
template <typename K, typename N, typename S>
class StateMap {
 public:
  StateMap() {
    tempTable_.resize(1024);
  }

  S get(K key, N ns) {
    auto hash = (std::hash<K>{}(key) + std::hash<N>{}(ns)) % tempTable_.size();
    if (tempTable_[hash] != nullptr && tempTable_[hash]->getKey() == key &&
        tempTable_[hash]->getNamespace() == ns) {
      return tempTable_[hash]->getState();
    }
    return S(); // Return default state if not found
  }

  void put(K key, N ns, S state) {
    // TODO: add enlarge logic.
    auto hash = (std::hash<K>{}(key) + std::hash<N>{}(ns)) % tempTable_.size();
    tempTable_[hash] = std::make_shared<StateEntry<K, N, S>>(key, ns, state);
  }

  void remove(K key, N ns) {
    auto hash = (std::hash<K>{}(key) + std::hash<N>{}(ns)) % tempTable_.size();
    if (tempTable_[hash] != nullptr && tempTable_[hash]->getKey() == key &&
        tempTable_[hash]->getNamespace() == ns) {
      tempTable_[hash] = nullptr; // Remove the entry
    }
  }

 private:
  // TODO: use map temporarily, not equal to flink.
  std::vector<std::shared_ptr<StateEntry<K, N, S>>> tempTable_;
};
} // namespace facebook::velox::stateful
