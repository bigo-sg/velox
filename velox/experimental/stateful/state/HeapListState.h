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

#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateTable.h"

namespace facebook::velox::stateful {

// This class is relevent to flink HeapMapState.
template <typename K, typename N, typename V>
class HeapListState : public ListState<K, N, V> {
 public:
  HeapListState(int keyGroupNumber) {
    stateTable_ =
        std::make_unique<
            StateTable<K, N, std::shared_ptr<std::list<V>>>>(keyGroupNumber);
  }

  std::list<V>& get(K key, N ns) override {
    auto currentList = stateTable_->get(key, ns);
    return *currentList.get();
  }

  void add(K key, N ns, V value) override {
    auto currentList = getOrCreate( key, ns);
    currentList->push_back(value);
  }

  /**
  void addAll(int keyGroupIndex, std::list<T>& values) override {
    std::shared_ptr<std::list<T>> currentList = getOrCreate(keyGroupIndex);
    currentList->insert(currentList->end(), values.begin(), values.end());
  }

  void update(int keyGroupIndex, std::list<T>& values) override {
    std::shared_ptr<std::list<T>> currentList = getOrCreate(keyGroupIndex);
    *currentList = values; // Replace the current list with the new values
  }
  */

  void clear() override {
    stateTable_->clear();
  }

  void remove(K key, N ns) override {
    stateTable_->remove(key, ns);
  }

 private:
  std::shared_ptr<std::list<V>> getOrCreate(K key, N ns) {
    std::shared_ptr<std::list<V>> currentList =
        stateTable_->get(key, ns);
    if (currentList == nullptr) {
      currentList = std::make_shared<std::list<V>>();
      stateTable_->put(key, ns, currentList);
    }
    return currentList;
  }

  std::unique_ptr<StateTable<K, N, std::shared_ptr<std::list<V>>>> stateTable_;
};
} // namespace facebook::velox::stateful
