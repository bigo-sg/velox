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

#include <memory>
#include <utility>
#include <vector>

#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateTable.h"

namespace facebook::velox::stateful {

/// ListState on the heap storage: the value is a shared vector, created on
/// the first add and removed state reads as an empty list, as in Flink
/// HeapListState.
/// @param <K> type of key, a StateKey subclass
/// @param <N> type of namespace, a Namespace subclass
/// @param <T> type of the list elements
template <typename K, typename N, typename T>
class HeapListState : public ListState<K, N, T> {
 public:
  HeapListState(
      std::shared_ptr<StateTable<K, N, std::shared_ptr<std::vector<T>>>>
          stateTable)
      : stateTable_(std::move(stateTable)) {}

  std::vector<T> get(const K& key, const N& ns) override {
    auto currentList = stateTable_->get(key, ns);
    return currentList == nullptr ? std::vector<T>{} : *currentList;
  }

  void add(const K& key, const N& ns, const T& value) override {
    getOrCreate(key, ns)->push_back(value);
  }

  void remove(const K& key, const N& ns) override {
    stateTable_->remove(key, ns);
  }

  void clear() override {
    stateTable_->clear();
  }

 private:
  std::shared_ptr<std::vector<T>> getOrCreate(const K& key, const N& ns) {
    if (auto currentList = stateTable_->get(key, ns)) {
      return currentList;
    }
    auto freshList = std::make_shared<std::vector<T>>();
    stateTable_->put(key, ns, freshList);
    return freshList;
  }

  std::shared_ptr<StateTable<K, N, std::shared_ptr<std::vector<T>>>> stateTable_;
};
} // namespace facebook::velox::stateful
