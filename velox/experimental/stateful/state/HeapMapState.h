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
template <typename K, typename N, typename UK, typename UV>
class HeapMapState : public MapState<K, N, UK, UV> {
 public:
  HeapMapState(int keyGroupNumber) : keyGroupNumber_(keyGroupNumber) {
    stateTable_ =
        std::make_unique<StateTable<K, N, std::shared_ptr<std::map<UK, UV>>>>(
            keyGroupNumber);
  }

  UV get(K key, N ns, UK userKey) override {
    std::shared_ptr<std::map<UK, UV>> currentMap =
        getOrCreate(key, ns);
    return currentMap->count(userKey) ? (*currentMap)[userKey] : UV();
  }

  void put(K key, N ns, UK userKey, UV value) override {
    std::shared_ptr<std::map<UK, UV>> currentMap =
        getOrCreate(key, ns);
    currentMap->insert({userKey, value});
  }

  std::map<UK, UV> entries(K key, N ns) override {
    return *getOrCreate(key, ns).get();
  }

  void clear() override {
    stateTable_->clear();
  }

  void remove(K key, N ns, UK userKey) override {
    std::shared_ptr<std::map<UK, UV>> currentMap =
        getOrCreate(key, ns);
    currentMap->erase(userKey);
  }

 private:
  std::shared_ptr<std::map<UK, UV>> getOrCreate(K key, N ns) {
    std::shared_ptr<std::map<UK, UV>> currentMap =
        stateTable_->get(key, ns);
    if (currentMap == nullptr) {
      currentMap = std::make_shared<std::map<UK, UV>>();
      stateTable_->put(key, ns, currentMap);
    }
    return currentMap;
  }

  std::unique_ptr<StateTable<K, N, std::shared_ptr<std::map<UK, UV>>>> stateTable_;
  int keyGroupNumber_;
};
} // namespace facebook::velox::stateful
