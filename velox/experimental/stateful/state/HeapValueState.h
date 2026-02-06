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

// This class is relevant to Flink HeapMapState.
template <typename K, typename N, typename V>
class HeapValueState : public ValueState<K, N, V> {
 public:
  HeapValueState(int keyGroupNumber) {
    stateTable_ = std::make_unique<StateTable<K, N, V>>(keyGroupNumber);
  }

  V value(const K& key, const N& ns) override {
    return stateTable_->get(key, ns);
  }

  void update(const K& key, const N& ns, const V& value) override {
    stateTable_->put(key, ns, value);
  }

  void remove(const K& key, const N& ns) override {
    stateTable_->remove(key, ns);
  }

  void clear() override {
    stateTable_->clear();
  }

 private:
  std::unique_ptr<StateTable<K, N, V>> stateTable_;
};
} // namespace facebook::velox::stateful
