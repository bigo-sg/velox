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

namespace facebook::velox::stateful {

/**
 * This class is relevent to flink org.apache.flink.runtime.state.heap.StateMap.
 * remove namespace first. It is a simplified implementation.
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
template <typename K, typename N, typename S>
class StateEntry {
 public:
 StateEntry(K key, N ns, S state)
     : key_(std::move(key)), namespace_(ns), state_(std::move(state)) {}

  K getKey() {
    return key_;
  }

  N getNamespace() {
    return namespace_;
  }

  S getState() {
    return state_;
  }

 private:
  K key_;
  N namespace_;
  S state_;
};
} // namespace facebook::velox::stateful
