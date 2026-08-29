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
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/state/StateMap.h"

namespace facebook::velox::stateful {

/// Two-layer (key, namespace) -> state storage for one state name. Layer 1
/// buckets by key group: bucket count is the backend's key-group sub-range
/// and the bucket index is key.keyGroup() - startKeyGroup, so the bucket
/// index is the key group and snapshotting can stream bucket by bucket
/// without recomputing hashes. Layer 2 is a StateMap keyed by the composite
/// (K, N). All StateMaps are created eagerly at construction, as in Flink
/// org.apache.flink.runtime.state.heap.StateTable.
/// @param <K> type of key, a StateKey subclass
/// @param <N> type of namespace, a Namespace subclass
/// @param <S> type of state, a nullable pointer type (miss is nullptr)
template <typename K, typename N, typename S>
class StateTable {
 public:
  StateTable(uint32_t startKeyGroup, uint32_t numKeyGroups)
      : startKeyGroup_(startKeyGroup), buckets_(numKeyGroups) {
    VELOX_CHECK(numKeyGroups > 0, "numKeyGroups must be greater than 0");
  }

  /// Returns the state for (key, ns) or nullptr on a miss.
  S get(const K& key, const N& ns) {
    return bucket(key).get(key, ns);
  }

  void put(const K& key, const N& ns, S state) {
    bucket(key).put(key, ns, state);
  }

  void remove(const K& key, const N& ns) {
    bucket(key).remove(key, ns);
  }

  /// Number of entries across all key groups.
  size_t size() const {
    size_t total = 0;
    for (const auto& bucket : buckets_) {
      total += bucket.size();
    }
    return total;
  }

  /// Drops the entries of all key groups.
  void clear() {
    buckets_.assign(buckets_.size(), StateMap<K, N, S>());
  }

  uint32_t startKeyGroup() const {
    return startKeyGroup_;
  }

  uint32_t numKeyGroups() const {
    return buckets_.size();
  }

  /// Returns the StateMap of one key group; the snapshot path streams
  /// bucket by bucket. 'keyGroup' must be inside the sub-range.
  StateMap<K, N, S>& stateMapForKeyGroup(uint32_t keyGroup) {
    VELOX_CHECK(
        keyGroup >= startKeyGroup_ &&
            keyGroup < startKeyGroup_ + buckets_.size(),
        "Key group {} is outside the state table range [{}, {})",
        keyGroup,
        startKeyGroup_,
        startKeyGroup_ + buckets_.size());
    return buckets_[keyGroup - startKeyGroup_];
  }

 private:
  StateMap<K, N, S>& bucket(const K& key) {
    return stateMapForKeyGroup(static_cast<uint32_t>(key.keyGroup()));
  }

  const uint32_t startKeyGroup_;
  std::vector<StateMap<K, N, S>> buckets_;
};
} // namespace facebook::velox::stateful
