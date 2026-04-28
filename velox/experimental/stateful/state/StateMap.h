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

#include "velox/common/base/BitUtil.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/experimental/stateful/utils/MathUtils.h"

#include <memory>
#include <vector>
#include <set>
#include <climits>

namespace facebook::velox::stateful {

template<typename K, typename N, typename S>
struct StateMapEntry {

  K key_;
  N namespace_;
  S state_;
  uint64_t hash_;
  std::shared_ptr<StateMapEntry<K, N, S>> next_;
  int32_t entryVersion_;
  int32_t stateVersion_;

  StateMapEntry(K key, N ns, S state, uint64_t hash, std::shared_ptr<StateMapEntry<K, N, S>>& next, int32_t entryVersion, int32_t stateVersion)
   : key_(key), namespace_(ns), state_(state), hash_(hash), next_(next), entryVersion_(entryVersion), stateVersion_(stateVersion) {}

  StateMapEntry(StateMapEntry<K, N, S> other, int entryVersion)
    : StateMapEntry(other.key_, other.namespace_, other.state_, other.hash_, other.next_, entryVersion, other.stateVersion_) {}

  bool operator==(const StateMapEntry<K, N, S>& entry) const {
    return entry.key_ == key_ && entry.namespace_ == namespace_ && entry.state_ == state_;
  }

  uint64_t hashCode() {
    return bits::hashMix(bits::hashMix(key_, namespace_), state_);
  }
};

/**
 * This class is relevant to Flink org.apache.flink.runtime.state.heap.StateMap.
 * remove namespace first.
 * TODO: It is a simplified implementation, not equal to Flink.
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
template <typename K, typename N, typename S>
class StateMap {

#define MIN_TRANSFERRED_PER_INCREMENTAL_REHASH 4
#define MINIMUM_CAPACITY 4
#define MAXIMUM_CAPACITY 1 << 30
#define DEFAULT_CAPACITY 128
#define MAX_ARRAY_SIZE std::numeric_limits<int32_t>::max() - 8

 public:
  StateMap() : StateMap(DEFAULT_CAPACITY) {}
  StateMap(int32_t capacity) {
      primaryTable_ = empty_;
      incrementalRehashTable_ = empty_;
      highestRequiredSnapshotVersion_ = 0;
      stateMapVersion_ = 0;
      primaryTableSize_ = 0;
      incrementalRehashTableSize_ = 0;
      modCount_ = 0;

      if (capacity < 0) {
        threshold_ = -1;
        return;
      }

      if (capacity < MINIMUM_CAPACITY) {
        capacity = MINIMUM_CAPACITY;
      } else if (capacity > MAXIMUM_CAPACITY) {
        capacity = MAXIMUM_CAPACITY;
      } else {
        capacity = roundUpToPowerOfTwo(capacity);
      }

      primaryTable_ = makeTable(capacity);
  }

  S get(K key, N ns) {
    uint64_t hash = computeHashForOperationAndDoIncrementalRehash(key, ns);
    int32_t requiredVersion = highestRequiredSnapshotVersion_;
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& tab = selectActiveTable(hash);
    uint64_t index = hash & (tab.size() - 1);
    for (auto e = tab[index]; e != nullptr; e = e->next_) {
      if (e->hash_ == hash && e->key_ == key && e->namespace_ == ns) {
        if (e->stateVersion_ < requiredVersion) {
          if (e->entryVersion_ < requiredVersion) {
            e = handleChainedEntryCopyOnWrite(tab, hash & (tab.size() - 1), e);
          }
          e->stateVersion_ = stateMapVersion_;
          /// TODO: use type serializer to copy a state
          // e->state_ =
        }
        return e->state_;
      }
    }
    return nullptr;
  }

  void put(K key, N ns, S state) {
    std::shared_ptr<StateMapEntry<K, N, S>> e = putEntry(key, ns);
    e->state_ = state;
    e->stateVersion_ = stateMapVersion_;
  }

  bool containsKey(K key, N ns) {
    uint64_t hash = computeHashForOperationAndDoIncrementalRehash(key, ns);
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& tab = selectActiveTable(hash);
    uint64_t index = hash & (tab.size() - 1);
    for (auto e = tab[index]; e != nullptr; e = e->next_) {
      if (e->hash_ == hash && e->namespace_ == ns && e->key_ == key) {
        return true;
      }
    }
    return false;
  }

  void remove(K key, N ns) {
    removeEntry(key, ns);
  }

  void clear() {

  }

  size_t size() {
    return primaryTableSize_ + incrementalRehashTableSize_;
  }

 private:
  std::vector<std::shared_ptr<StateMapEntry<K, N, S>>> primaryTable_;
  std::vector<std::shared_ptr<StateMapEntry<K, N, S>>> incrementalRehashTable_;
  std::vector<std::shared_ptr<StateMapEntry<K, N, S>>> empty_;
  int32_t highestRequiredSnapshotVersion_{0};
  int32_t stateMapVersion_{0};
  // Must be zero-initialized: selectActiveTable() compares this against
  // (hash & (primaryTable_.size() - 1)) to decide which table holds an entry.
  // An indeterminate value here can route lookups to the empty rehash table
  // and cause an out-of-bounds read in get()/put(), leading to SIGSEGV.
  int32_t rehashIndex_{0};
  uint64_t primaryTableSize_{0};
  uint64_t incrementalRehashTableSize_{0};
  uint64_t modCount_{0};
  uint64_t threshold_{0};
  N lastNamespace_{};
  std::set<int32_t> snapshotVersions_;

  std::shared_ptr<StateMapEntry<K, N, S>> putEntry(K key, N ns) {
    uint64_t hash = computeHashForOperationAndDoIncrementalRehash(key, ns);
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& tab = selectActiveTable(hash);
    uint64_t index = hash & (tab.size() - 1);
    for (auto e = tab[index]; e != nullptr; e = e->next_) {
      if (e->hash_ == hash && e->key_ == key && e->namespace_ == ns) {
        if (e->entryVersion_ < highestRequiredSnapshotVersion_) {
          e = handleChainedEntryCopyOnWrite(tab, index, e);
        }
        return e;
      }
    }
    ++modCount_;
    if (size() > threshold_) {
      doubleCapacity();
    }
    return addNewStateMapEntry(tab, key,ns, hash);
  }

  std::shared_ptr<StateMapEntry<K, N, S>> removeEntry(K key, N ns) {
    uint64_t hash = computeHashForOperationAndDoIncrementalRehash(key, ns);
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& tab = selectActiveTable(hash);
    uint64_t index = hash & (tab.size() - 1);
    for (std::shared_ptr<StateMapEntry<K, N, S>> e = tab[index], prev = nullptr; e != nullptr; prev = e, e = e->next_) {
      if (e->hash_ == hash && e->key_ == key && e->namespace_ == ns) {
        if (prev == nullptr) {
          tab[index] = e->next_;
        } else {
          if (prev->entryVersion_ < highestRequiredSnapshotVersion_) {
            prev = handleChainedEntryCopyOnWrite(tab, index, prev);
          }
          prev->next_ = e->next_;
        }
        ++modCount_;
        if (&tab == &primaryTable_) {
          --primaryTableSize_;
        } else {
          --incrementalRehashTableSize_;
        }
        return e;
      }
    }
    return nullptr;
  }

  std::shared_ptr<StateMapEntry<K, N, S>> addNewStateMapEntry(std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& table, K key, N& ns, uint64_t hash) {
    if (ns == lastNamespace_) {
      ns = lastNamespace_;
    } else {
      lastNamespace_ = ns;
    }

    uint64_t index = hash & (table.size() - 1);
    std::shared_ptr<StateMapEntry<K, N, S>> newEntry = std::make_shared<StateMapEntry<K, N, S>>(key, ns, nullptr, hash, table[index], stateMapVersion_, stateMapVersion_);
    table[index] = newEntry;
    if (&table == &primaryTable_) {
      ++primaryTableSize_;
    } else {
      ++incrementalRehashTableSize_;
    }
    return newEntry;
  }

  std::shared_ptr<StateMapEntry<K, N, S>> handleChainedEntryCopyOnWrite(
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& tab, uint64_t mapIdx, std::shared_ptr<StateMapEntry<K, N, S>>& untilEntry) {
      int32_t required = highestRequiredSnapshotVersion_;
      std::shared_ptr<StateMapEntry<K, N, S>> current = tab[mapIdx];
      std::shared_ptr<StateMapEntry<K, N, S>> copy = nullptr;
      if (current->entryVersion_ < required) {
        copy = std::make_shared<StateMapEntry<K, N, S>>(*current, stateMapVersion_);
        tab[mapIdx] = copy;
      } else {
        copy = current;
      }

      while (current != untilEntry) {
        current = current->next_;
        if (current->entryVersion_ < required) {
          copy->next_ = std::make_shared<StateMapEntry<K, N, S>>(*current, stateMapVersion_);
          copy = copy->next_;
        } else {
          copy = current;
        }
      }
      return copy;
  }

  std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& selectActiveTable(uint64_t hashCode) {
    return (hashCode & (primaryTable_.size() - 1)) >= rehashIndex_ ? primaryTable_ : incrementalRehashTable_;
  }

  uint64_t compositeHash(K key, N ns) {
    if constexpr (std::is_same<N, TimeWindow>::value) {
      return bits::hashMix(key, ns.hashCode());
    } else {
      return bits::hashMix(key, ns);
    }
  }

  void incrementalRehash() {
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& oldMap = primaryTable_;
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& newMap = incrementalRehashTable_;

    size_t oldCapacity = oldMap.size();
    size_t newMask = newMap.size() - 1;
    int32_t requiredVersion = highestRequiredSnapshotVersion_;
    int32_t rhIdx = rehashIndex_;
    int32_t transferred = 0;

    while (transferred < MIN_TRANSFERRED_PER_INCREMENTAL_REHASH) {
      std::shared_ptr<StateMapEntry<K, N, S>> e = oldMap[rhIdx];
      while (e != nullptr) {
        if (e->entryVersion_ < requiredVersion) {
          e = std::make_shared<StateMapEntry<K, N, S>>(*e, stateMapVersion_);
        }
        std::shared_ptr<StateMapEntry<K, N, S>> n = e->next_;
        uint64_t pos = e->hash_ & newMask;
        e->next_ = newMap[pos];
        newMap[pos] = e;
        e = n;
        ++transferred;
      }

      oldMap[rhIdx] = nullptr;
      if (++rhIdx == oldCapacity) {
        primaryTable_ = newMap;
        incrementalRehashTable_ = empty_;
        primaryTableSize_ += incrementalRehashTableSize_;
        incrementalRehashTableSize_ = 0;
        rehashIndex_ = 0;
        return ;
      }
    }

    primaryTableSize_ -= transferred;
    incrementalRehashTableSize_ += transferred;
    rehashIndex_ = rhIdx;
  }

  bool isRehashing() {
    return empty_ != incrementalRehashTable_;
  }

  uint64_t computeHashForOperationAndDoIncrementalRehash(K key, N ns) {
    if (isRehashing()) {
      incrementalRehash();
    }
    return compositeHash(key, ns);
  }

  void doubleCapacity() {
    VELOX_CHECK(!isRehashing(), "There is already a rehash in progress.");
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>>& oldMap = primaryTable_;
    int oldCapacity = oldMap.size();
    if (oldCapacity == MAXIMUM_CAPACITY) {
      return;
    }
    incrementalRehashTable_ = makeTable(oldCapacity * 2);
  }

  std::vector<std::shared_ptr<StateMapEntry<K, N, S>>> makeTable(uint64_t newCapacity) {
    if (newCapacity < MAXIMUM_CAPACITY) {
      threshold_ = (newCapacity >> 1) + (newCapacity >> 2);
    } else {
      if (size() > MAX_ARRAY_SIZE) {
        VELOX_FAIL("Maximum capacity of CopyOnWriteStateMap is reached and the job cannot continue.");
      } else {
        LOG(WARNING) << "Maximum capacity of 2^30 in StateMap reached. Cannot increase hash map size.";
        threshold_ = MAX_ARRAY_SIZE;
      }
    }
    std::vector<std::shared_ptr<StateMapEntry<K, N, S>>> newMap;
    newMap.resize(newCapacity);
    return newMap;
  }
};
} // namespace facebook::velox::stateful
