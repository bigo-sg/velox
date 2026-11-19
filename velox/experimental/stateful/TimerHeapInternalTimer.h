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

#include "velox/common/base/BitUtil.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink InternalTimerServiceImpl.
//
// The key rides the heap of an IndexedPriorityQueue, whose percolation
// assigns elements: state keys that are immutable views with const members
// (RowContainerStateKey) delete copy assignment. Storing the key behind a
// shared_ptr keeps copy assignment available for any K while preserving the
// value semantics - key() returns const K&, and operator== compares the
// pointees, not the pointers.
template <typename K, typename N>
class TimerHeapInternalTimer {
 public:
  TimerHeapInternalTimer(int64_t timestamp, const K& key, const N& ns)
      : timestamp_(timestamp),
        key_(std::make_shared<const K>(key)),
        ns_(ns),
        keyGroupIndex_(0) {}

  int64_t timestamp() const {
    return timestamp_;
  }

  const K& key() const {
    return *key_;
  }

  const N& ns() const {
    return ns_;
  }

  int keyGroupIndex() const {
    return keyGroupIndex_;
  }

  bool operator==(const TimerHeapInternalTimer& other) const {
    return timestamp_ == other.timestamp_ && *key_ == *other.key_ &&
        ns_ == other.ns_;
  }

 private:
  int64_t timestamp_;
  // The key rides the heap behind a shared_ptr (see the class comment): the
  // heap's percolation assigns elements, and state keys with const members
  // delete copy assignment; the wrapper restores assignability for any K
  // while key() keeps returning const K&.
  std::shared_ptr<const K> key_;
  N ns_;
  int keyGroupIndex_;
};

// Hash/Equal/Comparator/PriorityFn functors operate on TimerHeapInternalTimer
// values directly (no shared_ptr indirection), so they can be used with a
// HeapPriorityQueue whose element type is TimerHeapInternalTimer<K, N>.
template <typename K, typename N>
struct HeapTimerHasher {
  size_t operator()(const TimerHeapInternalTimer<K, N>& a) const {
    return bits::hashMix(a.timestamp(), a.key());
  }
};

template <typename K, typename N>
struct HeapTimerEquals {
  bool operator()(
      const TimerHeapInternalTimer<K, N>& a,
      const TimerHeapInternalTimer<K, N>& b) const {
    return a == b;
  }
};

template <typename K, typename N>
struct HeapTimerComparator {
  bool operator()(
      const TimerHeapInternalTimer<K, N>& a,
      const TimerHeapInternalTimer<K, N>& b) const {
    return a.timestamp() < b.timestamp();
  }
};

// Priority extractor used with HeapPriorityQueue (which is layered on
// IndexedPriorityQueue). Returning timestamp as the priority yields a
// min-heap whose head is the earliest pending timer.
template <typename K, typename N>
struct HeapTimerPriorityFn {
  int64_t operator()(const TimerHeapInternalTimer<K, N>& timer) const {
    return timer.timestamp();
  }
};

} // namespace facebook::velox::stateful
