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
#include <limits>
#include <utility>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/IndexedPriorityQueue.h"

namespace facebook::velox::stateful {

// Flink-style internal priority queue interface.
//
// This class is relevant to Flink InternalPriorityQueue.
template <typename T>
class InternalPriorityQueue {
 public:
  virtual ~InternalPriorityQueue() = default;

  virtual bool add(const T& toAdd) = 0;

  virtual bool add(T&& toAdd) = 0;

  virtual T poll() = 0;

  virtual const T& peek() const = 0;

  virtual void clear() = 0;

  virtual bool empty() const = 0;

  virtual bool remove(const T& toRemove) = 0;

  virtual size_t size() const = 0;

  virtual bool contains(const T& value) const = 0;
};

// Heap-backed implementation of InternalPriorityQueue, layered on top of
// velox::IndexedPriorityQueue without modifying the latter.
//
// Template parameters:
//   - T: element type.
//   - PriorityFn: callable producing an int64_t priority from a const T&.
//                 The priority defines ordering; ties are broken by
//                 IndexedPriorityQueue using insertion/update order (FIFO).
//   - kMaxQueue: false for min-heap (default), true for max-heap.
//   - Allocator/Hash/EqualTo: forwarded to IndexedPriorityQueue.
//
// CONTRACT for remove(value):
//   The implementation evicts arbitrary values by reassigning their priority
//   to a sentinel that is strictly more extreme than any real priority and
//   then popping the head. Therefore:
//     - For min-heap: real priorities MUST be > std::numeric_limits<int64_t>::min().
//     - For max-heap: real priorities MUST be < std::numeric_limits<int64_t>::max().
//   Timer services using timestamps satisfy this trivially.
template <
    typename T,
    typename PriorityFn,
    bool kMaxQueue = false,
    typename Allocator = std::allocator<T>,
    typename Hash = std::hash<T>,
    typename EqualTo = std::equal_to<T>>
class HeapPriorityQueue
    : public InternalPriorityQueue<T>,
      public velox::IndexedPriorityQueue<T, kMaxQueue, Allocator, Hash, EqualTo> {
 public:
  using IndexedBase =
      velox::IndexedPriorityQueue<T, kMaxQueue, Allocator, Hash, EqualTo>;

  HeapPriorityQueue() = default;

  explicit HeapPriorityQueue(
      PriorityFn priorityFn,
      const Allocator& allocator = {})
      : IndexedBase(allocator), priorityFn_(std::move(priorityFn)) {}

  // Adds an element. Returns true if the queue logically changed (new value
  // inserted, or an existing value's priority changed); false if the value
  // was already present with the same priority.
  bool add(const T& value) override {
    return IndexedBase::addOrUpdate(value, priorityFn_(value));
  }

  bool add(T&& value) override {
    const int64_t priority = priorityFn_(value);
    return IndexedBase::addOrUpdate(value, priority);
  }

  // Returns and removes the head. Throws if empty.
  T poll() override {
    VELOX_CHECK(!IndexedBase::empty(), "Cannot poll from an empty priority queue");
    return IndexedBase::pop();
  }

  // Returns the head without removing it. Throws if empty.
  const T& peek() const override {
    VELOX_CHECK(!IndexedBase::empty(), "Cannot peek from an empty priority queue");
    return IndexedBase::top();
  }

  // Removes the first occurrence of `value`. Returns true if removed.
  // Complexity: O(log n) plus one extra percolate compared to a native
  // remove-at-index implementation.
  bool remove(const T& value) override {
    const auto idx = IndexedBase::getValueIndex(value);
    if (!idx.has_value()) {
      return false;
    }
    constexpr int64_t kSentinel = kMaxQueue
        ? std::numeric_limits<int64_t>::max()
        : std::numeric_limits<int64_t>::min();
    IndexedBase::updatePriority(idx.value(), kSentinel);
    IndexedBase::pop();
    return true;
  }

  // Removes all elements. Complexity: O(n log n).
  void clear() override {
    while (!IndexedBase::empty()) {
      IndexedBase::pop();
    }
  }

  bool empty() const override {
    return IndexedBase::empty();
  }

  size_t size() const override {
    return static_cast<size_t>(IndexedBase::size());
  }

  bool contains(const T& value) const override {
    return IndexedBase::getValueIndex(value).has_value();
  }

 private:
  PriorityFn priorityFn_{};
};

} // namespace facebook::velox::stateful
