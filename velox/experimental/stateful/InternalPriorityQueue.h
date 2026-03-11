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

// This class is relevant to Flink InternalPriorityQueue.
template <typename T>
class InternalPriorityQueue {
 public:
  virtual bool add(const T& toAdd) = 0;

  virtual bool add(T&& toAdd) = 0;

  virtual T poll() = 0;

  virtual T& peek() = 0;

  virtual void clear() = 0;

  virtual bool empty() const = 0;

  virtual bool remove(const T& toRemove) = 0;

  virtual size_t size() const = 0;

  virtual bool contains(const T& value) const = 0;
};

template <
    typename T,
    typename Compare = std::less<T>,
    typename Hash = std::hash<T>,
    typename EqualTo = std::equal_to<T>>
class HeapPriorityQueue : public InternalPriorityQueue<T> {
 public:
  explicit HeapPriorityQueue(const Compare& comp = Compare())
      : comparator_(comp) {}

  /// Constructs a priority queue with elements from the range [first, last).
  template <typename InputIt>
  HeapPriorityQueue(InputIt first, InputIt last, const Compare& comp = Compare())
      : comparator_(comp), heap_(first, last) {
    buildHeap();
  }

  /// Returns the top element without removing it.
  /// Throws if the queue is empty.
  T& peek() override {
    VELOX_CHECK(!empty(), "Cannot peek from an empty priority queue");
    return heap_[0];
  }

  /// Removes and returns the top element.
  /// Throws if the queue is empty.
  T poll() override {
    VELOX_CHECK(!empty(), "Cannot poll from an empty priority queu");
    return removeAt(0);
  }

  /// Adds an element to the queue.
  bool add(const T& value) override {
    addImpl(value);
    return true;
  }

  /// Adds an element to the queue (move version).
  bool add(T&& value) override {
    addImpl(std::move(value));
    return true;
  }

  /// Removes the first occurrence of the specified element from the queue.
  /// Returns true if the element was found and removed, false otherwise.
  bool remove(const T& value) override {
    auto it = valueToIndex_.find(value);
    if (it == valueToIndex_.end()) {
      return false;
    }
    removeAt(it->second);
    return true;
  }

  /// Adds all elements from the range [first, last) to the queue.
  template <typename InputIt>
  void addAll(InputIt first, InputIt last) {
    for (auto it = first; it != last; ++it) {
      add(*it);
    }
  }

  /// Adds all elements from the container to the queue.
  template <typename Container>
  void addAll(const Container& container) {
    addAll(container.begin(), container.end());
  }

  /// Returns the number of elements in the queue.
  size_t size() const override {
    return heap_.size();
  }

  /// Returns true if the queue is empty.
  bool empty() const override {
    return heap_.empty();
  }

  /// Removes all elements from the queue.
  void clear() override {
    heap_.clear();
    valueToIndex_.clear();
  }

  /// Returns true if the queue contains the specified element.
  bool contains(const T& value) const {
    return valueToIndex_.find(value) != valueToIndex_.end();
  }

 private:
  Compare comparator_;
  std::vector<T> heap_;
  std::unordered_map<T, size_t, Hash, EqualTo> valueToIndex_;

  void addImpl(const T& value) {
    // Check if value already exists
    if (valueToIndex_.find(value) != valueToIndex_.end()) {
      // Update existing element
      size_t index = valueToIndex_[value];
      heap_[index] = value;
      // Re-heapify from this position
      percolateUp(index);
      percolateDown(index);
    } else {
      // Add new element
      size_t index = heap_.size();
      heap_.push_back(value);
      valueToIndex_[value] = index;
      percolateUp(index);
    }
  }

  void addImpl(T&& value) {
    // Check if value already exists
    auto it = valueToIndex_.find(value);
    if (it != valueToIndex_.end()) {
      // Update existing element
      size_t index = it->second;
      heap_[index] = std::move(value);
      // Re-heapify from this position
      percolateUp(index);
      percolateDown(index);
    } else {
      // Add new element
      size_t index = heap_.size();
      heap_.push_back(std::move(value));
      valueToIndex_[heap_.back()] = index;
      percolateUp(index);
    }
  }

  T removeAt(size_t index) {
    if (index >= heap_.size()) {
      VELOX_FAIL("Logical error, the removed index {} is greater than heap size {}", index, heap_.size());
    }
    T t = std::move(heap_[index]);
    // Remove from valueToIndex_
    valueToIndex_.erase(t);

    if (index == heap_.size() - 1) {
      // Removing the last element
      heap_.pop_back();
      return t;
    }

    // Move last element to the removed position
    T last = std::move(heap_.back());
    heap_.pop_back();
    heap_[index] = std::move(last);
    valueToIndex_[heap_[index]] = index;

    // Re-heapify
    percolateUp(index);
    percolateDown(index);
    return t;
  }

  void percolateUp(size_t index) {
    while (index > 0) {
      size_t parent = (index - 1) / 2;
      if (!comparator_(heap_[index], heap_[parent])) {
        break;
      }
      swapElements(index, parent);
      index = parent;
    }
  }

  void percolateDown(size_t index) {
    while (true) {
      size_t left = 2 * index + 1;
      size_t right = 2 * index + 2;
      size_t smallest = index;

      if (left < heap_.size() && comparator_(heap_[left], heap_[smallest])) {
        smallest = left;
      }
      if (right < heap_.size() && comparator_(heap_[right], heap_[smallest])) {
        smallest = right;
      }

      if (smallest == index) {
        break;
      }

      swapElements(index, smallest);
      index = smallest;
    }
  }

  void swapElements(size_t i, size_t j) {
    std::swap(heap_[i], heap_[j]);
    valueToIndex_[heap_[i]] = i;
    valueToIndex_[heap_[j]] = j;
  }

  void buildHeap() {
    // Build index map
    valueToIndex_.clear();
    for (size_t i = 0; i < heap_.size(); ++i) {
      valueToIndex_[heap_[i]] = i;
    }

    // Build heap from the bottom up
    for (int i = static_cast<int>(heap_.size()) / 2 - 1; i >= 0; --i) {
      percolateDown(static_cast<size_t>(i));
    }
  }

 private:
  std::vector<T> queue_;
  int size_ = 0;
};

} // namespace facebook::velox::stateful
