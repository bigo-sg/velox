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
#include <vector>

namespace facebook::velox::stateful {

// This class is relevent to flink InternalPriorityQueue.
template<typename T>
class InternalPriorityQueue {
 public:
  virtual bool add(T toAdd) = 0;

  virtual T poll() = 0;

  virtual T peek() = 0;

  virtual void clear() = 0;

  virtual bool empty() const = 0;

  virtual void remove(T toRemove) = 0;
};

// This class is relevant to flink HeapPriorityQueue.
// TODO: need to make it equal to flink
template<typename T>
class HeapPriorityQueue : public InternalPriorityQueue<T> {
 public:
  HeapPriorityQueue() {
    queue_.resize(1024); // Initial capacity, can be adjusted
  }

  bool add(T toAdd) override {
    // Implementation for adding to the priority queue set
    queue_[size_] = toAdd;
    size_++;
    if (size_ >= queue_.size()) {
      size_ = 0;
    }
    return true;
  }

  T poll() override {
    // Implementation for polling from the priority queue set
    size_--;
    if (size_ < 0) {
      size_ = queue_.size() - 1;
    }
    return queue_[size_];
  }

  T peek() override {
    int index = size_ - 1;
    if (index < 0) {
      index = queue_.size() - 1;
    }
    return queue_[index];
  }

  void clear() override {
    queue_.clear();
    size_ = 0;
  }

  bool empty() const override {
    return size_ == 0;
  }

  void remove(T toRemove) override {
    // Implementation for removing an element from the priority queue set
    auto it = std::find(queue_.begin(), queue_.end(), toRemove);
    if (it != queue_.end()) {
      *it = queue_[size_ - 1]; // Replace with the last element
      size_--;
      if (size_ < 0) {
        size_ = queue_.size() - 1;
      }
    }
  }
 private:
  std::vector<T> queue_;
  int size_ = 0;
};

// This class is relevant to flink HeapPriorityQueueSet.
template<typename T, typename H, typename C>
class HeapPriorityQueueSet : public HeapPriorityQueue<T> {

public:
  HeapPriorityQueueSet() : HeapPriorityQueue<T>() {
    deduplicationMapsByKeyGroup.resize(1);
  }

  T poll() override {
    T t = HeapPriorityQueue<T>::poll();
    std::unordered_map<T, T, H, C>& deduplicateMap = getDedupMapForElement(t);
    auto it = deduplicateMap.find(t);
    if (it != deduplicateMap.end()) {
      deduplicateMap.erase(it);
    }
    return t;
  }

  bool add(T t) override {
    std::unordered_map<T, T, H, C>& deduplicateMap = getDedupMapForElement(t);
    if (deduplicateMap.find(t) == deduplicateMap.end()) {
      HeapPriorityQueue<T>::add(t);
      deduplicateMap.insert({t, t});
      return true;
    } else {
      return false;
    }
  }

  void remove(T t) override {
    std::unordered_map<T, T, H, C>& deduplicateMap = getDedupMapForElement(t);
    auto it = deduplicateMap.find(t);
    if (it != deduplicateMap.end()) {
      deduplicateMap.erase(it);
      HeapPriorityQueue<T>::remove(t);
    }
  }

  void clear() override {
    HeapPriorityQueue<T>::clear();
    for (auto& map : deduplicationMapsByKeyGroup) {
      map.clear();
    }
  }
private:
  mutable std::vector<std::unordered_map<T, T, H, C>> deduplicationMapsByKeyGroup;
  // KeyExtractorFunction<T> keyExtrator;
  // KeyGroupRange keyGroupRange

  //TODO: get deDuplicationMap by keyGroupRange
  std::unordered_map<T, T, H, C>& getDedupMapForElement(T element) {
    return deduplicationMapsByKeyGroup[0];
  }
};
} // namespace facebook::velox::stateful
