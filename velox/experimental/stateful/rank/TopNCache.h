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

#include <cstddef>
#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/rank/CompactRow.h"

namespace facebook::velox::stateful {

/// Bounded LRU of per-key TopN buffers - the mirror of
/// AppendOnlyTopNFunction.kvSortedMap (guava Cache, design alignments
/// #62-#65): an intrusive doubly-linked recency list (head = most recent)
/// plus a hash index on the backend key row pointer. Write-through is the
/// caller's side of the contract: the state list is the source of truth
/// and holds every row, an entry here is a derived structure, so evicting
/// the LRU tail loses nothing a miss replay cannot rebuild (insertion
/// position through the comparator, ties in list arrival order - the
/// bufferFor lazy rebuild). The key pointer is one row of the backend-owned
/// key table; a non-empty state list entry pins that row (KeyTable
/// refcount), and the batch boundary drops empty entries (eraseEmpty)
/// before the backend reclaims key rows, so a cached key row is always a
/// live row (design alignment #76).
class TopNCache {
 public:
  using TopNBuffer = std::vector<CompactRowPtr>;

  /// 'capacity' is the key-count bound (the lruCapacity_ fold of
  /// cacheSize / rankEnd, Flink AppendOnlyTopNFunction#open); at least 1 -
  /// the caller folds with max(1, ...) before constructing.
  explicit TopNCache(size_t capacity) : capacity_(capacity) {
    VELOX_CHECK_GE(capacity_, 1, "TopNCache capacity must be at least 1");
  }

  ~TopNCache() {
    clear();
  }

  TopNCache(const TopNCache&) = delete;
  TopNCache& operator=(const TopNCache&) = delete;

  size_t capacity() const {
    return capacity_;
  }

  size_t size() const {
    return index_.size();
  }

  /// Hit: moves the entry to the most-recent end. Miss: runs 'replay' (the
  /// lazy rebuild from the state list), links the fresh buffer at the most
  /// recent end and evicts LRU tails while over capacity - insertion
  /// eviction, so the resident key count is a hard bound (design
  /// alignment #62, the native maximumSize contract with a strict bound
  /// instead of guava's approximate one).
  TopNBuffer& touchOrReplay(
      const char* key,
      const std::function<TopNBuffer()>& replay) {
    const auto it = index_.find(key);
    if (it != index_.end()) {
      unlink(it->second);
      pushFront(it->second);
      return it->second->buffer;
    }
    auto* entry = new Entry(key, replay());
    pushFront(entry);
    index_.emplace(key, entry);
    while (index_.size() > capacity_) {
      auto* victim = tail_;
      VELOX_CHECK_NOT_NULL(victim, "TopNCache eviction lost its own tail");
      unlink(victim);
      index_.erase(victim->key);
      delete victim;
    }
    return entry->buffer;
  }

  /// Drops entries whose buffer is empty (their keys hold no state rows):
  /// the batch-boundary cleanup that runs before the backend reclaims key
  /// rows - keeping an entry for a key whose row is about to die would
  /// leave a dangling key pointer (design alignment #73/#76, the buffers_
  /// empty-entry sweep of the pre-cache shape).
  void eraseEmpty() {
    Entry* entry = head_;
    while (entry != nullptr) {
      Entry* next = entry->next;
      if (entry->buffer.empty()) {
        unlink(entry);
        index_.erase(entry->key);
        delete entry;
      }
      entry = next;
    }
  }

  void clear() {
    Entry* entry = head_;
    while (entry != nullptr) {
      Entry* next = entry->next;
      delete entry;
      entry = next;
    }
    head_ = nullptr;
    tail_ = nullptr;
    index_.clear();
  }

 private:
  struct Entry {
    Entry(const char* key, TopNBuffer&& buf)
        : key(key), buffer(std::move(buf)) {}
    const char* key;
    TopNBuffer buffer;
    Entry* prev = nullptr;
    Entry* next = nullptr;
  };

  void unlink(Entry* entry) {
    if (entry->prev != nullptr) {
      entry->prev->next = entry->next;
    } else {
      head_ = entry->next;
    }
    if (entry->next != nullptr) {
      entry->next->prev = entry->prev;
    } else {
      tail_ = entry->prev;
    }
  }

  void pushFront(Entry* entry) {
    entry->prev = nullptr;
    entry->next = head_;
    if (head_ != nullptr) {
      head_->prev = entry;
    } else {
      tail_ = entry;
    }
    head_ = entry;
  }

  const size_t capacity_;
  std::unordered_map<const char*, Entry*> index_;
  Entry* head_ = nullptr;
  Entry* tail_ = nullptr;
};

} // namespace facebook::velox::stateful
