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

#include <memory>
#include <utility>
#include <vector>

#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateTable.h"

namespace facebook::velox::stateful {

/// ListState on the heap storage: the value is a shared vector, created on
/// the first add and removed state reads as an empty list, as in Flink
/// HeapListState. The checkpoint payload of one entry is the element count
/// followed by the length-prefixed serialization of each element.
/// @param <K> type of key, a StateKey subclass
/// @param <N> type of namespace, a Namespace subclass
/// @param <T> type of the list elements
template <typename K, typename N, typename T>
class HeapListState : public ListState<K, N, T> {
 public:
  HeapListState(
      std::shared_ptr<StateTable<K, N, std::shared_ptr<std::vector<T>>>>
          stateTable,
      const ListStateDescriptor<T>& descriptor,
      std::shared_ptr<TypeSerializer<K>> keySerializer)
      : stateTable_(std::move(stateTable)),
        keySerializer_(std::move(keySerializer)),
        nsSerializer_(NamespaceSerializerTraits<N>::create()),
        elementSerializer_(std::dynamic_pointer_cast<TypeSerializer<T>>(
            descriptor.serializer())) {
    VELOX_CHECK_NOT_NULL(
        elementSerializer_,
        "List state '{}' requires a serializer for its element type",
        descriptor.name());
  }

  std::vector<T> get(const K& key, const N& ns) override {
    auto currentList = stateTable_->get(key, ns);
    return currentList == nullptr ? std::vector<T>{} : *currentList;
  }

  void add(const K& key, const N& ns, const T& value) override {
    getOrCreate(key, ns)->push_back(value);
  }

  void remove(const K& key, const N& ns) override {
    stateTable_->remove(key, ns);
  }

  void clear() override {
    stateTable_->clear();
  }

  void snapshotKeyGroup(int32_t keyGroupId, CheckpointWriter& writer) override {
    auto& map =
        stateTable_->stateMapForKeyGroup(static_cast<uint32_t>(keyGroupId));
    auto snapshot = map.createSnapshot();
    const auto countPosition = writer.writeInt32Placeholder();
    int32_t entryCount = 0;
    for (const auto& head : snapshot.heads) {
      for (auto entry = head; entry != nullptr; entry = entry->next_) {
        writer.writeBytes(nsSerializer_->serialize(entry->namespace_));
        writer.writeBytes(keySerializer_->serialize(entry->key_));
        writer.writeBytes(serializeList(entry->state_));
        ++entryCount;
      }
    }
    writer.patchInt32(countPosition, entryCount);
    map.releaseSnapshot(snapshot.version);
  }

  void restoreEntry(CheckpointReader& reader) override {
    const auto ns = nsSerializer_->deserialize(reader.readBytes());
    const auto key = keySerializer_->deserialize(reader.readBytes());
    const auto bytes = reader.readBytes();
    CheckpointReader listReader(bytes.data(), bytes.size());
    const auto size = listReader.readInt32();
    VELOX_CHECK_GE(size, 0, "Corrupt checkpoint: negative list size {}", size);
    auto list = std::make_shared<std::vector<T>>();
    list->reserve(size);
    for (int32_t i = 0; i < size; ++i) {
      list->push_back(elementSerializer_->deserialize(listReader.readBytes()));
    }
    VELOX_CHECK(
        listReader.atEnd(),
        "Corrupt checkpoint: trailing bytes in a list payload");
    stateTable_->put(key, ns, list);
  }

  std::string namespaceSchema() const override {
    return nsSerializer_->schema();
  }

  std::string valueSchema() const override {
    return "list<" + elementSerializer_->schema() + ">";
  }

 private:
  std::string serializeList(const std::shared_ptr<std::vector<T>>& list) const {
    VELOX_CHECK_NOT_NULL(list, "Corrupt state: a stored list is null");
    std::string blob;
    CheckpointWriter listWriter(blob);
    listWriter.writeInt32(static_cast<int32_t>(list->size()));
    for (const auto& element : *list) {
      listWriter.writeBytes(elementSerializer_->serialize(element));
    }
    return blob;
  }

  std::shared_ptr<std::vector<T>> getOrCreate(const K& key, const N& ns) {
    if (auto currentList = stateTable_->get(key, ns)) {
      return currentList;
    }
    auto freshList = std::make_shared<std::vector<T>>();
    stateTable_->put(key, ns, freshList);
    return freshList;
  }

  std::shared_ptr<StateTable<K, N, std::shared_ptr<std::vector<T>>>>
      stateTable_;
  std::shared_ptr<TypeSerializer<K>> keySerializer_;
  std::shared_ptr<TypeSerializer<N>> nsSerializer_;
  std::shared_ptr<TypeSerializer<T>> elementSerializer_;
};
} // namespace facebook::velox::stateful
