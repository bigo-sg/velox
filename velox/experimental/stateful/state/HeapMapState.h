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

#include <map>
#include <memory>
#include <utility>

#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateTable.h"

namespace facebook::velox::stateful {

/// MapState on the heap storage: the value is a shared std::map, created on
/// the first put, overwritten on put of an existing user key and read as an
/// empty map when absent, as in Flink HeapMapState. Removing from an absent
/// map does not materialize one. The checkpoint payload of one entry is the
/// user-entry count followed by the length-prefixed serialization of each
/// user key and user value.
/// @param <K> type of key, a StateKey subclass
/// @param <N> type of namespace, a Namespace subclass
/// @param <UK> type of the user keys of the map
/// @param <UV> type of the user values of the map
template <typename K, typename N, typename UK, typename UV>
class HeapMapState : public MapState<K, N, UK, UV> {
 public:
  HeapMapState(
      std::shared_ptr<StateTable<K, N, std::shared_ptr<std::map<UK, UV>>>>
          stateTable,
      const MapStateDescriptor<UK, UV>& descriptor,
      std::shared_ptr<TypeSerializer<K>> keySerializer)
      : stateTable_(std::move(stateTable)),
        keySerializer_(std::move(keySerializer)),
        nsSerializer_(NamespaceSerializerTraits<N>::create()),
        userKeySerializer_(std::dynamic_pointer_cast<TypeSerializer<UK>>(
            descriptor.keySerializer())),
        userValueSerializer_(std::dynamic_pointer_cast<TypeSerializer<UV>>(
            descriptor.valueSerializer())) {
    VELOX_CHECK_NOT_NULL(
        userKeySerializer_,
        "Map state '{}' requires a serializer for its user key type",
        descriptor.name());
    VELOX_CHECK_NOT_NULL(
        userValueSerializer_,
        "Map state '{}' requires a serializer for its user value type",
        descriptor.name());
  }

  UV get(const K& key, const N& ns, const UK& userKey) override {
    auto currentMap = stateTable_->get(key, ns);
    if (currentMap == nullptr) {
      return UV();
    }
    auto it = currentMap->find(userKey);
    return it == currentMap->end() ? UV() : it->second;
  }

  void put(const K& key, const N& ns, const UK& userKey, const UV& value)
      override {
    (*getOrCreate(key, ns))[userKey] = value;
  }

  std::map<UK, UV> entries(const K& key, const N& ns) override {
    auto currentMap = stateTable_->get(key, ns);
    return currentMap == nullptr ? std::map<UK, UV>{} : *currentMap;
  }

  void remove(const K& key, const N& ns, const UK& userKey) override {
    if (auto currentMap = stateTable_->get(key, ns)) {
      currentMap->erase(userKey);
    }
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
        writer.writeBytes(serializeMap(entry->state_));
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
    CheckpointReader mapReader(bytes.data(), bytes.size());
    const auto size = mapReader.readInt32();
    VELOX_CHECK_GE(size, 0, "Corrupt checkpoint: negative map size {}", size);
    auto map = std::make_shared<std::map<UK, UV>>();
    for (int32_t i = 0; i < size; ++i) {
      auto userKey = userKeySerializer_->deserialize(mapReader.readBytes());
      auto userValue = userValueSerializer_->deserialize(mapReader.readBytes());
      map->emplace(std::move(userKey), std::move(userValue));
    }
    VELOX_CHECK(
        mapReader.atEnd(),
        "Corrupt checkpoint: trailing bytes in a map payload");
    stateTable_->put(key, ns, map);
  }

  std::string namespaceSchema() const override {
    return nsSerializer_->schema();
  }

  std::string valueSchema() const override {
    return "map<" + userKeySerializer_->schema() + "," +
        userValueSerializer_->schema() + ">";
  }

 private:
  std::string serializeMap(const std::shared_ptr<std::map<UK, UV>>& map) const {
    VELOX_CHECK_NOT_NULL(map, "Corrupt state: a stored map is null");
    std::string blob;
    CheckpointWriter mapWriter(blob);
    mapWriter.writeInt32(static_cast<int32_t>(map->size()));
    for (const auto& [userKey, userValue] : *map) {
      mapWriter.writeBytes(userKeySerializer_->serialize(userKey));
      mapWriter.writeBytes(userValueSerializer_->serialize(userValue));
    }
    return blob;
  }

  std::shared_ptr<std::map<UK, UV>> getOrCreate(const K& key, const N& ns) {
    if (auto currentMap = stateTable_->get(key, ns)) {
      return currentMap;
    }
    auto freshMap = std::make_shared<std::map<UK, UV>>();
    stateTable_->put(key, ns, freshMap);
    return freshMap;
  }

  std::shared_ptr<StateTable<K, N, std::shared_ptr<std::map<UK, UV>>>>
      stateTable_;
  std::shared_ptr<TypeSerializer<K>> keySerializer_;
  std::shared_ptr<TypeSerializer<N>> nsSerializer_;
  std::shared_ptr<TypeSerializer<UK>> userKeySerializer_;
  std::shared_ptr<TypeSerializer<UV>> userValueSerializer_;
};
} // namespace facebook::velox::stateful
