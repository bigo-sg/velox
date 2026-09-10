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

#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateTable.h"

namespace facebook::velox::stateful {

/// ValueState on the heap storage. The value V is stored directly in the
/// state table, so V must be a nullable pointer type (raw or smart): the
/// state table signals a miss with nullptr. Relevant to Flink
/// HeapValueState.
/// @param <K> type of key, a StateKey subclass
/// @param <N> type of namespace, a Namespace subclass
/// @param <V> type of value, a nullable pointer type
template <typename K, typename N, typename V>
class HeapValueState : public ValueState<K, N, V> {
 public:
  HeapValueState(
      std::shared_ptr<StateTable<K, N, V>> stateTable,
      const ValueStateDescriptor<V>& descriptor,
      std::shared_ptr<TypeSerializer<K>> keySerializer)
      : stateTable_(std::move(stateTable)),
        keySerializer_(std::move(keySerializer)),
        nsSerializer_(NamespaceSerializerTraits<N>::create()),
        valueSerializer_(std::dynamic_pointer_cast<TypeSerializer<V>>(
            descriptor.serializer())) {
    VELOX_CHECK_NOT_NULL(
        valueSerializer_,
        "Value state '{}' requires a serializer for its value type",
        descriptor.name());
  }

  V value(const K& key, const N& ns) override {
    return stateTable_->get(key, ns);
  }

  void update(const K& key, const N& ns, const V& value) override {
    stateTable_->put(key, ns, value);
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
        writer.writeBytes(valueSerializer_->serialize(entry->state_));
        ++entryCount;
      }
    }
    writer.patchInt32(countPosition, entryCount);
    map.releaseSnapshot(snapshot.version);
  }

  void restoreEntry(CheckpointReader& reader) override {
    const auto ns = nsSerializer_->deserialize(reader.readBytes());
    const auto key = keySerializer_->deserialize(reader.readBytes());
    const auto value = valueSerializer_->deserialize(reader.readBytes());
    stateTable_->put(key, ns, value);
  }

  std::string namespaceSchema() const override {
    return nsSerializer_->schema();
  }

  std::string valueSchema() const override {
    return valueSerializer_->schema();
  }

 private:
  std::shared_ptr<StateTable<K, N, V>> stateTable_;
  std::shared_ptr<TypeSerializer<K>> keySerializer_;
  std::shared_ptr<TypeSerializer<N>> nsSerializer_;
  std::shared_ptr<TypeSerializer<V>> valueSerializer_;
};
} // namespace facebook::velox::stateful
