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

#include <folly/Range.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateTable.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

/// AggregatingState on the heap storage: the value is a char* row of a value
/// RowContainer whose layout is derived once from the descriptor's acc
/// types. On a miss the state materializes a fresh row, initializes it
/// through the descriptor's callback (the operator wraps
/// Aggregate::initializeNewGroups over its own aggregates) and inserts it
/// into the state table. The state never calls any Aggregate method itself.
/// Relevant to Flink HeapAggregatingState.
/// @param <K> type of key, a StateKey subclass
/// @param <N> type of namespace, a Namespace subclass
template <typename K, typename N>
class HeapAggregatingState : public AggregatingState<K, N> {
 public:
  HeapAggregatingState(
      std::shared_ptr<StateTable<K, N, char*>> stateTable,
      const AggregatingStateDescriptor& descriptor,
      std::shared_ptr<TypeSerializer<K>> keySerializer)
      : stateTable_(std::move(stateTable)),
        valueRows_(std::make_unique<exec::RowContainer>(
            descriptor.accTypes(),
            descriptor.memoryPool())),
        initializeRow_(descriptor.initializeRow()),
        pool_(descriptor.memoryPool()),
        keySerializer_(std::move(keySerializer)),
        nsSerializer_(NamespaceSerializerTraits<N>::create()),
        valueSchema_(accTypesSchema(descriptor.accTypes())) {}

  void rows(folly::Range<const K*> keys, const N& ns, char** outRows) override {
    for (size_t i = 0; i < keys.size(); ++i) {
      outRows[i] = row(keys[i], ns);
    }
  }

  char* row(const K& key, const N& ns) override {
    if (char* value = stateTable_->get(key, ns)) {
      return value;
    }
    char* newRow = valueRows_->newRow();
    initializeRow_(newRow);
    stateTable_->put(key, ns, newRow);
    return newRow;
  }

  exec::RowContainer* valueRows() override {
    return valueRows_.get();
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
        writer.writeBytes(serializeRow(entry->state_));
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
    char* row = valueRows_->newRow();
    auto input = BaseVector::create(VARBINARY(), 1, pool_);
    auto* flat = input->as<FlatVector<StringView>>();
    flat->set(0, StringView(bytes.data(), bytes.size()));
    valueRows_->storeSerializedRow(*flat, 0, row);
    stateTable_->put(key, ns, row);
  }

  std::string namespaceSchema() const override {
    return nsSerializer_->schema();
  }

  std::string valueSchema() const override {
    return valueSchema_;
  }

 private:
  /// Serializes one value row through the container's row serialization
  /// (the same format as spilling), which is column-wise typed and handles
  /// fixed-width, variable-width and nested accumulator columns.
  std::string serializeRow(const char* row) {
    auto result = BaseVector::create(VARBINARY(), 1, pool_);
    char* rowPtr = const_cast<char*>(row);
    valueRows_->extractSerializedRows(folly::Range<char**>(&rowPtr, 1), result);
    const auto& value = result->as<FlatVector<StringView>>()->valueAt(0);
    return std::string(value.data(), value.size());
  }

  static std::string accTypesSchema(const std::vector<TypePtr>& accTypes) {
    std::string schema;
    for (const auto& accType : accTypes) {
      if (!schema.empty()) {
        schema += ",";
      }
      schema += accType->toString();
    }
    return "(" + schema + ")";
  }

  std::shared_ptr<StateTable<K, N, char*>> stateTable_;
  std::unique_ptr<exec::RowContainer> valueRows_;
  AggregatingStateDescriptor::InitRowCallback initializeRow_;
  memory::MemoryPool* pool_;
  std::shared_ptr<TypeSerializer<K>> keySerializer_;
  std::shared_ptr<TypeSerializer<N>> nsSerializer_;
  const std::string valueSchema_;
};
} // namespace facebook::velox::stateful
