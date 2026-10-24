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
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "velox/common/memory/ByteStream.h"
#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/state/CheckpointStream.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateTable.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

/// AggregatingState on the heap storage: the value is a char* row of a value
/// RowContainer in accumulator layout, built from the descriptor's
/// accumulator array so that aggregate functions read and write their slots
/// in place (the same layout HashAggregation rows use). On a miss the state
/// materializes a fresh row, inserts it into the state table, and constructs
/// the accumulators through the descriptor's init callback — one batched
/// call per rows() batch of misses (the operator wraps
/// Aggregate::initializeNewGroups over its own aggregates). The state never
/// calls any Aggregate method itself. Relevant to Flink
/// HeapAggregatingState.
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
            // No key or dependent columns: keys live in the backend's key
            // table and the value rows host nothing but accumulators.
            std::vector<TypePtr>{},
            true,
            descriptor.accumulators(),
            std::vector<TypePtr>{},
            false,
            false,
            false,
            false,
            descriptor.memoryPool())),
        initializeRows_(descriptor.initializeRows()),
        mergeRow_(descriptor.mergeRow()),
        pool_(descriptor.memoryPool()),
        keySerializer_(std::move(keySerializer)),
        nsSerializer_(NamespaceSerializerTraits<N>::create()),
        valueSchemaId_(
            "presto-agg:" + spillTypesSchema(descriptor.spillTypes())) {}

  void rows(folly::Range<const K*> keys, const N& ns, char** outRows) override {
    std::vector<char*> newRows;
    for (size_t i = 0; i < keys.size(); ++i) {
      if (char* value = stateTable_->get(keys[i], ns)) {
        outRows[i] = value;
        continue;
      }
      char* row = valueRows_->newRow();
      stateTable_->put(keys[i], ns, row);
      outRows[i] = row;
      newRows.push_back(row);
    }
    if (!newRows.empty()) {
      initializeRows_(newRows.data(), newRows.size());
    }
  }

  char* row(const K& key, const N& ns) override {
    if (char* value = stateTable_->get(key, ns)) {
      return value;
    }
    char* newRow = valueRows_->newRow();
    initializeRows_(&newRow, 1);
    stateTable_->put(key, ns, newRow);
    return newRow;
  }

  exec::RowContainer* valueRows() override {
    return valueRows_.get();
  }

  void remove(const K& key, const N& ns) override {
    // The accumulator row lives in the value container, not in the state
    // table: return it before dropping the entry (a miss removes nothing).
    if (char* valueRow = stateTable_->get(key, ns)) {
      valueRows_->eraseRows(folly::Range<char**>(&valueRow, 1));
    }
    stateTable_->remove(key, ns);
  }

  void clear() override {
    // The table clear reports every dropped entry to the key-row
    // bookkeeping; the container clear then frees all accumulator rows.
    stateTable_->clear();
    valueRows_->clear();
  }

  // Synchronous snapshots only: entries are serialized inline while the
  // caller guarantees no concurrent mutation. The copy-on-write machinery
  // in StateMap does not copy values (see the deep-copy TODO in
  // StateMap::get), so an asynchronous snapshot could observe partially
  // updated values; enabling one requires implementing the value copy
  // first.
  void snapshotKeyGroup(int32_t keyGroupId, CheckpointWriter& writer) override {
    auto& map =
        stateTable_->stateMapForKeyGroup(static_cast<uint32_t>(keyGroupId));
    auto snapshot = map.createSnapshot();
    const auto countPosition = writer.writeInt32Placeholder();
    int32_t entryCount = 0;
    for (const auto& head : snapshot.heads) {
      for (auto entry = head; entry != nullptr; entry = entry->next_) {
        writeEntryFrames(
            writer,
            nsSerializer_->serialize(entry->namespace_),
            keySerializer_->serialize(entry->key_),
            serializeRow(entry->state_));
        ++entryCount;
      }
    }
    writer.patchInt32(countPosition, entryCount);
    map.releaseSnapshot(snapshot.version);
  }

  void restoreEntry(int32_t keyGroupId, CheckpointReader& reader) override {
    const auto frames = readEntryFrames(reader);
    const auto ns = nsSerializer_->deserialize(frames.ns);
    const auto key = keySerializer_->deserialize(frames.key);
    checkEntryKeyGroup(key, keyGroupId);
    // Decode the payload into one-row intermediate columns, then rebuild the
    // row exactly like a spilled-input merge: construct fresh accumulators,
    // then merge the intermediates in (there is no byte-level inverse —
    // accumulator slots hold live objects).
    std::vector<VectorPtr> columns;
    columns.reserve(valueRows_->accumulators().size());
    size_t offset = 0;
    for (const auto& accumulator : valueRows_->accumulators()) {
      VELOX_CHECK_LE(
          offset + sizeof(uint32_t),
          frames.payload.size(),
          "Corrupt checkpoint: truncated accumulator column frame");
      const auto size = littleEndianValue<uint32_t>(
          frames.payload.substr(offset, sizeof(uint32_t)));
      offset += sizeof(uint32_t);
      VELOX_CHECK_LE(
          offset + size,
          frames.payload.size(),
          "Corrupt checkpoint: truncated accumulator column frame");
      const auto data = frames.payload.substr(offset, size);
      offset += size;
      ByteRange range{
          reinterpret_cast<uint8_t*>(const_cast<char*>(data.data())),
          static_cast<int64_t>(size),
          0};
      BufferInputStream stream(std::vector<ByteRange>{{range}});
      VectorPtr column;
      serde_.deserializeSingleColumn(
          &stream, pool_, accumulator.spillType(), &column, nullptr);
      columns.push_back(std::move(column));
    }
    char* row = valueRows_->newRow();
    initializeRows_(&row, 1);
    mergeRow_(row, columns);
    // Last write wins on a duplicate restore, as in Flink; the row the put
    // replaces goes back to the value container instead of leaking.
    if (char* valueRow = stateTable_->get(key, ns)) {
      valueRows_->eraseRows(folly::Range<char**>(&valueRow, 1));
    }
    stateTable_->put(key, ns, row);
  }

  std::string namespaceSchemaId() const override {
    return nsSerializer_->schemaId();
  }

  std::string valueSchemaId() const override {
    return valueSchemaId_;
  }

 private:
  /// Serializes one value row: each accumulator column is extracted into a
  /// one-row intermediate vector (Accumulator::extractForSpill — the same
  /// extraction spilling uses) and framed as [4-byte little-endian length +
  /// presto-serde bytes], concatenated in accumulator order.
  std::string serializeRow(const char* row) {
    char* rowPtr = const_cast<char*>(row);
    const folly::Range<char**> rows(&rowPtr, 1);
    std::string payload;
    for (const auto& accumulator : valueRows_->accumulators()) {
      // Like SpillerBase::extractSpill: the extraction requires a
      // pre-allocated result vector of the accumulator's spill type.
      VectorPtr intermediate =
          BaseVector::create(accumulator.spillType(), rows.size(), pool_);
      accumulator.extractForSpill(rows, intermediate);
      std::ostringstream output;
      serde_.serializeSingleColumn(intermediate, nullptr, pool_, &output);
      const auto bytes = output.str();
      payload += littleEndianBytes(static_cast<uint32_t>(bytes.size()));
      payload += bytes;
    }
    return payload;
  }

  static std::string spillTypesSchema(const std::vector<TypePtr>& spillTypes) {
    std::string schema;
    for (const auto& spillType : spillTypes) {
      if (!schema.empty()) {
        schema += ",";
      }
      schema += spillType->toString();
    }
    return "(" + schema + ")";
  }

  std::shared_ptr<StateTable<K, N, char*>> stateTable_;
  std::unique_ptr<exec::RowContainer> valueRows_;
  AggregatingStateDescriptor::InitRowsCallback initializeRows_;
  AggregatingStateDescriptor::MergeRowCallback mergeRow_;
  serializer::presto::PrestoVectorSerde serde_;
  memory::MemoryPool* pool_;
  std::shared_ptr<TypeSerializer<K>> keySerializer_;
  std::shared_ptr<TypeSerializer<N>> nsSerializer_;
  const std::string valueSchemaId_;
};
} // namespace facebook::velox::stateful
