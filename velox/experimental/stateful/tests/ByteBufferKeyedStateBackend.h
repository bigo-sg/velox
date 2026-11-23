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
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <folly/Range.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/CheckpointStream.h"
#include "velox/experimental/stateful/state/KeyIndex.h"
#include "velox/experimental/stateful/state/KeyRowLifecycleObserver.h"
#include "velox/experimental/stateful/state/KeySerializer.h"
#include "velox/experimental/stateful/state/KeyTable.h"
#include "velox/experimental/stateful/state/KeyedStateBackend.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

/// Aggregating state over serialized entries, the storage of the test-only
/// ByteBufferKeyedStateBackend. The committed state is a byte map from key
/// bytes to accumulator bytes; rows() stages the entries an operator works
/// on in a RowContainer (materializing a committed entry back into a row,
/// or creating a fresh row) and commit() writes the staged rows back into
/// the map and drops them. Unlike the heap state, which commits in place,
/// this makes commit() an observable step and every row handle handed out
/// before a commit() invalid after it - the two properties the backend
/// contract test pins. The payload bytes are the heap format (one
/// [length + presto-serde bytes] frame per accumulator); the test double
/// shares the encode/decode code rather than the layout being a contract.
class ByteBufferAggregatingState
    : public AggregatingState<RowContainerStateKey, VoidNamespace> {
 public:
  ByteBufferAggregatingState(
      const AggregatingStateDescriptor& descriptor,
      std::shared_ptr<TypeSerializer<RowContainerStateKey>> keySerializer,
      KeyRowLifecycleObserver<RowContainerStateKey>* keyLifecycle)
      : staging_(std::make_unique<exec::RowContainer>(
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
        keyLifecycle_(keyLifecycle),
        valueSchemaId_(
            "presto-agg:" + spillTypesSchema(descriptor.spillTypes())) {
    // The wiring the contract asks of a state: each aggregate function
    // points at its accumulator column of the row storage.
    const auto& aggregates = descriptor.aggregates();
    for (size_t i = 0; i < aggregates.size(); ++i) {
      aggregates[i]->setAllocator(&staging_->stringAllocator());
      const auto column = staging_->columnAt(static_cast<int32_t>(i));
      aggregates[i]->setOffsets(
          column.offset(),
          column.nullByte(),
          column.nullMask(),
          column.initializedByte(),
          column.initializedMask(),
          staging_->rowSizeOffset());
    }
  }

  void rows(
      folly::Range<const RowContainerStateKey*> keys,
      const VoidNamespace& /* ns */,
      char** outRows) override {
    for (size_t i = 0; i < keys.size(); ++i) {
      outRows[i] = stagedRow(keys[i]);
    }
  }

  char* row(const RowContainerStateKey& key, const VoidNamespace& /* ns */)
      override {
    return stagedRow(key);
  }

  void remove(const RowContainerStateKey& key, const VoidNamespace& /* ns */)
      override {
    const auto keyBytes = keySerializer_->serialize(key);
    if (!hasEntry(keyBytes)) {
      return;
    }
    eraseStaged(keyBytes);
    storage_.erase(keyBytes);
    keyLifecycle_->onEntryRemoved(key);
  }

  void clear() override {
    for (const auto& keyBytes : entryKeys()) {
      eraseStaged(keyBytes);
      storage_.erase(keyBytes);
      keyLifecycle_->onEntryRemoved(keySerializer_->deserialize(keyBytes));
    }
  }

  /// Writes the staged rows into the byte map and drops the staging rows:
  /// from here on the entries live in committed form, and every handle
  /// handed out before is invalid.
  void commit() {
    for (const auto& [keyBytes, row] : staged_) {
      storage_[keyBytes] = serializeRow(row);
    }
    for (const auto& [keyBytes, row] : staged_) {
      char* rowPtr = row;
      staging_->eraseRows(folly::Range<char**>(&rowPtr, 1));
    }
    staged_.clear();
  }

  /// Appends the committed entries as one count-headed block, each entry
  /// preceded by its key group as a uint32: a restore routes an entry by
  /// that tag alone, so an entry of another key group is read past before
  /// its key is materialized (a key's key group is only known after a
  /// probe; the heap backend skips whole key-group blocks the same way). In
  /// the byte order of the map (the contract test does not rely on a
  /// particular order).
  void writeEntries(CheckpointWriter& writer) const {
    const auto countPosition = writer.writeInt32Placeholder();
    int32_t entryCount = 0;
    for (const auto& [keyBytes, payload] : storage_) {
      writer.writeUInt32(keySerializer_->deserialize(keyBytes).keyGroup());
      writeEntryFrames(writer, std::string_view(), keyBytes, payload);
      ++entryCount;
    }
    writer.patchInt32(countPosition, entryCount);
  }

  /// Reads one entry - the key-group tag and the frames are consumed either
  /// way, so the stream stays aligned - and stores it in the map as is (the
  /// payload already is the stored form) only when the tag is one of this
  /// backend's key groups. An entry of another key group is read past
  /// before its key is materialized, so no trace of it reaches the key
  /// table: a rescale restore feeds a wider stream and this backend takes
  /// its own groups out of it, as the heap backend does. A tag that
  /// disagrees with the key is corrupt. A duplicate key overwrites the
  /// earlier entry, as in Flink.
  void readEntry(
      CheckpointReader& reader,
      uint32_t backendStartKeyGroup,
      uint32_t backendNumKeyGroups) {
    const auto keyGroup = reader.readUInt32();
    const auto frames = readEntryFrames(reader);
    if (keyGroup < backendStartKeyGroup ||
        keyGroup >= backendStartKeyGroup + backendNumKeyGroups) {
      return;
    }
    const auto key = keySerializer_->deserialize(frames.key);
    VELOX_CHECK_EQ(
        key.keyGroup(),
        keyGroup,
        "Corrupt checkpoint: entry key belongs to key group {}, its tag says {}",
        key.keyGroup(),
        keyGroup);
    const std::string keyBytes(frames.key);
    const bool fresh =
        storage_.count(keyBytes) == 0 && staged_.count(keyBytes) == 0;
    storage_[keyBytes] = std::string(frames.payload);
    if (fresh) {
      keyLifecycle_->onEntryAdded(key);
    }
  }

  std::string namespaceSchemaId() const override {
    return nsSerializer_.schemaId();
  }

  std::string valueSchemaId() const override {
    return valueSchemaId_;
  }

 private:
  /// The staging row for 'key': an already staged row as is, a committed
  /// entry materialized back into a row (fresh accumulators, then the
  /// stored intermediates merged in), or a fresh row for a new entry.
  char* stagedRow(const RowContainerStateKey& key) {
    const auto keyBytes = keySerializer_->serialize(key);
    if (auto it = staged_.find(keyBytes); it != staged_.end()) {
      return it->second;
    }
    char* row = staging_->newRow();
    initializeRows_(&row, 1);
    if (auto stored = storage_.find(keyBytes); stored != storage_.end()) {
      mergeRow_(row, decodePayload(stored->second));
    } else {
      keyLifecycle_->onEntryAdded(key);
    }
    staged_.emplace(keyBytes, row);
    return row;
  }

  bool hasEntry(const std::string& keyBytes) const {
    return staged_.count(keyBytes) != 0 || storage_.count(keyBytes) != 0;
  }

  void eraseStaged(const std::string& keyBytes) {
    if (auto it = staged_.find(keyBytes); it != staged_.end()) {
      staging_->eraseRows(folly::Range<char**>(&it->second, 1));
      staged_.erase(it);
    }
  }

  /// Every key with an entry, staged or stored (or both).
  std::vector<std::string> entryKeys() const {
    std::vector<std::string> keys;
    for (const auto& [keyBytes, row] : staged_) {
      keys.push_back(keyBytes);
    }
    for (const auto& [keyBytes, payload] : storage_) {
      if (staged_.count(keyBytes) == 0) {
        keys.push_back(keyBytes);
      }
    }
    return keys;
  }

  /// Decodes a stored payload into one one-row intermediate column per
  /// accumulator; the same frames the heap state decodes on restore.
  std::vector<VectorPtr> decodePayload(std::string_view payload) {
    std::vector<VectorPtr> columns;
    columns.reserve(staging_->accumulators().size());
    size_t offset = 0;
    for (const auto& accumulator : staging_->accumulators()) {
      VELOX_CHECK_LE(
          offset + sizeof(uint32_t),
          payload.size(),
          "Corrupt entry: truncated accumulator column frame");
      const auto size =
          littleEndianValue<uint32_t>(payload.substr(offset, sizeof(uint32_t)));
      offset += sizeof(uint32_t);
      VELOX_CHECK_LE(
          offset + size,
          payload.size(),
          "Corrupt entry: truncated accumulator column frame");
      const auto data = payload.substr(offset, size);
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
    return columns;
  }

  /// Encodes one staging row: each accumulator column extracted into a
  /// one-row intermediate vector and framed as [4-byte little-endian length
  /// + presto-serde bytes], in accumulator order.
  std::string serializeRow(const char* row) {
    char* rowPtr = const_cast<char*>(row);
    const folly::Range<char**> rows(&rowPtr, 1);
    std::string payload;
    for (const auto& accumulator : staging_->accumulators()) {
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

  std::unique_ptr<exec::RowContainer> staging_;
  std::unordered_map<std::string, char*> staged_;
  std::unordered_map<std::string, std::string> storage_;
  AggregatingStateDescriptor::InitRowsCallback initializeRows_;
  AggregatingStateDescriptor::MergeRowCallback mergeRow_;
  serializer::presto::PrestoVectorSerde serde_;
  memory::MemoryPool* pool_;
  std::shared_ptr<TypeSerializer<RowContainerStateKey>> keySerializer_;
  KeyRowLifecycleObserver<RowContainerStateKey>* keyLifecycle_;
  VoidNamespaceSerializer nsSerializer_;
  const std::string valueSchemaId_;
};

/// A second keyed state backend, next to the heap one, for the backend
/// contract test: same key table, same probe path, same state interface,
/// a different storage. Entries live as bytes in a map and reach operators
/// through staging rows written back at commit(), so commit() is a real
/// step here and row handles do not survive it. Checkpoints are one flat
/// block of entry frames per state over the shared header, schema table
/// and CRC trailer - no key-group nesting (the format is this double's
/// own; the header names the stream's key-group range and every entry is
/// tagged with its key group). Restore follows the heap backend's rules:
/// the stream must overlap this backend's key-group range, and entries of
/// key groups outside it are read past rather than stored, as a rescale
/// restore feeds a wider stream.
/// Aggregating state under the void namespace only; everything else on the
/// interface is not-implemented, as on the heap backend.
class ByteBufferKeyedStateBackend : public KeyedStateBackend {
 public:
  ByteBufferKeyedStateBackend(
      std::vector<TypePtr> keyTypes,
      uint32_t maxParallelism,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups,
      memory::MemoryPool* pool)
      : keyTable_(std::make_unique<KeyTable>(
            std::move(keyTypes),
            maxParallelism,
            pool)),
        keySerializer_(keyTable_->keySerializer()),
        maxParallelism_(maxParallelism),
        startKeyGroup_(startKeyGroup),
        numKeyGroups_(numKeyGroups) {
    VELOX_CHECK(numKeyGroups > 0, "numKeyGroups must be greater than 0");
    VELOX_CHECK(
        static_cast<uint64_t>(startKeyGroup) + numKeyGroups <= maxParallelism,
        "Key-group range [{}, {}) exceeds maxParallelism {}",
        startKeyGroup,
        startKeyGroup + numKeyGroups,
        maxParallelism);
  }

  KeyIndex& keyIndex() {
    return *keyTable_;
  }

  uint32_t maxParallelism() const {
    return maxParallelism_;
  }

  std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
  getOrCreateMapState(StateDescriptor& /* stateDescriptor */) override {
    VELOX_NYI(
        "getOrCreateMapState is not available on the byte-buffer backend");
  }

  std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>>
  getOrCreateListState(StateDescriptor& /* stateDescriptor */) override {
    VELOX_NYI(
        "getOrCreateListState is not available on the byte-buffer backend");
  }

  std::shared_ptr<ValueState<int64_t, int64_t, RowVectorPtr>>
  getOrCreateValueState(StateDescriptor& /* stateDescriptor */) override {
    VELOX_NYI(
        "getOrCreateValueState is not available on the byte-buffer backend");
  }

  std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
  getOrCreateGroupValueState(StateDescriptor& /* stateDescriptor */) override {
    VELOX_NYI(
        "getOrCreateGroupValueState is not available on the byte-buffer backend");
  }

  std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
  getOrCreateGroupMapState(StateDescriptor& /* stateDescriptor */) override {
    VELOX_NYI(
        "getOrCreateGroupMapState is not available on the byte-buffer backend");
  }

  std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
  getOrCreateRankMapState(StateDescriptor& /* stateDescriptor */) override {
    VELOX_NYI(
        "getOrCreateRankMapState is not available on the byte-buffer backend");
  }

  std::shared_ptr<InternalTimerService<int64_t, int64_t>> createTimerService(
      Triggerable<int64_t, int64_t>* /* triggerable */) override {
    VELOX_NYI("createTimerService is not available on the byte-buffer backend");
  }

  std::shared_ptr<InternalTimerService<int64_t, TimeWindow>>
  createGroupWindowAggTimerService(
      Triggerable<int64_t, TimeWindow>* /* triggerable */) override {
    VELOX_NYI(
        "createGroupWindowAggTimerService is not available on the byte-buffer backend");
  }

  void snapshot(
      int64_t /* checkpointId */,
      int64_t /* timestamp */,
      CheckpointOptions /* checkpointOptions */) override {}

  void notifyCheckpointComplete(int64_t /* checkpointId */) override {}

  void notifyCheckpointAborted(int64_t /* checkpointId */) override {}

  /// Writes every state's staged rows back into its byte map.
  void commit() override {
    for (auto& [name, state] : states_) {
      state->commit();
    }
  }

  void reclaim() override {
    keyTable_->advance();
  }

  /// Same shape as the heap entry; the byte map is written for the void
  /// namespace only.
  template <typename N>
  std::shared_ptr<AggregatingState<RowContainerStateKey, N>>
  getOrCreateAggregatingState(const AggregatingStateDescriptor& descriptor) {
    static_assert(
        std::is_same_v<N, VoidNamespace>,
        "The byte-buffer backend stores aggregating state under the void namespace only");
    if (auto it = states_.find(descriptor.name()); it != states_.end()) {
      return std::static_pointer_cast<
          AggregatingState<RowContainerStateKey, N>>(it->second);
    }
    auto state = std::make_shared<ByteBufferAggregatingState>(
        descriptor, keySerializer_, keyTable_->keyLifecycle());
    states_.emplace(descriptor.name(), state);
    return state;
  }

  void snapshot(CheckpointSink& sink) override {
    commit();
    reclaim();
    std::vector<std::pair<std::string, ByteBufferAggregatingState*>> states;
    for (const auto& [name, state] : states_) {
      states.emplace_back(name, state.get());
    }
    std::sort(states.begin(), states.end(), [](const auto& a, const auto& b) {
      return a.first < b.first;
    });
    std::string bytes;
    CheckpointWriter writer(bytes);
    CheckpointHeader header;
    header.flags = kCheckpointFlagLittleEndian | kCheckpointFlagCrcTrailer;
    header.maxParallelism = maxParallelism_;
    header.startKeyGroup = startKeyGroup_;
    header.numKeyGroups = numKeyGroups_;
    writeCheckpointHeader(writer, header);
    writer.writeBytes(keySerializer_->schemaId());
    writer.writeUInt32(static_cast<uint32_t>(states.size()));
    for (const auto& [name, state] : states) {
      writer.writeBytes(name);
      writer.writeBytes(state->namespaceSchemaId());
      writer.writeBytes(state->valueSchemaId());
    }
    for (const auto& [name, state] : states) {
      writer.writeBytes(name);
      state->writeEntries(writer);
    }
    writer.writeUInt32(checkpointCrc32(bytes));
    sink.append(bytes);
  }

  void restore(CheckpointSource& source) override {
    std::string bytes;
    for (;;) {
      const auto chunk = source.next(kRestoreChunkBytes);
      if (chunk.empty()) {
        break;
      }
      bytes.append(chunk.data(), chunk.size());
    }
    CheckpointReader headerReader(bytes.data(), bytes.size());
    const auto header = readCheckpointHeader(headerReader);
    size_t bodySize = bytes.size();
    if ((header.flags & kCheckpointFlagCrcTrailer) != 0) {
      VELOX_CHECK_GE(
          bytes.size(),
          kCheckpointHeaderSize + sizeof(uint32_t),
          "Corrupt checkpoint: too short for the announced CRC trailer");
      bodySize = bytes.size() - sizeof(uint32_t);
      VELOX_CHECK_EQ(
          littleEndianValue<uint32_t>(
              std::string_view(bytes.data() + bodySize, sizeof(uint32_t))),
          checkpointCrc32(std::string_view(bytes.data(), bodySize)),
          "Corrupt checkpoint: CRC-32 mismatch");
    }
    CheckpointReader reader(bytes.data(), bodySize);
    const auto streamHeader = readCheckpointHeader(reader);
    VELOX_CHECK_EQ(
        streamHeader.maxParallelism,
        maxParallelism_,
        "Checkpoint maxParallelism {} does not match this backend's {}",
        streamHeader.maxParallelism,
        maxParallelism_);
    // The stream must overlap this backend's range, as on the heap backend:
    // this backend picks its own key groups out of the stream and reads
    // past the rest (rescale restore); no overlap at all is a mis-delivery.
    const uint64_t streamEndKeyGroup =
        static_cast<uint64_t>(streamHeader.startKeyGroup) +
        streamHeader.numKeyGroups;
    const uint64_t myStartKeyGroup = static_cast<uint64_t>(startKeyGroup_);
    const uint64_t myEndKeyGroup =
        static_cast<uint64_t>(startKeyGroup_) + numKeyGroups_;
    VELOX_CHECK(
        streamHeader.startKeyGroup < myEndKeyGroup &&
            streamEndKeyGroup > myStartKeyGroup,
        "Checkpoint key-group range [{},{}) does not overlap this backend's [{},{})",
        streamHeader.startKeyGroup,
        streamEndKeyGroup,
        myStartKeyGroup,
        myEndKeyGroup);
    VELOX_CHECK_EQ(
        reader.readBytes(),
        keySerializer_->schemaId(),
        "Checkpoint key schema does not match this backend's key serializer");
    const auto stateCount = reader.readUInt32();
    std::vector<std::string> stateIds;
    std::vector<ByteBufferAggregatingState*> statesById;
    for (uint32_t id = 0; id < stateCount; ++id) {
      const std::string stateId(reader.readBytes());
      const auto nsSchemaId = reader.readBytes();
      const auto valueSchemaId = reader.readBytes();
      auto it = states_.find(stateId);
      VELOX_CHECK(
          it != states_.end(),
          "Checkpoint contains state '{}' which is not registered",
          stateId);
      VELOX_CHECK_EQ(
          it->second->namespaceSchemaId(),
          nsSchemaId,
          "Namespace schema of state '{}' changed since the checkpoint",
          stateId);
      VELOX_CHECK_EQ(
          it->second->valueSchemaId(),
          valueSchemaId,
          "Value schema of state '{}' changed since the checkpoint",
          stateId);
      stateIds.push_back(stateId);
      statesById.push_back(it->second.get());
    }
    for (uint32_t id = 0; id < stateCount; ++id) {
      VELOX_CHECK_EQ(
          reader.readBytes(),
          stateIds[id],
          "Corrupt checkpoint: state blocks out of schema-table order");
      const auto entryCount = reader.readInt32();
      VELOX_CHECK_GE(entryCount, 0, "Corrupt checkpoint: negative entry count");
      for (int32_t i = 0; i < entryCount; ++i) {
        statesById[id]->readEntry(reader, startKeyGroup_, numKeyGroups_);
      }
    }
    VELOX_CHECK(reader.atEnd(), "Corrupt checkpoint: trailing bytes");
  }

 private:
  static constexpr size_t kRestoreChunkBytes = 4096;

  std::unique_ptr<KeyTable> keyTable_;
  std::shared_ptr<RowContainerStateKeySerializer> keySerializer_;
  const uint32_t maxParallelism_;
  const uint32_t startKeyGroup_;
  const uint32_t numKeyGroups_;
  std::unordered_map<std::string, std::shared_ptr<ByteBufferAggregatingState>>
      states_;
};

} // namespace facebook::velox::stateful
