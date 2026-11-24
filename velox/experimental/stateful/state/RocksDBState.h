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
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/HeapAggregatingState.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

/// RocksDB identity is (state name, key group, namespace, serialized key).
/// The hash chooses a key group; the complete serialized key distinguishes
/// entries even when two keys have the same hash-derived bucket.
template <typename K, typename N, typename V>
class RocksDBState {
 public:
  RocksDBState(
      rocksdb::DB& db,
      const rocksdb::ReadOptions& readOptions,
      const rocksdb::WriteOptions& writeOptions,
      rocksdb::ColumnFamilyHandle& columnFamily,
      std::shared_ptr<TypeSerializer<K>> keySerializer,
      std::shared_ptr<TypeSerializer<V>> valueSerializer,
      std::string stateName,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups)
      : db_(db),
        cf_(columnFamily),
        readOptions_(readOptions),
        writeOptions_(writeOptions),
        stateName_(std::move(stateName)),
        keySerializer_(std::move(keySerializer)),
        nsSerializer_(NamespaceSerializerTraits<N>::create()),
        valueSerializer_(std::move(valueSerializer)),
        startKeyGroup_(startKeyGroup),
        numKeyGroups_(numKeyGroups) {
    VELOX_CHECK_NOT_NULL(keySerializer_);
  }

  std::string serializeKeyWithGroupAndNamespace(
      const K& userKey,
      const N& ns) const {
    const auto group = userKey.keyGroup();
    VELOX_CHECK(
        group >= startKeyGroup_ &&
            static_cast<uint64_t>(group) <
                static_cast<uint64_t>(startKeyGroup_) + numKeyGroups_,
        "Key group {} is outside this RocksDB backend's range [{}, {})",
        group,
        startKeyGroup_,
        static_cast<uint64_t>(startKeyGroup_) + numKeyGroups_);
    auto bytes = prefix(group);
    CheckpointWriter writer(bytes);
    writer.writeBytes(nsSerializer_->serialize(ns));
    writer.writeBytes(keySerializer_->serialize(userKey));
    return bytes;
  }

  std::optional<V> getOptional(const K& userKey, const N& ns) const {
    std::string value;
    const auto status = db_.Get(
        readOptions_,
        &cf_,
        serializeKeyWithGroupAndNamespace(userKey, ns),
        &value);
    if (status.IsNotFound()) {
      return std::nullopt;
    }
    VELOX_CHECK(status.ok(), "RocksDB get failed: {}", status.ToString());
    return deserializeValue(value);
  }

  V get(const K& userKey, const N& ns) const {
    auto value = getOptional(userKey, ns);
    return value ? std::move(*value) : V{};
  }

  void put(const K& userKey, const N& ns, const V& value) const {
    const auto status = db_.Put(
        writeOptions_,
        &cf_,
        serializeKeyWithGroupAndNamespace(userKey, ns),
        serializeValue(value));
    VELOX_CHECK(status.ok(), "RocksDB put failed: {}", status.ToString());
  }

  void remove(const K& userKey, const N& ns) const {
    const auto status = db_.Delete(
        writeOptions_, &cf_, serializeKeyWithGroupAndNamespace(userKey, ns));
    VELOX_CHECK(status.ok(), "RocksDB delete failed: {}", status.ToString());
  }

  void clear() const {
    for (uint32_t group = startKeyGroup_;
         group < startKeyGroup_ + numKeyGroups_;
         ++group) {
      const auto groupPrefix = prefix(group);
      rocksdb::WriteBatch batch;
      auto iterator = std::unique_ptr<rocksdb::Iterator>(
          db_.NewIterator(readOptions_, &cf_));
      for (iterator->Seek(groupPrefix); iterator->Valid(); iterator->Next()) {
        const auto dbKey = iterator->key().ToStringView();
        if (dbKey.substr(0, groupPrefix.size()) != groupPrefix) {
          break;
        }
        batch.Delete(&cf_, iterator->key());
      }
      VELOX_CHECK(
          iterator->status().ok(),
          "RocksDB iterator failed: {}",
          iterator->status().ToString());
      const auto status = db_.Write(writeOptions_, &batch);
      VELOX_CHECK(status.ok(), "RocksDB clear failed: {}", status.ToString());
    }
  }

  void snapshotKeyGroup(int32_t group, CheckpointWriter& writer) const {
    VELOX_CHECK(
        group >= 0 && static_cast<uint32_t>(group) >= startKeyGroup_ &&
            static_cast<uint64_t>(group) <
                static_cast<uint64_t>(startKeyGroup_) + numKeyGroups_,
        "Key group {} is outside this RocksDB backend",
        group);
    const auto groupPrefix = prefix(static_cast<uint32_t>(group));
    const auto countPosition = writer.writeInt32Placeholder();
    int32_t count = 0;
    auto iterator =
        std::unique_ptr<rocksdb::Iterator>(db_.NewIterator(readOptions_, &cf_));
    for (iterator->Seek(groupPrefix); iterator->Valid(); iterator->Next()) {
      const auto dbKey = iterator->key().ToStringView();
      if (dbKey.substr(0, groupPrefix.size()) != groupPrefix) {
        break;
      }
      CheckpointReader reader(
          dbKey.data() + groupPrefix.size(), dbKey.size() - groupPrefix.size());
      const auto ns = reader.readBytes();
      const auto keyBytes = reader.readBytes();
      VELOX_CHECK(reader.atEnd(), "Corrupt RocksDB state key");
      writeEntryFrames(writer, ns, keyBytes, iterator->value().ToStringView());
      ++count;
    }
    VELOX_CHECK(
        iterator->status().ok(),
        "RocksDB iterator failed: {}",
        iterator->status().ToString());
    writer.patchInt32(countPosition, count);
  }

  void restoreEntry(int32_t group, CheckpointReader& reader) const {
    const auto frames = readEntryFrames(reader);
    const auto userKey = keySerializer_->deserialize(frames.key);
    checkEntryKeyGroup(userKey, group);
    const auto ns = nsSerializer_->deserialize(frames.ns);
    put(userKey, ns, deserializeValue(frames.payload));
  }

  std::string namespaceSchemaId() const {
    return nsSerializer_->schemaId();
  }

 protected:
  std::string serializeValue(const V& value) const {
    if constexpr (std::is_same_v<V, std::string>) {
      return valueSerializer_ ? valueSerializer_->serialize(value) : value;
    } else {
      VELOX_CHECK_NOT_NULL(valueSerializer_);
      return valueSerializer_->serialize(value);
    }
  }

  V deserializeValue(std::string_view bytes) const {
    if constexpr (std::is_same_v<V, std::string>) {
      return valueSerializer_ ? valueSerializer_->deserialize(bytes)
                              : std::string(bytes);
    } else {
      VELOX_CHECK_NOT_NULL(valueSerializer_);
      return valueSerializer_->deserialize(bytes);
    }
  }

  std::string prefix(uint32_t group) const {
    std::string bytes;
    CheckpointWriter writer(bytes);
    writer.writeBytes(stateName_);
    writer.writeUInt32(group);
    return bytes;
  }

  rocksdb::DB& db_;
  rocksdb::ColumnFamilyHandle& cf_;
  const rocksdb::ReadOptions& readOptions_;
  const rocksdb::WriteOptions& writeOptions_;
  const std::string stateName_;
  std::shared_ptr<TypeSerializer<K>> keySerializer_;
  std::shared_ptr<TypeSerializer<N>> nsSerializer_;
  std::shared_ptr<TypeSerializer<V>> valueSerializer_;
  const uint32_t startKeyGroup_;
  const uint32_t numKeyGroups_;
};

template <typename K, typename N, typename V>
class RocksDBValueState : public RocksDBState<K, N, V>,
                          public ValueState<K, N, V> {
 public:
  RocksDBValueState(
      rocksdb::DB& db,
      rocksdb::ColumnFamilyHandle& cf,
      const rocksdb::ReadOptions& readOptions,
      const rocksdb::WriteOptions& writeOptions,
      const ValueStateDescriptor<V>& descriptor,
      std::shared_ptr<TypeSerializer<K>> keySerializer,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups)
      : RocksDBState<K, N, V>(
            db,
            readOptions,
            writeOptions,
            cf,
            std::move(keySerializer),
            std::dynamic_pointer_cast<TypeSerializer<V>>(
                descriptor.serializer()),
            descriptor.name(),
            startKeyGroup,
            numKeyGroups) {
    VELOX_CHECK_NOT_NULL(this->valueSerializer_);
    VELOX_CHECK(
        (!std::is_same_v<V, StringView>),
        "StringView values cannot borrow a RocksDB read buffer");
  }

  V value(const K& key, const N& ns) override {
    return RocksDBState<K, N, V>::get(key, ns);
  }
  void update(const K& key, const N& ns, const V& value) override {
    RocksDBState<K, N, V>::put(key, ns, value);
  }
  void remove(const K& key, const N& ns) override {
    RocksDBState<K, N, V>::remove(key, ns);
  }
  void clear() override {
    RocksDBState<K, N, V>::clear();
  }
  void snapshotKeyGroup(int32_t group, CheckpointWriter& writer) override {
    RocksDBState<K, N, V>::snapshotKeyGroup(group, writer);
  }
  void restoreEntry(int32_t group, CheckpointReader& reader) override {
    RocksDBState<K, N, V>::restoreEntry(group, reader);
  }
  std::string namespaceSchemaId() const override {
    return RocksDBState<K, N, V>::namespaceSchemaId();
  }
  std::string valueSchemaId() const override {
    return this->valueSerializer_->schemaId();
  }
};

template <typename K, typename N, typename T>
class RocksDBListState : public RocksDBState<K, N, std::string>,
                         public ListState<K, N, T> {
 public:
  RocksDBListState(
      rocksdb::DB& db,
      rocksdb::ColumnFamilyHandle& cf,
      const rocksdb::ReadOptions& readOptions,
      const rocksdb::WriteOptions& writeOptions,
      const ListStateDescriptor<T>& descriptor,
      std::shared_ptr<TypeSerializer<K>> keySerializer,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups)
      : RocksDBState<K, N, std::string>(
            db,
            readOptions,
            writeOptions,
            cf,
            std::move(keySerializer),
            nullptr,
            descriptor.name(),
            startKeyGroup,
            numKeyGroups),
        serializer_(
            std::dynamic_pointer_cast<TypeSerializer<T>>(
                descriptor.serializer())) {
    VELOX_CHECK_NOT_NULL(serializer_);
    VELOX_CHECK(
        (!std::is_same_v<T, StringView>),
        "StringView list elements cannot borrow a RocksDB read buffer");
  }

  std::vector<T> get(const K& key, const N& ns) override {
    auto bytes = RocksDBState<K, N, std::string>::getOptional(key, ns);
    if (!bytes) {
      return {};
    }
    CheckpointReader reader(bytes->data(), bytes->size());
    const auto size = reader.readInt32();
    VELOX_CHECK_GE(size, 0, "Corrupt RocksDB list size");
    std::vector<T> result;
    result.reserve(size);
    for (int32_t i = 0; i < size; ++i) {
      result.push_back(serializer_->deserialize(reader.readBytes()));
    }
    VELOX_CHECK(reader.atEnd(), "Corrupt RocksDB list payload");
    return result;
  }
  void add(const K& key, const N& ns, const T& value) override {
    auto values = get(key, ns);
    values.push_back(value);
    std::string bytes;
    CheckpointWriter writer(bytes);
    writer.writeInt32(static_cast<int32_t>(values.size()));
    for (const auto& element : values) {
      writer.writeBytes(serializer_->serialize(element));
    }
    RocksDBState<K, N, std::string>::put(key, ns, bytes);
  }
  void remove(const K& key, const N& ns) override {
    RocksDBState<K, N, std::string>::remove(key, ns);
  }
  void clear() override {
    RocksDBState<K, N, std::string>::clear();
  }
  void snapshotKeyGroup(int32_t group, CheckpointWriter& writer) override {
    RocksDBState<K, N, std::string>::snapshotKeyGroup(group, writer);
  }
  void restoreEntry(int32_t group, CheckpointReader& reader) override {
    RocksDBState<K, N, std::string>::restoreEntry(group, reader);
  }
  std::string namespaceSchemaId() const override {
    return RocksDBState<K, N, std::string>::namespaceSchemaId();
  }
  std::string valueSchemaId() const override {
    return "list<" + serializer_->schemaId() + ">";
  }

 private:
  std::shared_ptr<TypeSerializer<T>> serializer_;
};

template <typename K, typename N, typename UK, typename UV>
class RocksDBMapState : public RocksDBState<K, N, std::string>,
                        public MapState<K, N, UK, UV> {
 public:
  RocksDBMapState(
      rocksdb::DB& db,
      rocksdb::ColumnFamilyHandle& cf,
      const rocksdb::ReadOptions& readOptions,
      const rocksdb::WriteOptions& writeOptions,
      const MapStateDescriptor<UK, UV>& descriptor,
      std::shared_ptr<TypeSerializer<K>> keySerializer,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups)
      : RocksDBState<K, N, std::string>(
            db,
            readOptions,
            writeOptions,
            cf,
            std::move(keySerializer),
            nullptr,
            descriptor.name(),
            startKeyGroup,
            numKeyGroups),
        keySerializer_(
            std::dynamic_pointer_cast<TypeSerializer<UK>>(
                descriptor.keySerializer())),
        valueSerializer_(
            std::dynamic_pointer_cast<TypeSerializer<UV>>(
                descriptor.valueSerializer())) {
    VELOX_CHECK_NOT_NULL(keySerializer_);
    VELOX_CHECK_NOT_NULL(valueSerializer_);
    VELOX_CHECK(
        (!std::is_same_v<UK, StringView> && !std::is_same_v<UV, StringView>),
        "StringView map entries cannot borrow a RocksDB read buffer");
  }

  std::optional<UV> get(const K& key, const N& ns, const UK& userKey) override {
    auto values = entries(key, ns);
    auto it = values.find(userKey);
    return it == values.end() ? std::nullopt : std::optional<UV>(it->second);
  }
  void put(const K& key, const N& ns, const UK& userKey, const UV& value)
      override {
    auto values = entries(key, ns);
    values[userKey] = value;
    write(key, ns, values);
  }
  std::map<UK, UV> entries(const K& key, const N& ns) override {
    auto bytes = RocksDBState<K, N, std::string>::getOptional(key, ns);
    if (!bytes) {
      return {};
    }
    CheckpointReader reader(bytes->data(), bytes->size());
    const auto size = reader.readInt32();
    VELOX_CHECK_GE(size, 0, "Corrupt RocksDB map size");
    std::map<UK, UV> result;
    for (int32_t i = 0; i < size; ++i) {
      auto userKey = keySerializer_->deserialize(reader.readBytes());
      auto userValue = valueSerializer_->deserialize(reader.readBytes());
      result.emplace(std::move(userKey), std::move(userValue));
    }
    VELOX_CHECK(reader.atEnd(), "Corrupt RocksDB map payload");
    return result;
  }
  void remove(const K& key, const N& ns, const UK& userKey) override {
    auto values = entries(key, ns);
    if (values.erase(userKey) == 0) {
      return;
    }
    if (values.empty()) {
      RocksDBState<K, N, std::string>::remove(key, ns);
    } else {
      write(key, ns, values);
    }
  }
  void clear() override {
    RocksDBState<K, N, std::string>::clear();
  }
  void snapshotKeyGroup(int32_t group, CheckpointWriter& writer) override {
    RocksDBState<K, N, std::string>::snapshotKeyGroup(group, writer);
  }
  void restoreEntry(int32_t group, CheckpointReader& reader) override {
    RocksDBState<K, N, std::string>::restoreEntry(group, reader);
  }
  std::string namespaceSchemaId() const override {
    return RocksDBState<K, N, std::string>::namespaceSchemaId();
  }
  std::string valueSchemaId() const override {
    return "map<" + keySerializer_->schemaId() + "," +
        valueSerializer_->schemaId() + ">";
  }

 private:
  void write(const K& key, const N& ns, const std::map<UK, UV>& values) {
    std::string bytes;
    CheckpointWriter writer(bytes);
    writer.writeInt32(static_cast<int32_t>(values.size()));
    for (const auto& [userKey, userValue] : values) {
      writer.writeBytes(keySerializer_->serialize(userKey));
      writer.writeBytes(valueSerializer_->serialize(userValue));
    }
    RocksDBState<K, N, std::string>::put(key, ns, bytes);
  }

  std::shared_ptr<TypeSerializer<UK>> keySerializer_;
  std::shared_ptr<TypeSerializer<UV>> valueSerializer_;
};

/// The operator mutates accumulator rows through returned pointers. Cache
/// rows for one batch, flush them using the heap spill codec, then release
/// the cache so RocksDB remains the durable storage.
template <typename K, typename N>
class RocksDBAggregatingState : public RocksDBState<K, N, std::string>,
                                public AggregatingState<K, N> {
 public:
  RocksDBAggregatingState(
      rocksdb::DB& db,
      rocksdb::ColumnFamilyHandle& cf,
      const rocksdb::ReadOptions& readOptions,
      const rocksdb::WriteOptions& writeOptions,
      const AggregatingStateDescriptor& descriptor,
      std::shared_ptr<TypeSerializer<K>> keySerializer,
      KeyRowLifecycleObserver<K>* lifecycle,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups)
      : RocksDBState<K, N, std::string>(
            db,
            readOptions,
            writeOptions,
            cf,
            keySerializer,
            nullptr,
            descriptor.name(),
            startKeyGroup,
            numKeyGroups),
        heap_(
            std::make_shared<StateTable<K, N, char*>>(
                startKeyGroup,
                numKeyGroups,
                lifecycle),
            descriptor,
            this->keySerializer_) {}

  void rows(folly::Range<const K*> keys, const N& ns, char** outRows) override {
    for (const auto& key : keys) {
      load(key, ns);
    }
    heap_.rows(keys, ns, outRows);
  }
  char* row(const K& key, const N& ns) override {
    load(key, ns);
    return heap_.row(key, ns);
  }
  exec::RowContainer* valueRows() override {
    return heap_.valueRows();
  }
  void remove(const K& key, const N& ns) override {
    if (loaded_.erase(
            this->serializeKeyWithGroupAndNamespace(key, ns)) != 0) {
      heap_.remove(key, ns);
    }
    RocksDBState<K, N, std::string>::remove(key, ns);
  }
  void clear() override {
    loaded_.clear();
    heap_.clear();
    RocksDBState<K, N, std::string>::clear();
  }

  void flush() {
    for (uint32_t group = this->startKeyGroup_;
         group < this->startKeyGroup_ + this->numKeyGroups_;
         ++group) {
      std::string bytes;
      CheckpointWriter writer(bytes);
      heap_.snapshotKeyGroup(static_cast<int32_t>(group), writer);
      CheckpointReader reader(bytes.data(), bytes.size());
      const auto count = reader.readInt32();
      for (int32_t i = 0; i < count; ++i) {
        const auto frames = readEntryFrames(reader);
        const auto key = this->keySerializer_->deserialize(frames.key);
        const auto ns = this->nsSerializer_->deserialize(frames.ns);
        RocksDBState<K, N, std::string>::put(
            key, ns, std::string(frames.payload));
      }
      VELOX_CHECK(reader.atEnd(), "Corrupt in-memory accumulator snapshot");
    }
    heap_.clear();
    loaded_.clear();
  }

  void snapshotKeyGroup(int32_t group, CheckpointWriter& writer) override {
    RocksDBState<K, N, std::string>::snapshotKeyGroup(group, writer);
  }
  void restoreEntry(int32_t group, CheckpointReader& reader) override {
    const auto frames = readEntryFrames(reader);
    const auto key = this->keySerializer_->deserialize(frames.key);
    checkEntryKeyGroup(key, group);
    const auto ns = this->nsSerializer_->deserialize(frames.ns);
    RocksDBState<K, N, std::string>::put(
        key, ns, std::string(frames.payload));
    if (loaded_.count(this->serializeKeyWithGroupAndNamespace(key, ns)) != 0) {
      std::string bytes;
      CheckpointWriter writer(bytes);
      writeEntryFrames(writer, frames.ns, frames.key, frames.payload);
      CheckpointReader reload(bytes.data(), bytes.size());
      heap_.restoreEntry(group, reload);
    }
  }
  std::string namespaceSchemaId() const override {
    return RocksDBState<K, N, std::string>::namespaceSchemaId();
  }
  std::string valueSchemaId() const override {
    return heap_.valueSchemaId();
  }

 private:
  void load(const K& key, const N& ns) {
    if (!loaded_.insert(this->serializeKeyWithGroupAndNamespace(key, ns))
             .second) {
      return;
    }
    if (auto payload = RocksDBState<K, N, std::string>::getOptional(key, ns)) {
      std::string bytes;
      CheckpointWriter writer(bytes);
      writer.writeBytes(this->nsSerializer_->serialize(ns));
      writer.writeBytes(this->keySerializer_->serialize(key));
      writer.writeBytes(*payload);
      CheckpointReader reader(bytes.data(), bytes.size());
      heap_.restoreEntry(static_cast<int32_t>(key.keyGroup()), reader);
    }
  }

  HeapAggregatingState<K, N> heap_;
  std::unordered_set<std::string> loaded_;
};

} // namespace facebook::velox::stateful
