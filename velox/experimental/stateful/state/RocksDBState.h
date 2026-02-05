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

#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/ComplexVector.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/SerializedCompositeKeyBuilder.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include <exception>
#include <memory>

namespace facebook::velox::stateful {

template<typename K, typename N, typename V>
class RocksDBState {
public:
    RocksDBState(
        rocksdb::DB& db,
        const rocksdb::ReadOptions& readOptions,
        const rocksdb::WriteOptions& writeOptions,
        rocksdb::ColumnFamilyHandle& columnFamily,
        const std::shared_ptr<stateful::TypeSerializer<K>> keySerializer,
        const std::shared_ptr<stateful::TypeSerializer<N>> namespaceSerializer,
        const std::shared_ptr<stateful::TypeSerializer<V>> valueSerializer,
        const V defaultValue,
        memory::MemoryPool* pool) 
        : db_(db),
        columnFamily_(columnFamily),
        readOptions_(readOptions),
        writeOptions_(writeOptions),
        defaultValue_(defaultValue),
        keySerializer_(keySerializer),
        namespaceSerializer_(namespaceSerializer),
        valueSerializer_(valueSerializer),
        sharedKeyAndNamespaceSerializer_(
            std::make_shared<stateful::SerializedCompositeKeyBuilder<K, N>>(keySerializer, namespaceSerializer, keyGroupPrefixBytes_, 0)),
        pool_(pool)
    {}

    std::string serializeKeyWithGroupAndNamespace(const K& key, const N& ns) {
        /// TODO: calculate keyGroup
        // int32_t keyGroupId = 0;
        // sharedKeyAndNamespaceSerializer_->setKeyAndKeyGroup(key, keyGroupId);
        return sharedKeyAndNamespaceSerializer_->buildCompositeKeyNamespace(key, ns);
    }

    void remove(const K& key) {
        try {
            db_.Delete(writeOptions_, &columnFamily_, serializeKeyWithGroupAndNamespace(key, currentNamespace_));
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to clear rocksdb, {}", e.what());
        }
    }

    V get(const K& key, const N& ns) {
        const std::string keyStr = serializeKeyWithGroupAndNamespace(key, ns);
        try {
            std::string value;
            auto status = db_.Get(readOptions_, &columnFamily_, keyStr, &value);
            if (!status.ok()) {
                return defaultValue_;
            } else {
                return valueSerializer_->deserialize(value);
            }
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to get from rocksdb:{}", e.what());
        }
    }

    void put(const K& key, const N& ns, const V& value) {
        const std::string keyStr = serializeKeyWithGroupAndNamespace(key, ns);
        const std::string valueStr = valueSerializer_->serialize(value);
        try {
            auto status = db_.Put(writeOptions_, &columnFamily_, keyStr, valueStr);
            if (!status.ok()) {
                VELOX_FAIL("Failed to put value into rocksdb, with key {}, namespace {}", key, ns);
            }
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to put into rocksdb:{}", e.what());
        }
    }

    void remove(const K& key, const N& ns) {
        const std::string keyStr = serializeKeyWithGroupAndNamespace(key, ns);
        try {
            auto status = db_.Delete(writeOptions_, &columnFamily_, keyStr);
            if (!status.ok()) {
                VELOX_FAIL("Failed to remove from rocksdb, with key {}, namespace:{}", key, ns);
            }
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to remove from rocksdb:{}", e.what());
        }
    }

protected:
    rocksdb::DB& db_;
    rocksdb::ColumnFamilyHandle& columnFamily_;
    const rocksdb::ReadOptions& readOptions_;
    const rocksdb::WriteOptions& writeOptions_;
    N currentNamespace_;
    V defaultValue_;
    int32_t keyGroupPrefixBytes_;
    const std::shared_ptr<stateful::TypeSerializer<K>> keySerializer_;
    const std::shared_ptr<stateful::TypeSerializer<N>> namespaceSerializer_;
    const std::shared_ptr<stateful::TypeSerializer<V>> valueSerializer_;
    const std::shared_ptr<stateful::SerializedCompositeKeyBuilder<K, N>> sharedKeyAndNamespaceSerializer_;
    memory::MemoryPool* pool_;
};

template<typename K, typename N, typename V>
class RocksDBValueState : public RocksDBState<K, N, V>, public ValueState<K, N, V> {
public:
    RocksDBValueState(
        rocksdb::DB& db,
        const rocksdb::ReadOptions& readOptions,
        const rocksdb::WriteOptions& writeOptions,
        rocksdb::ColumnFamilyHandle& columnFamily,
        const std::shared_ptr<stateful::TypeSerializer<K>> keySerializer,
        const std::shared_ptr<stateful::TypeSerializer<N>> namespaceSerializer,
        const std::shared_ptr<stateful::TypeSerializer<V>> valueSerializer,
        const V defaultValue,
        memory::MemoryPool* pool)
    : RocksDBState<K, N, V>(db, readOptions, writeOptions, columnFamily, keySerializer, namespaceSerializer, valueSerializer, defaultValue, pool) {}

    V value(const K& key, const N& ns) override {
        return RocksDBState<K, N, V>::get(key, ns);
    }

    void update(const K& key, const N& ns, const V& value) override {
        return RocksDBState<K, N, V>::put(key, ns, value);
    }

    void remove(const K& key, const N& ns) override {
        return RocksDBState<K, N, V>::remove(key, ns);
    }

    void clear() override {}
};

template<typename K, typename N, typename V>
class RocksDBListState : public RocksDBState<K, N, ArrayVectorPtr>, public stateful::ListState<K, N, V>{
public:
    RocksDBListState(
        rocksdb::DB& db,
        rocksdb::ReadOptions& readOptions,
        rocksdb::WriteOptions& writeOptions,
        rocksdb::ColumnFamilyHandle& columnFamily,
        std::shared_ptr<stateful::TypeSerializer<K>> keySerializer,
        std::shared_ptr<stateful::TypeSerializer<N>> namespaceSerializer,
        std::shared_ptr<stateful::TypeSerializer<ArrayVectorPtr>> valueSerializer,
        const ArrayVectorPtr defaultValue,
        memory::MemoryPool* pool)
    : RocksDBState<K, N, ArrayVectorPtr>(db, readOptions, writeOptions, columnFamily, keySerializer, namespaceSerializer, valueSerializer, defaultValue, pool) {}

    ArrayVectorPtr vectorGet(const K& key, const N& ns) override {
        return RocksDBState<K, N, ArrayVectorPtr>::get(key, ns);
    }

    void vectorUpdate(const K& key, const N& ns, const ArrayVectorPtr& vec) override {  
        RocksDBState<K, N, ArrayVectorPtr>::put(key, ns, vec);
    }

    void vectorAdd(const K& key, const N& ns, const ArrayVectorPtr& vec) override {
        ArrayVectorPtr stateVector = RocksDBState<K, N, ArrayVectorPtr>::get(key, ns);
        if (stateVector) {
            stateVector->append(vec.get());
            RocksDBState<K, N, ArrayVectorPtr>::put(key, ns, stateVector);
        } else {
            RocksDBState<K, N, ArrayVectorPtr>::put(key, ns, vec);
        }
    }

    std::vector<V> get(const K& key, const N& ns) override {
        std::vector<V> result;
        ArrayVectorPtr arrayVector = RocksDBState<K, N, ArrayVectorPtr>::get(key, ns);
        const VectorPtr& elements = arrayVector->elements();
        if (arrayVector && elements) {
            auto size = arrayVector->size();
            for (vector_size_t i = 0; i < size; ++i) {
                if (!arrayVector->isNullAt(i)) {
                    auto offset = arrayVector->offsetAt(i);
                    auto length = arrayVector->sizeAt(i);
                    for (vector_size_t j = 0; j < length; ++j) {
                        vector_size_t index = offset + j;
                        if (!elements->isNullAt(index)) {
                            result.emplace_back(elements->asFlatVector<V>()->valueAt(index));
                        }
                    }
                }
            }
        }
        return result;
    }

    void add(const K& key, const N& ns, const V& value) override {
        ArrayVectorPtr stateVector = RocksDBState<K, N, ArrayVectorPtr>::get(key, ns);
        if (stateVector) {
            FlatVector<V>* flatVector = stateVector->elements()->asFlatVector<V>();
            VELOX_CHECK(flatVector != nullptr, "FlatVector is null.");
            flatVector->resize(flatVector->size() + 1);
            flatVector->set(flatVector->size() - 1, value);
            RocksDBState<K, N, ArrayVectorPtr>::put(key, ns, stateVector);
        } else {
            const auto valueSerializer = std::dynamic_pointer_cast<ComplexVectorSerializer<ArrayVectorPtr>>(RocksDBState<K, N, ArrayVectorPtr>::valueSerializer_);
            auto type = valueSerializer->getDataType();
            VELOX_CHECK(type->kind() == TypeKind::ARRAY, "Type is not an array.");
            stateVector = std::make_shared<ArrayVector>(this->pool_, type, nullptr, 1, BufferPtr(), BufferPtr(), 0);
            stateVector->setNull(0, false);
            stateVector->elements()->asFlatVector<V>()->set(0, value);
            RocksDBState<K, N, ArrayVectorPtr>::put(key, ns, stateVector);
        }
    }

    void remove(const K& key, const N& ns) override {
        RocksDBState<K, N, ArrayVectorPtr>::remove(key, ns);
    }

    void clear() override {}
};

template<typename K, typename N, typename UK, typename UV>
class RocksDBMapState : public RocksDBState<K, N, MapVectorPtr>, public stateful::MapState<K, N, UK, UV>{
public:
    RocksDBMapState(
        rocksdb::DB& db,
        rocksdb::ReadOptions& readOptions,
        rocksdb::WriteOptions& writeOptions,
        rocksdb::ColumnFamilyHandle& columnFamily,
        std::shared_ptr<stateful::TypeSerializer<K>> keySerializer,
        std::shared_ptr<stateful::TypeSerializer<N>> namespaceSerializer,
        std::shared_ptr<stateful::TypeSerializer<MapVectorPtr>> valueSerializer,
        const MapVectorPtr defaultValue,
        memory::MemoryPool* pool)
    : RocksDBState<K, N, MapVectorPtr>(db, readOptions, writeOptions, columnFamily, keySerializer, namespaceSerializer, valueSerializer, defaultValue, pool) {}

    UV get(K key, N ns, UK userKey) override {
        MapVectorPtr mapVector = RocksDBState<K, N, MapVectorPtr>::get(key, ns);
        VELOX_CHECK(mapVector != nullptr, "MapVector is null.");
        FlatVector<UK>* keyVector = mapVector->mapKeys()->asFlatVector<UK>();
        FlatVector<UV>* valueVector = mapVector->mapValues()->asFlatVector<UV>();
        auto size = mapVector->size();
        for (vector_size_t i = 0; i < size; ++i) {
            if (!mapVector->isNullAt(i)) {
                if (keyVector->valueAt(i) == userKey) {
                    return valueVector->valueAt(i);
                }
            }
        }
        return UV();
    }

    MapVectorPtr vectorGet(const K& key, const N& ns) override {
        return RocksDBState<K, N, MapVectorPtr>::get(key, ns);
    }

    void vectorPut(const K& key, const N& ns, const MapVectorPtr& vec) override {
        RocksDBState<K, N, MapVectorPtr>::put(key, ns, vec);
    }

    void put(const K& key, const N& ns, const UK& userKey, const UV& value) override {
        MapVectorPtr mapVector = RocksDBState<K, N, MapVectorPtr>::get(key, ns);
        if (!mapVector) {
            const auto valueSerializer = std::dynamic_pointer_cast<ComplexVectorSerializer<MapVectorPtr>>(RocksDBState<K, N, MapVectorPtr>::valueSerializer_);
            auto type = valueSerializer->getDataType();
            VELOX_CHECK(type->kind() == TypeKind::MAP, "Type is not a map.");
            mapVector = std::make_shared<MapVector>(this->pool_, type, nullptr, 1, BufferPtr(), BufferPtr(), 0);
            mapVector->setNull(0, false);
            mapVector->mapKeys()->asFlatVector<UK>()->set(0, userKey);
            mapVector->mapValues()->asFlatVector<UV>()->set(0, value);
            RocksDBState<K, N, MapVectorPtr>::put(key, ns, mapVector);
            return;
        }
        FlatVector<UK>* keyVector = mapVector->mapKeys()->asFlatVector<UK>();
        FlatVector<UV>* valueVector = mapVector->mapValues()->asFlatVector<UV>();
        auto size = mapVector->size();
        for (vector_size_t i = 0; i < size; ++i) {
            if (!mapVector->isNullAt(i)) {
                if (keyVector->valueAt(i) == userKey) {
                    valueVector->set(i, value);
                    RocksDBState<K, N, MapVectorPtr>::put(key, ns, mapVector);
                    return;
                }
            }
        }
        mapVector->setNull(size, false);
        keyVector->set(size, userKey);
        valueVector->set(size, value);
        RocksDBState<K, N, MapVectorPtr>::put(key, ns, mapVector);
    }

    std::map<UK, UV> entries(const K& key, const N& ns) override {
        MapVectorPtr mapVector = RocksDBState<K, N, MapVectorPtr>::get(key, ns);
        if (mapVector) {
            FlatVector<UK>* keyVector = mapVector->mapKeys()->asFlatVector<UK>();
            FlatVector<UV>* valueVector =  mapVector->mapValues()->asFlatVector<UV>();
            std::map<UK, UV> result;
            auto size = mapVector->size();
            for (vector_size_t i = 0; i < size; ++i) {
                if (!mapVector->isNullAt(i)) {
                    result[keyVector->valueAt(i)] = valueVector->valueAt(i);
                }
            }
            return result;
        }
        return {{}};
    }

    void remove(const K& key, const N& ns, const UK& userKey) override {
        MapVectorPtr mapVector = RocksDBState<K, N, MapVectorPtr>::get(key, ns);
        if (mapVector) {
            FlatVector<UK>* keyVector = mapVector->mapKeys()->asFlatVector<UK>();
            auto size = mapVector->size();
            for (vector_size_t i = 0; i < size; ++i) {
                if (!mapVector->isNullAt(i)) {
                    if (keyVector->valueAt(i) == userKey) {
                        mapVector->setNull(i, true);
                        RocksDBState<K, N, MapVectorPtr>::put(  mapVector);
                        return;
                    }
                }
            }
        }
    }

    void clear() override {}
};

}
