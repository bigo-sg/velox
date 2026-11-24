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
#include "velox/experimental/stateful/state/RocksDBStateBackend.h"

#include <cstdint>
#include <memory>
#include <unordered_map>

#include <folly/json/dynamic.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "velox/experimental/stateful/KeyedStateSpec.h"
#include "velox/experimental/stateful/state/RocksDBKeyedStateBackend.h"
#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/experimental/stateful/state/StateKey.h"

namespace facebook::velox::stateful {

std::string RocksDBStateBackend::getName() const {
  return "rocksdb";
}

std::shared_ptr<KeyedStateBackend>
RocksDBStateBackend::createKeyedStateBackend(
    const KeyedStateSpec& spec,
    memory::MemoryPool* pool) {
  const auto rocksParameters =
      std::dynamic_pointer_cast<const RocksDBKeyedStateBackendParameters>(
          parameters_);
  VELOX_CHECK_NOT_NULL(
      rocksParameters, "RocksDB backend requires RocksDB parameters");
  VELOX_CHECK_NOT_NULL(pool);
  return std::make_shared<RocksDBKeyedStateBackend<RowContainerStateKey>>(
      spec.keyTypes(), *rocksParameters, pool);
}

folly::dynamic RocksDBStateBackend::serialize() const {
  folly::dynamic obj;
  return obj;
}

RocksDBKeyedStateBackendParameters::RocksDBKeyedStateBackendParameters(
    const StateBackendType backendType,
    const std::string jobId,
    const std::string operatorId,
    const int64_t dbHandle,
    const int64_t readOptionHandle,
    const int64_t writeOptionHandle,
    const std::set<std::string>& states,
    const std::unordered_map<std::string, int64_t>& columnFamilies,
    const std::unordered_map<std::string, std::string>& stateOperators,
    const std::unordered_map<std::string, TypePtr>& stateKeys,
    const std::unordered_map<std::string, TypePtr>& stateNamespaces,
    const std::unordered_map<std::string, TypePtr>& stateValues,
    uint32_t maxParallelism,
    uint32_t startKeyGroup,
    uint32_t numKeyGroups)
    : KeyedStateBackendParameters(
          backendType,
          jobId,
          operatorId,
          maxParallelism,
          startKeyGroup,
          numKeyGroups),
      dbHandle_(dbHandle),
      readOptionHandle_(readOptionHandle),
      writeOptionHandle_(writeOptionHandle),
      states_(states),
      stateOperators_(stateOperators),
      columnFamilyHandles_(columnFamilies),
      stateKeys_(stateKeys),
      stateValues_(stateValues),
      stateNamespaces_(stateNamespaces) {}

rocksdb::DB* RocksDBKeyedStateBackendParameters::getDB() const {
  rocksdb::DB* db = reinterpret_cast<rocksdb::DB*>(dbHandle_);
  VELOX_CHECK(
      db != nullptr, "Failed to convert rocksdb native handle: {}", dbHandle_);
  return db;
}

const rocksdb::ReadOptions* RocksDBKeyedStateBackendParameters::getReadOptions()
    const {
  rocksdb::ReadOptions* readOptions =
      reinterpret_cast<rocksdb::ReadOptions*>(readOptionHandle_);
  VELOX_CHECK(
      readOptions != nullptr,
      "Failed to convert rocksdb read options: {}",
      readOptionHandle_);
  return readOptions;
}

const rocksdb::WriteOptions*
RocksDBKeyedStateBackendParameters::getWriteOptions() const {
  rocksdb::WriteOptions* writeOptions =
      reinterpret_cast<rocksdb::WriteOptions*>(writeOptionHandle_);
  VELOX_CHECK(
      writeOptions != nullptr,
      "Failed to convert rocksdb write options: {}",
      writeOptionHandle_);
  return writeOptions;
}

std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*>
RocksDBKeyedStateBackendParameters::getColumnFamilies() const {
  std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> cfs;
  for (const auto& [name, cf] : columnFamilyHandles_) {
    rocksdb::ColumnFamilyHandle* columnFamily =
        reinterpret_cast<rocksdb::ColumnFamilyHandle*>(cf);
    cfs[name] = columnFamily;
  }
  return cfs;
}

const std::set<std::string>& RocksDBKeyedStateBackendParameters::getStates()
    const {
  return states_;
}

const std::unordered_map<std::string, std::string>&
RocksDBKeyedStateBackendParameters::getStateOperators() const {
  return stateOperators_;
}

const std::unordered_map<std::string, TypePtr>&
RocksDBKeyedStateBackendParameters::getStateKeys() const {
  return stateKeys_;
}

const std::unordered_map<std::string, TypePtr>&
RocksDBKeyedStateBackendParameters::getStateNamespaces() const {
  return stateNamespaces_;
}

const std::unordered_map<std::string, TypePtr>&
RocksDBKeyedStateBackendParameters::getStateValues() const {
  return stateValues_;
}

folly::dynamic RocksDBKeyedStateBackendParameters::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["jobId"] = getJobId();
  obj["operatorId"] = getOperatorIdentifier();
  obj["stateBackendType"] = static_cast<int32_t>(getBackendType());
  obj["maxParallelism"] = getMaxParallelism();
  obj["startKeyGroup"] = getStartKeyGroup();
  obj["numKeyGroups"] = getNumKeyGroups();
  obj["dbHandle"] = dbHandle_;
  obj["readOptionHandle"] = readOptionHandle_;
  obj["writeOptionHandle"] = writeOptionHandle_;
  folly::dynamic states = folly::dynamic::array();
  for (const auto& state : states_) {
    states.push_back(state);
  }
  obj["states"] = states;
  folly::dynamic stateOperators = folly::dynamic::object;
  folly::dynamic stateColumnFamilies = folly::dynamic::object;
  folly::dynamic stateKeys = folly::dynamic::object;
  folly::dynamic stateNamespaces = folly::dynamic::object;
  folly::dynamic stateValues = folly::dynamic::object;
  for (const auto& [state, op] : stateOperators_) {
    stateOperators[state] = op;
  }
  for (const auto& [state, cf] : columnFamilyHandles_) {
    stateColumnFamilies[state] = cf;
  }
  for (const auto& [state, key] : stateKeys_) {
    stateKeys[state] = key->serialize();
  }
  for (const auto& [state, ns] : stateNamespaces_) {
    stateNamespaces[state] = ns->serialize();
  }
  for (const auto& [state, value] : stateValues_) {
    stateValues[state] = value->serialize();
  }
  obj["stateOperators"] = stateOperators;
  obj["columnFamilies"] = stateColumnFamilies;
  obj["stateKeys"] = stateKeys;
  obj["stateNamespaces"] = stateNamespaces;
  obj["stateValues"] = stateValues;
  return obj;
}

std::shared_ptr<const RocksDBKeyedStateBackendParameters>
RocksDBKeyedStateBackendParameters::create(
    const folly::dynamic& obj,
    void* context) {
  const StateBackendType backendType =
      static_cast<StateBackendType>(obj["stateBackendType"].asInt());
  const std::string jobId = obj["jobId"].asString();
  const std::string operatorId = obj["operatorId"].asString();
  const int64_t dbHandle = obj["dbHandle"].asInt();
  const int64_t dbReadOptionHandle = obj["readOptionHandle"].asInt();
  const int64_t dbWriteOptionHandle = obj["writeOptionHandle"].asInt();
  std::set<std::string> states;
  for (const auto& state : obj["states"]) {
    states.emplace(state.asString());
  }
  std::unordered_map<std::string, std::string> stateOperators;
  std::unordered_map<std::string, int64_t> columnFamilies;
  std::unordered_map<std::string, TypePtr> stateKeys;
  std::unordered_map<std::string, TypePtr> stateValues;
  std::unordered_map<std::string, TypePtr> stateNamespaces;
  for (const auto& stateOp : obj["stateOperators"].items()) {
    stateOperators[stateOp.first.asString()] = stateOp.second.asString();
  }
  for (const auto& stateCF : obj["columnFamilies"].items()) {
    columnFamilies[stateCF.first.asString()] = stateCF.second.asInt();
  }
  for (const auto& stateKey : obj["stateKeys"].items()) {
    stateKeys[stateKey.first.asString()] = Type::create(stateKey.second);
  }
  for (const auto& stateNamespace : obj["stateNamespaces"].items()) {
    stateNamespaces[stateNamespace.first.asString()] =
        Type::create(stateNamespace.second);
  }
  for (const auto& stateValue : obj["stateValues"].items()) {
    stateValues[stateValue.first.asString()] = Type::create(stateValue.second);
  }
  return std::make_shared<const RocksDBKeyedStateBackendParameters>(
      backendType,
      jobId,
      operatorId,
      dbHandle,
      dbReadOptionHandle,
      dbWriteOptionHandle,
      states,
      columnFamilies,
      stateOperators,
      stateKeys,
      stateNamespaces,
      stateValues,
      obj.count("maxParallelism")
          ? static_cast<uint32_t>(obj["maxParallelism"].asInt())
          : kDefaultMaxParallelism,
      obj.count("startKeyGroup")
          ? static_cast<uint32_t>(obj["startKeyGroup"].asInt())
          : 0,
      obj.count("numKeyGroups")
          ? static_cast<uint32_t>(obj["numKeyGroups"].asInt())
          : kDefaultMaxParallelism);
}

void RocksDBKeyedStateBackendParameters::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("RocksDBKeyedStateBackendParameters", create);
}

} // namespace facebook::velox::stateful
