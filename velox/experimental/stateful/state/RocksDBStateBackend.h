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
#include <set>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include "velox/experimental/stateful/state/StateBackend.h"

namespace facebook::velox::stateful {

class RocksDBKeyedStateBackendParameters;

class RocksDBStateBackend : public StateBackend {
public:
    RocksDBStateBackend(const std::shared_ptr<const RocksDBKeyedStateBackendParameters> params)
    : StateBackend(std::dynamic_pointer_cast<const KeyedStateBackendParameters>(params)) {}

    std::string getName() const override;

    folly::dynamic serialize() const override;

    std::shared_ptr<KeyedStateBackend> createKeyedStateBackend() override;
};

class RocksDBKeyedStateBackendParameters : public KeyedStateBackendParameters {
public:
    RocksDBKeyedStateBackendParameters(
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
        const std::unordered_map<std::string, TypePtr>& stateValues);

    rocksdb::DB* getDB() const;

    const rocksdb::ReadOptions* getReadOptions() const;

    const rocksdb::WriteOptions* getWriteOptions() const;

    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> getColumnFamilies() const;

    const std::set<std::string>& getStates() const;

    const std::unordered_map<std::string, std::string>& getStateOperators() const;

    const std::unordered_map<std::string, TypePtr>& getStateKeys() const;

    const std::unordered_map<std::string, TypePtr>& getStateNamespaces() const;

    const std::unordered_map<std::string, TypePtr>& getStateValues() const;

    folly::dynamic serialize() const override;

    static std::shared_ptr<const RocksDBKeyedStateBackendParameters> create(const folly::dynamic& obj, void* context);

    static void registerSerDe();

private:
    int64_t dbHandle_;
    int64_t readOptionHandle_;
    int64_t writeOptionHandle_;
    /// The list of state names, related to the states have been registered in rocksdb state backend. It's used to justify 
    /// the given state name is valid or not.
    std::set<std::string> states_;
    /// The map between state and operators, it's used to justify the relationship between the state and the operator. 
    /// In Flink, the state is binded to operator one-to-one.
    std::unordered_map<std::string, std::string> stateOperators_;
    std::unordered_map<std::string, int64_t> columnFamilyHandles_;
    std::unordered_map<std::string, TypePtr> stateKeys_;
    std::unordered_map<std::string, TypePtr> stateValues_;
    std::unordered_map<std::string, TypePtr> stateNamespaces_;
};

}
