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

#include <fmt/format.h>
#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::filesystem {

class FileSystemIndexTableHandle : public connector::ConnectorTableHandle {
 public:
  FileSystemIndexTableHandle(
      std::string connectorId,
      const std::string& tableName,
      const RowTypePtr& tableSchema,
      const std::vector<int32_t>& keyFields,
      bool asyncLookup = false,
      const std::unordered_map<std::string, std::string>& tableParameters = {})
      : ConnectorTableHandle(std::move(connectorId)),
        tableName_(tableName),
        tableSchema_(tableSchema),
        keyFields_(keyFields),
        asyncLookup_(asyncLookup),
        tableParameters_(tableParameters) {}

  ~FileSystemIndexTableHandle() override = default;

  std::string toString() const override {
    return fmt::format(
        "IndexTableHandle: tableName: {}, tableSchema: {}, asyncLookup: {}",
        tableName_,
        tableSchema_->toString(),
        asyncLookup_);
  }

  const std::string& name() const override {
    return tableName_;
  }

  const RowTypePtr keyType() {
    std::vector<std::string> keyNames;
    std::vector<TypePtr> keyTypes;
    const std::vector<std::string>& fieldNames = tableSchema_->names();
    for (size_t i = 0; i < keyFields_.size(); ++i) {
      keyNames.emplace_back(fieldNames[i]);
      keyTypes.emplace_back(tableSchema_->childAt(i));
    }
    return std::make_shared<const RowType>(
        std::move(keyNames), std::move(keyTypes));
  }

  const RowTypePtr valueType() {
    std::vector<std::string> valueNames;
    std::vector<TypePtr> valueTypes;
    const std::vector<std::string>& fieldNames = tableSchema_->names();
    for (int32_t i = 0; i < tableSchema_->children().size(); ++i) {
      if (std::find(keyFields_.begin(), keyFields_.end(), i) ==
          keyFields_.end()) {
        valueNames.emplace_back(fieldNames[i]);
        valueTypes.emplace_back(tableSchema_->childAt(i));
      }
    }
    return std::make_shared<const RowType>(
        std::move(valueNames), std::move(valueTypes));
  }

  const RowTypePtr tableSchema() {
    return tableSchema_;
  }

  std::unordered_map<std::string, std::string>& tableParameters() {
    return tableParameters_;
  }

  const std::vector<int32_t> keyFields() {
    return keyFields_;
  }

  bool supportsIndexLookup() const override {
    return true;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["tableName"] = name();
    obj["connectorId"] = connectorId();
    obj["asyncLookup"] = asyncLookup_;
    if (tableSchema_) {
      obj["tableSchema"] = tableSchema_->serialize();
    }
    folly::dynamic keyFieldsArray = folly::dynamic::array;
    for (const auto& keyField : keyFields_) {
      keyFieldsArray.push_back(keyField);
    }
    obj["keyFields"] = keyFieldsArray;
    folly::dynamic tableParameters = folly::dynamic::object;
    for (const auto& param : tableParameters_) {
      tableParameters[param.first] = param.second;
    }
    obj["tableParameters"] = tableParameters;
    return obj;
  }

  static std::shared_ptr<FileSystemIndexTableHandle> create(
      const folly::dynamic& obj,
      void* context) {
    std::string connectorId = obj["connectorId"].asString();
    std::string tableName = obj["tableName"].asString();
    bool asyncLookup = obj["asyncLookup"].asBool();
    std::vector<int32_t> keyFields;
    const auto keyFieldsArray = obj["keyFields"];
    for (const auto& item : keyFieldsArray) {
      keyFields.emplace_back(item.asInt());
    }
    RowTypePtr tableSchema;
    if (auto it = obj.find("tableSchema"); it != obj.items().end()) {
      tableSchema = ISerializable::deserialize<RowType>(it->second, context);
    }
    std::unordered_map<std::string, std::string> tableParameters{};
    const auto& tableParametersObj = obj["tableParameters"];
    for (const auto& key : tableParametersObj.keys()) {
      const auto& value = tableParametersObj[key];
      tableParameters.emplace(key.asString(), value.asString());
    }
    return std::make_shared<FileSystemIndexTableHandle>(
        connectorId,
        tableName,
        tableSchema,
        keyFields,
        asyncLookup,
        tableParameters);
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("FileSystemIndexTableHandle", create);
  }

  /// If true, we returns the lookup result asynchronously for testing purpose.
  bool asyncLookup() const {
    return asyncLookup_;
  }

 private:
  const std::string tableName_;
  const RowTypePtr tableSchema_;
  const std::vector<int32_t> keyFields_;
  const bool asyncLookup_;
  std::unordered_map<std::string, std::string> tableParameters_;
};

} // namespace facebook::velox::connector::filesystem
