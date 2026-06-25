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

#include "velox/connectors/filesystem/FileSystemInsertTableHandle.h"

namespace facebook::velox::connector::filesystem {

FileSystemInsertTableHandle::FileSystemInsertTableHandle(
    std::string tableName,
    const RowTypePtr& dataColumns,
    const std::vector<uint32_t>& partitionIndexes,
    const std::vector<std::string>& partitionKeys,
    const std::unordered_map<std::string, std::string>& tableParameters)
    : tableName_(tableName),
      dataColumns_(dataColumns),
      partitionIndexes_(partitionIndexes),
      partitionKeys_(partitionKeys),
      tableParameters_(tableParameters) {
  VELOX_CHECK(
      partitionKeys_.size() == partitionIndexes_.size(),
      "Partition keys' size must euqals to Partition indexes' size");
}

std::string FileSystemInsertTableHandle::toString() const {
  std::stringstream out;
  out << "table: " << tableName_;
  if (dataColumns_) {
    out << ", data columns: " << dataColumns_->toString();
  }
  if (!partitionIndexes_.empty()) {
    out << ", partition indexes: " << folly::join(", ", partitionIndexes_);
  }
  if (!partitionKeys_.empty()) {
    out << ", partition keys: " << folly::join(", ", partitionKeys_);
  }
  if (!tableParameters_.empty()) {
    out << ", table parameters: [";
    bool firstParam = true;
    for (const auto& param : tableParameters_) {
      if (!firstParam) {
        out << ", ";
      }
      out << param.first << ":" << param.second;
      firstParam = false;
    }
    out << "]";
  }
  return out.str();
}

folly::dynamic FileSystemInsertTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["tableName"] = tableName_;
  if (dataColumns_) {
    obj["dataColumns"] = dataColumns_->serialize();
  }
  folly::dynamic partitionIndexArray = folly::dynamic::array;
  for (const auto& pIndex : partitionIndexes_) {
    partitionIndexArray.push_back(pIndex);
  }
  obj["partitionIndexes"] = partitionIndexArray;
  folly::dynamic partitionKeyArray = folly::dynamic::array;
  for (const auto& pKey : partitionKeys_) {
    partitionKeyArray.push_back(pKey);
  }
  obj["partitionKeys"] = partitionKeyArray;
  folly::dynamic tableParameters = folly::dynamic::object;
  for (const auto& param : tableParameters_) {
    tableParameters[param.first] = param.second;
  }
  obj["tableParameters"] = tableParameters;
  return obj;
}

ConnectorInsertTableHandlePtr FileSystemInsertTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  auto tableName = obj["tableName"].asString();
  RowTypePtr dataColumns;
  if (auto it = obj.find("dataColumns"); it != obj.items().end()) {
    dataColumns = ISerializable::deserialize<RowType>(it->second, context);
  }
  std::vector<std::string> partitionKeys;
  if (auto it = obj.find("partitionKeys"); it != obj.items().end()) {
    const auto& partitionKeysArray = obj["partitionKeys"];
    for (const auto& item : partitionKeysArray) {
      partitionKeys.emplace_back(item.asString());
    }
  }
  std::vector<uint32_t> partitionIndexes;
  if (auto it = obj.find("partitionIndexes"); it != obj.items().end()) {
    const auto& partitionIndexArray = obj["partitionIndexes"];
    for (const auto& item : partitionIndexArray) {
      partitionIndexes.emplace_back(item.asInt());
    }
  }
  std::unordered_map<std::string, std::string> tableParameters{};
  if (auto it = obj.find("tableParameters"); it != obj.items().end()) {
    const auto& tableParametersObj = obj["tableParameters"];
    for (const auto& key : tableParametersObj.keys()) {
      const auto& value = tableParametersObj[key];
      tableParameters.emplace(key.asString(), value.asString());
    }
  }
  return std::make_shared<const FileSystemInsertTableHandle>(
      tableName, dataColumns, partitionIndexes, partitionKeys, tableParameters);
}

void FileSystemInsertTableHandle::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("FileSystemInsertTableHandle", create);
}

} // namespace facebook::velox::connector::filesystem
