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

#include "velox/connectors/filesystem/FileSystemColumnHandle.h"
#include <boost/algorithm/string.hpp>

namespace facebook::velox::connector::filesystem {

std::string FileSystemColumnHandle::toString() const {
  std::string s = hive::HiveColumnHandle::toString();
  boost::algorithm::replace_all(
      s, "HiveColumnHandle", "FileSystemColumnHandle");
  return s;
}

folly::dynamic FileSystemColumnHandle::serialize() const {
  folly::dynamic obj = ColumnHandle::serializeBase("FileSystemColumnHandle");
  obj["fileSystemColumnHandleName"] = hive::HiveColumnHandle::name();
  obj["columnType"] = columnTypeName(hive::HiveColumnHandle::columnType());
  obj["dataType"] = hive::HiveColumnHandle::dataType()->serialize();
  folly::dynamic requiredSubfields = folly::dynamic::array;
  const std::vector<common::Subfield>& subFields =
      hive::HiveColumnHandle::requiredSubfields();
  for (const auto& subfield : subFields) {
    requiredSubfields.push_back(subfield.toString());
  }
  obj["requiredSubfields"] = requiredSubfields;
  return obj;
}

ColumnHandlePtr FileSystemColumnHandle::create(const folly::dynamic& obj) {
  auto name = obj["fileSystemColumnHandleName"].asString();
  auto columnType = columnTypeFromName(obj["columnType"].asString());
  auto dataType = ISerializable::deserialize<Type>(obj["dataType"]);
  const auto& arr = obj["requiredSubfields"];
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.reserve(arr.size());
  for (auto& s : arr) {
    requiredSubfields.emplace_back(s.asString());
  }
  return std::make_shared<FileSystemColumnHandle>(
      name, columnType, dataType, std::move(requiredSubfields));
}

void FileSystemColumnHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("FileSystemColumnHandle", FileSystemColumnHandle::create);
}
} // namespace facebook::velox::connector::filesystem