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

#include "velox/connectors/from_elements/FromElementsTableHandle.h"
#include <folly/json/dynamic.h>

namespace facebook::velox::connector::from_elements {

std::string FromElementsTableHandle::toString() const {
  std::stringstream out;
  out << "table: " << tableName_;
  if (dataColumns_) {
    out << ", data columns: " << dataColumns_->toString();
  }
  out << ", data:" ;
  for (size_t i = 0; i < data_.size(); ++i) {
    out << data_[i];
    if (i != data_.size() - 1) {
      out << ",";
    }
  }
  return out.str();
}

folly::dynamic FromElementsTableHandle::serialize() const {
  folly::dynamic obj = ConnectorTableHandle::serializeBase("FromElementsTableHandle");
  obj["tableName"] = tableName_;
  if (dataColumns_) {
    obj["dataColumns"] = dataColumns_->serialize();
  }
  folly::dynamic dataArray = folly::dynamic::array;
  for (size_t i = 0; i < data_.size(); ++i) {
    dataArray[i] = data_[i];
  }
  obj["data"] = dataArray;
  return obj;
}

ConnectorTableHandlePtr FromElementsTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  auto connectorId = obj["connectorId"].asString();
  auto tableName = obj["tableName"].asString();
  RowTypePtr dataColumns;
  if (auto it = obj.find("dataColumns"); it != obj.items().end()) {
    dataColumns = ISerializable::deserialize<RowType>(it->second, context);
  }
  auto dataArray = obj["data"];
  std::vector<std::string> data;
  for (auto item : dataArray) {
    data.emplace_back(item.asString());
  }
  return std::make_shared<const FromElementsTableHandle>(
      connectorId, tableName, dataColumns, data);
}

void FromElementsTableHandle::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("FromElementsTableHandle", create);
}

} // namespace facebook::velox::connector::from_elements
