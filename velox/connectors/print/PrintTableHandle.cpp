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

#include "velox/connectors/print/PrintTableHandle.h"

namespace facebook::velox::connector::print {

PrintTableHandle::PrintTableHandle(
    std::string tableName,
    const RowTypePtr& dataColumns,
    const std::string& printIdentifier,
    bool isStdErr)
    : tableName_(tableName),
      dataColumns_(dataColumns),
      printIdentifier_(printIdentifier),
      isStdErr_(isStdErr) {}

std::string PrintTableHandle::toString() const {
  std::stringstream out;
  out << "table: " << tableName_;
  if (dataColumns_) {
    out << ", data columns: " << dataColumns_->toString();
  }
  out << ", printIdentifier: " << printIdentifier_
      << ", isStdErr: " << (isStdErr_ ? "true" : "false");
  return out.str();
}

folly::dynamic PrintTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "PrintTableHandle";
  obj["tableName"] = tableName_;
  if (dataColumns_) {
    obj["dataColumns"] = dataColumns_->serialize();
  }
  obj["printIdentifier"] = printIdentifier_;
  obj["isStdErr"] = isStdErr_;
  return obj;
}

ConnectorInsertTableHandlePtr PrintTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  auto tableName = obj["tableName"].asString();
  RowTypePtr dataColumns;
  if (auto it = obj.find("dataColumns"); it != obj.items().end()) {
    dataColumns = ISerializable::deserialize<RowType>(it->second, context);
  }
  std::string printIdentifier;
  if (auto it = obj.find("printIdentifier"); it != obj.items().end()) {
    printIdentifier = it->second.asString();
  }
  bool isStdErr = false;
  if (auto it = obj.find("isStdErr"); it != obj.items().end()) {
    isStdErr = it->second.asBool();
  }
  return std::make_shared<const PrintTableHandle>(
      tableName, dataColumns, printIdentifier, isStdErr);
}

void PrintTableHandle::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("PrintTableHandle", create);
}

} // namespace facebook::velox::connector::print
