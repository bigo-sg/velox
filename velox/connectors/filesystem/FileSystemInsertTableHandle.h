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
#include "folly/dynamic.h"
#include "velox/connectors/Connector.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::filesystem {

using ConnectorInsertTableHandlePtr =
    std::shared_ptr<const ConnectorInsertTableHandle>;

class FileSystemInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  FileSystemInsertTableHandle(
      std::string tableName,
      const RowTypePtr& dataColumns,
      const std::vector<uint32_t>& partitionIndexes = {},
      const std::vector<std::string>& partitionKeys = {},
      const std::unordered_map<std::string, std::string>& tableParameters = {});

  const RowTypePtr& dataColumns() const {
    return dataColumns_;
  }

  const std::unordered_map<std::string, std::string>& tableParameters() {
    return tableParameters_;
  }

  const std::string& tableName() {
    return tableName_;
  }

  const std::vector<uint32_t> parititonIndexes() {
    return partitionIndexes_;
  }

  const std::vector<std::string> partitionKeys() {
    return partitionKeys_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ConnectorInsertTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  std::string tableName_;
  const RowTypePtr dataColumns_;
  const std::vector<uint32_t> partitionIndexes_;
  const std::vector<std::string> partitionKeys_;
  const std::unordered_map<std::string, std::string> tableParameters_;
};
} // namespace facebook::velox::connector::filesystem
