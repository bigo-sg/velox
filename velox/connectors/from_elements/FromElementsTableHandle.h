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

#include <folly/dynamic.h>
#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::from_elements {

class FromElementsTableHandle : public ConnectorTableHandle {
 public:
  FromElementsTableHandle(
      std::string connectorId,
      std::string tableName,
      const RowTypePtr& dataColumns,
      const std::vector<std::string>& data)
      : ConnectorTableHandle(connectorId),
        tableName_(tableName),
        dataColumns_(dataColumns),
        data_(data) {}

  const std::string tableName() {
    return tableName_;
  }

  const RowTypePtr& dataColumns() {
    return dataColumns_;
  }

  const std::vector<std::string> data() {
    return data_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  const std::string tableName_;
  const RowTypePtr dataColumns_;
  const std::vector<std::string> data_;
};

} // namespace facebook::velox::connector::from_elements
