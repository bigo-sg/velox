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

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::from_elements {

using ConnectorHandlePtr = std::shared_ptr<connector::ConnectorTableHandle>;
using ConnectorInsertTableHandlePtr =
    std::shared_ptr<ConnectorInsertTableHandle>;

class FromElementsConnector : public Connector {
 public:
  FromElementsConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> /* config **/,
      folly::Executor* /* executor **/)
      : Connector(id) {}

  ConnectorMetadata* metadata() const override {
    VELOX_NYI();
  }

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const ConnectorHandlePtr& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      ConnectorInsertTableHandlePtr connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override {
    VELOX_NYI();
  }
};

} // namespace facebook::velox::connector::from_elements