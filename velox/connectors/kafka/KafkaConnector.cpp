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

#include "velox/connectors/kafka/KafkaConnector.h"
#include "velox/connectors/kafka/KafkaDataSource.h"

namespace facebook::velox::connector::kafka {

std::unique_ptr<DataSource> KafkaConnector::createDataSource(
    const RowTypePtr& outputType,
    const ConnectorHandlePtr& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& /* columnHandles **/,
    ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<KafkaDataSource>(
      outputType, tableHandle, connectorQueryCtx, config_);
}

std::unique_ptr<DataSink> KafkaConnector::createDataSink(
    RowTypePtr inputType,
    ConnectorInsertTableHandlePtr connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy) {
  VELOX_NYI();
}

} // namespace facebook::velox::connector::kafka
