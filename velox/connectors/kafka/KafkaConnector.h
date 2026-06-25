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
#include "velox/connectors/kafka/KafkaConfig.h"
#include "velox/connectors/kafka/KafkaDataSource.h"

namespace facebook::velox::connector::kafka {

using ConnectorHandlePtr = std::shared_ptr<connector::ConnectorTableHandle>;
using ConnectorInsertTableHandlePtr =
    std::shared_ptr<ConnectorInsertTableHandle>;

/// The kafka connector.
class KafkaConnector : public Connector {
 public:
  KafkaConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /* executor **/)
      : Connector(id), config_(std::make_shared<ConnectionConfig>(config)) {}

  const std::shared_ptr<const config::ConfigBase>& connectorConfig()
      const override {
    return config_->getConfig();
  }

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
      CommitStrategy commitStrategy) override;

 private:
  const ConnectionConfigPtr config_;
};

class KafkaConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kKafkaConnectorName{"Kafka"};

  KafkaConnectorFactory() : ConnectorFactory(kKafkaConnectorName) {}

  explicit KafkaConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<KafkaConnector>(id, config, ioExecutor);
  }
};

} // namespace facebook::velox::connector::kafka
