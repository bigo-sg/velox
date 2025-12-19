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
#include "velox/connectors/filesystem/FileSystemDataSink.h"

namespace facebook::velox::connector::filesystem {

class FileSystemConnector : public Connector {
 public:
  FileSystemConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /* executor **/
      )
      : Connector(id), config_(config) {}

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
          columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  std::shared_ptr<IndexSource> createIndexSource(
      const RowTypePtr& inputType,
      size_t numJoinKeys,
      const std::vector<std::shared_ptr<core::IndexLookupCondition>>&
          joinConditions,
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override;

  bool canAddDynamicFilter() const override {
    return false;
  }

  bool supportsIndexLookup() const override {
    return true;
  }

  const std::shared_ptr<const config::ConfigBase>& connectorConfig()
      const override {
    return config_;
  }

  ConnectorMetadata* metadata() const override {
    VELOX_NYI();
  }

  bool supportsSplitPreload() override {
    return false;
  }

 private:
  std::shared_ptr<const config::ConfigBase> config_;
};

class FileSystemConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kFileSystemConnectorName{"FileSystem"};

  FileSystemConnectorFactory() : ConnectorFactory(kFileSystemConnectorName) {}

  explicit FileSystemConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<FileSystemConnector>(id, config, ioExecutor);
  }
};

} // namespace facebook::velox::connector::filesystem