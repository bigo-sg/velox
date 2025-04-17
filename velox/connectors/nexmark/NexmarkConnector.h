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

#include "velox/common/config/Config.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/nexmark/NexmarkConnectorSplit.h"
#include "velox/connectors/nexmark/NexmarkGenerator.h"

namespace facebook::velox::connector::nexmark {

/// `NexmarkConnector` is a connector that generates data on-the-fly for
/// Flink nexmark benchmark.
///
/// NexmarkConnectorSplit lets clients specify how many rows are expected to be
/// generated.
class NexmarkTableHandle : public ConnectorTableHandle {
 public:
  explicit NexmarkTableHandle(std::string connectorId, GeneratorConfig config)
      : ConnectorTableHandle(std::move(connectorId)),
        config_(std::move(config)) {}

  ~NexmarkTableHandle() override = default;

  std::string toString() const override {
    return "nexmark-table";
  }

  folly::dynamic serialize() const override;

  static ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

  const GeneratorConfig config_;
};

class NexmarkDataSource : public DataSource {
 public:
  NexmarkDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      velox::memory::MemoryPool* pool);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<common::Filter>& /*filter*/) override {
    VELOX_NYI("Dynamic filters not supported by NexmarkConnector.");
  }

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    // TODO: Which stats do we want to expose here?
    return {};
  }

 private:
  const RowTypePtr outputType_;
  std::unique_ptr<NexmarkGenerator> nexmarkGenerator_;

  // The current split being processed.
  std::shared_ptr<NexmarkConnectorSplit> currentSplit_;

  // How many rows were generated for this split.
  uint64_t splitOffset_{0};
  uint64_t splitEnd_{0};

  size_t completedRows_{0};
  size_t completedBytes_{0};

  memory::MemoryPool* pool_;
};

class NexmarkConnector final : public Connector {
 public:
  NexmarkConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /*executor*/)
      : Connector(id) {}

  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& /*columnHandles*/,
      ConnectorQueryCtx* connectorQueryCtx) override final {
    return std::make_unique<NexmarkDataSource>(
        outputType, tableHandle, connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI("NexmarkConnector does not support data sink.");
  }
};

class NexmarkConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kNexmarkConnectorName{"nexmark"};

  NexmarkConnectorFactory() : ConnectorFactory(kNexmarkConnectorName) {}

  explicit NexmarkConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<NexmarkConnector>(id, config, ioExecutor);
  }
};

} // namespace facebook::velox::connector::nexmark
