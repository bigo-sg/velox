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

#include "velox/common/memory/Memory.h"
#include "velox/connectors/filesystem/FileSystemConnector.h"
#include "velox/connectors/filesystem/FileSystemDataSink.h"
#include "velox/connectors/filesystem/FileSystemInsertTableHandle.h"
#include "velox/dwio/text/RegisterTextWriter.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::filesystem::test {

class FileSystemConnectorTestBase : public exec::test::OperatorTestBase {
 protected:
  static constexpr const char* kFileNamePrefixKey = "file-name-prefix";
  static constexpr const char* kFileNameSuffixKey = "file-name-suffix";
  static constexpr const char* kTaskIdKey = "task-id";

 public:
  const std::string format = "csv";
  const std::string fileNamePrefix = "abcdefg";
  const std::string fileNameSuffix = "1234567";
  const std::string taskId = "0";
  const std::string fileSystemConnectorId = "test-filesystem";
  const std::vector<std::string> inputValues0 = {
      "2025-07-11",
      "14",
      "axxx11",
      "bxxx11"};
  const std::vector<std::string> inputValues1 = {
      "2025-07-11",
      "15",
      "axxx12",
      "bxxx12"};

  std::string dataPath = "";
  const std::shared_ptr<RowType> inputType = createInputType();
  memory::MemoryPool& rootPool = memory::memoryManager()->testingDefaultRoot();
  const std::shared_ptr<memory::MemoryPool> memoryPool =
      memory::memoryManager()->addLeafPool();
  const std::shared_ptr<filesystems::FileSystem> fs_;

  void SetUp() override {
    OperatorTestBase::SetUp();
    dwio::common::registerFileSinks();
    filesystems::registerLocalFileSystem();
    text::registerTextWriterFactory();
    init();
    connector::registerConnectorFactory(
        std::make_shared<connector::filesystem::FileSystemConnectorFactory>());
    std::unordered_map<std::string, std::string> configMap;
    configMap[connector::filesystem::FileSystemWriteConfig::kFormat] = format;
    configMap[connector::filesystem::FileSystemWriteConfig::kPath] = dataPath;
    configMap[kFileNamePrefixKey] = fileNamePrefix;
    configMap[kFileNameSuffixKey] = fileNameSuffix;
    configMap[kTaskIdKey] = taskId;

    std::shared_ptr<const config::ConfigBase> config =
        std::make_shared<const config::ConfigBase>(std::move(configMap));
    auto fileSystemConnector =
        connector::getConnectorFactory(
            connector::filesystem::FileSystemConnectorFactory::
                kFileSystemConnectorName)
            ->newConnector(fileSystemConnectorId, config);
    connector::registerConnector(fileSystemConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(fileSystemConnectorId);
    connector::unregisterConnectorFactory(
        connector::filesystem::FileSystemConnectorFactory::
            kFileSystemConnectorName);
    OperatorTestBase::TearDown();
  }

  void init() {
    char* currentPath;
    if ((currentPath = getcwd(NULL, 0)) == NULL) {
      VELOX_FAIL("Failed to get curent path");
    }
    std::string writeDir = "data";
    char writePath[strlen(currentPath) + writeDir.size() + 8];
    sprintf(
        writePath, "%s%s%s%s", "file://", currentPath, "/", writeDir.data());
    dataPath =
        std::string(writePath, strlen(currentPath) + writeDir.size() + 8);
  }

  const std::shared_ptr<connector::ConnectorQueryCtx> createQueryCtx() {
    const auto filesystemConnector = getConnector(fileSystemConnectorId);
    const auto connectorConfig = filesystemConnector->connectorConfig();
    std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx =
        std::make_shared<connector::ConnectorQueryCtx>(
            memoryPool.get(),
            &rootPool,
            connectorConfig.get(),
            nullptr,
            common::PrefixSortConfig(),
            nullptr,
            nullptr,
            "sink.filesystem",
            "task.filesystem",
            "planNodeId.filesystem",
            0,
            "UTC");
    return connectorQueryCtx;
  }

  static const std::shared_ptr<RowType> createInputType() {
    std::vector<std::string> outputFieldNames = {"dt", "hm", "a", "b"};
    std::vector<TypePtr> outputFieldTypes = {
        std::make_shared<const VarcharType>(),
        std::make_shared<const VarcharType>(),
        std::make_shared<const VarcharType>(),
        std::make_shared<const VarcharType>()};
    return std::make_shared<RowType>(
        std::move(outputFieldNames), std::move(outputFieldTypes));
  }

  const void insertData(
      const std::vector<std::string>& data,
      const size_t index,
      RowVectorPtr& inputRow) {
    std::vector<VectorPtr>& fields = inputRow->children();
    for (size_t i = 0; i < data.size(); ++i) {
      const std::string v = data[i];
      const StringView s(v.data(), v.size());
      std::shared_ptr<FlatVector<StringView>> flat =
          std::dynamic_pointer_cast<FlatVector<StringView>>(fields[i]);
      flat->set(index, s);
    }
  }

  const RowVectorPtr createSingleInputRow() {
    RowVectorPtr inputRow = RowVector::createEmpty(inputType, memoryPool.get());
    inputRow->resize(1);
    insertData(inputValues0, 0, inputRow);
    return inputRow;
  }

  const RowVectorPtr createInputRow() {
    RowVectorPtr inputRow = RowVector::createEmpty(inputType, memoryPool.get());
    inputRow->resize(2);
    insertData(inputValues0, 0, inputRow);
    insertData(inputValues1, 1, inputRow);
    return inputRow;
  }

  const std::shared_ptr<connector::filesystem::FileSystemInsertTableHandle>
  createFsTableHandle(
      const std::vector<uint32_t>& partitionIndexes,
      const std::vector<std::string>& partitionKeys,
      const std::unordered_map<std::string, std::string>& tableParams = {}) {
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>
        subFieldFilters;
    return std::make_shared<connector::filesystem::FileSystemInsertTableHandle>(
        "FilesystemWriter",
        inputType,
        partitionIndexes,
        partitionKeys,
        tableParams);
  }
};

} // namespace facebook::velox::connector::filesystem::test
