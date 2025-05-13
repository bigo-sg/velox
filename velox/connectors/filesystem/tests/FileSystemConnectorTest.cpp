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

#include "velox/common/file/File.h"
#include "velox/connectors/filesystem/tests/FileSystemConnectorTestBase.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <fstream>

namespace facebook::velox::connector::filesystem::test {
class FileSystemConnectorTest : public FileSystemConnectorTestBase {
  FileSystemDataSink* createFileSystemSink(
      const std::vector<uint32_t>& partitionIndexes,
      const std::vector<std::string>&);
};

TEST_F(FileSystemConnectorTest, testConfig) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  ASSERT_TRUE(fsConnector != nullptr);
  const std::shared_ptr<const config::ConfigBase>& connectorConfig =
      fsConnector->connectorConfig();
  const std::shared_ptr<FileSystemWriteConfig> writeConfig =
      std::make_shared<FileSystemWriteConfig>(connectorConfig);
  ASSERT_TRUE(
      writeConfig->exists(connector::filesystem::FileSystemWriteConfig::kPath));
  ASSERT_TRUE(writeConfig->exists(
      connector::filesystem::FileSystemWriteConfig::kFormat));
  ASSERT_TRUE(writeConfig->exists(
      connector::filesystem::FileSystemWriteConfig::kTaskId));
}

TEST_F(FileSystemConnectorTest, testWriteNonPartitionedTable) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({}, {}),
      createQueryCtx().get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);
  const RowVectorPtr inputData = createSingleInputRow();
  fsDataSink->appendData(inputData);
  const std::vector<std::shared_ptr<FsWriterInfo>> writerInfos =
      fsDataSink->getWriteInfos();
  ASSERT_TRUE(writerInfos.size() == 1);
  const std::shared_ptr<FsWriterInfo> writerInfo = writerInfos[0];
  const auto writerParams = writerInfo->writerParameters;
  ASSERT_TRUE(writerParams.writeDirectory() == dataPath);
  const std::string writeFilePath =
      writerParams.writeDirectory() + "/" + writerParams.writeFileName();
  const std::string targetFilePath =
      writerParams.targetDirectory() + "/" + writerParams.targetFileName();
  auto fs_ =
      filesystems::getFileSystem(writeFilePath, fsConnector->connectorConfig());
  ASSERT_TRUE(fs_ != nullptr);
  fs_->rename(writeFilePath, targetFilePath);
  ASSERT_TRUE(fs_->exists(targetFilePath));
  std::string localFilePath = targetFilePath.substr(7, targetFilePath.size());
  std::ifstream localFile(localFilePath);
  ASSERT_TRUE(localFile.is_open());
  std::string line;
  ASSERT_TRUE(getline(localFile, line));
  std::vector<std::string> lineValues;
  boost::algorithm::split(
      lineValues, line, boost::algorithm::is_any_of("\x01"));
  ASSERT_TRUE(lineValues.size() == inputValues0.size());
  for (size_t i = 0; i < lineValues.size(); ++i) {
    ASSERT_TRUE(lineValues[i] == inputValues0[i]);
  }
  fs_->remove(targetFilePath);
}

TEST_F(FileSystemConnectorTest, testWritePartitionedTable) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}),
      createQueryCtx().get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);
  const RowVectorPtr inputData = createInputRow();
  fsDataSink->appendData(inputData);
  ASSERT_TRUE(fsDataSink->getPartitionNums() == 2);
  const std::vector<std::shared_ptr<FsWriterInfo>> writerInfos =
      fsDataSink->getWriteInfos();
  ASSERT_TRUE(writerInfos.size() == 2);

  for (size_t i = 0; i < writerInfos.size(); ++i) {
    const std::shared_ptr<FsWriterInfo> writerInfo = writerInfos[i];
    const auto writerParams = writerInfo->writerParameters;
    const std::optional<std::string> partitionName =
        writerParams.partitionName();
    ASSERT_TRUE(partitionName.has_value());
    std::vector<std::string> partitionNames;
    boost::algorithm::split(
        partitionNames,
        partitionName.value(),
        boost::algorithm::is_any_of("/"));
    ASSERT_TRUE(partitionNames.size() == 2);
    for (size_t j = 0; j < partitionNames.size(); ++j) {
      std::vector<std::string> partitionKV;
      boost::algorithm::split(
          partitionKV, partitionNames[j], boost::algorithm::is_any_of("="));
      ASSERT_TRUE(partitionKV.size() == 2);
      if (j == 0) {
        ASSERT_TRUE(partitionKV[0] == "dt");
      } else {
        ASSERT_TRUE(partitionKV[0] == "hm");
      }
      if (i == 0) {
        ASSERT_TRUE(partitionKV[1] == inputValues0[j]);
      } else {
        ASSERT_TRUE(partitionKV[1] == inputValues1[j]);
      }
    }
    ASSERT_TRUE(
        writerParams.writeDirectory() ==
        dataPath + "/" + partitionName.value());
  }
}

TEST_F(FileSystemConnectorTest, testFileRolling) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unordered_map<std::string, std::string> tableParams;
  tableParams["sink.rolling-policy.file-size"] = "1 B";
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      createQueryCtx().get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);
  const RowVectorPtr inputData = createInputRow();
  fsDataSink->appendData(inputData);
  ASSERT_TRUE(fsDataSink->getWriteInfos().size() == 2);
  ASSERT_TRUE(fsDataSink->getPendingWriterInfosSize() == 0);
  const RowVectorPtr inputData1 = createInputRow();
  fsDataSink->appendData(inputData1);
  ASSERT_TRUE(fsDataSink->getWriteInfos().size() == 2);
  ASSERT_TRUE(fsDataSink->getPendingWriterInfosSize() == 2);
}

TEST_F(FileSystemConnectorTest, testFileCommit) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}),
      createQueryCtx().get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);
  const RowVectorPtr inputData = createInputRow();
  fsDataSink->appendData(inputData);
  fsDataSink->commit(0);
  const std::vector<std::shared_ptr<FsWriterInfo>> writerInfos =
      fsDataSink->getWriteInfos();
  ASSERT_TRUE(writerInfos.size() == 2);
  auto fs_ =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  for (size_t i = 0; i < writerInfos.size(); ++i) {
    const std::shared_ptr<FsWriterInfo> writerInfo = writerInfos[i];
    auto writerParams = writerInfo->writerParameters;
    const std::string writeFilePath =
        writerParams.writeDirectory() + "/" + writerParams.writeFileName();
    const std::string targetFilePath =
        writerParams.targetDirectory() + "/" + writerParams.targetFileName();
    ASSERT_TRUE(!fs_->exists(writeFilePath));
    ASSERT_TRUE(fs_->exists(targetFilePath));
    fs_->remove(targetFilePath);
  }
}
} // namespace facebook::velox::connector::filesystem::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}