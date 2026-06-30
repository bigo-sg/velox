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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/connectors/filesystem/tests/FileSystemConnectorTestBase.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/tests/HdfsMiniCluster.h"
#ifdef VELOX_ENABLE_PARQUET
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#endif

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <chrono>
#include <fstream>
#include <thread>

namespace facebook::velox::connector::filesystem::test {

std::shared_ptr<FileSystemWriteConfig> makeWriteConfig(
    std::unordered_map<std::string, std::string> configs) {
  return std::make_shared<FileSystemWriteConfig>(
      std::make_shared<const config::ConfigBase>(std::move(configs)));
}

std::vector<std::string> snapshotAndCommit(
    FileSystemDataSink* fsDataSink,
    int64_t checkpointId) {
  fsDataSink->snapshot(checkpointId);
  return fsDataSink->commit(checkpointId);
}

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
  ASSERT_TRUE(writeConfig->exists(kTaskIdKey));
}

TEST_F(FileSystemConnectorTest, testWriteConfigDefaults) {
  const auto writeConfig = makeWriteConfig({});

  ASSERT_EQ(writeConfig->getPath(), "");
  ASSERT_EQ(writeConfig->getFileRollingIntervalMillis(), 60'000);
  ASSERT_EQ(writeConfig->getFileRollingSize(), 128L * 1024 * 1024);
  ASSERT_EQ(writeConfig->getPartitionCommitTrigger(), "process-time");
  ASSERT_EQ(writeConfig->getPartitionCommitPolicy(), "metastore");
  ASSERT_EQ(writeConfig->getPartitionCommitDelayMillis(), 60'000);
  ASSERT_EQ(writeConfig->getPartitionTimeExtractPattern(), "");
  ASSERT_EQ(
      writeConfig->getFileCompressionType(), common::CompressionKind_NONE);
}

TEST_F(FileSystemConnectorTest, testWriteConfigValues) {
  const auto writeConfig = makeWriteConfig({
      {FileSystemWriteConfig::kPath, "file:///tmp/filesystem_sink"},
      {FileSystemWriteConfig::kFormat, "parquet"},
      {FileSystemWriteConfig::kFileRollingInterval, "2 minutes"},
      {FileSystemWriteConfig::kFileRollingSize, "2KB"},
      {FileSystemWriteConfig::kPartitionCommitTrigger, "partition-time"},
      {FileSystemWriteConfig::kPartitionCommitPolicy, "success-file"},
      {FileSystemWriteConfig::kPartitionCommitDelay, "3 sec"},
      {FileSystemWriteConfig::kPartitionTimeExtractPattern, "$dt $hour:00:00"},
      {FileSystemWriteConfig::kParquetCompressionCodec, "snappy"},
  });

  ASSERT_EQ(writeConfig->getPath(), "file:///tmp/filesystem_sink");
  ASSERT_EQ(writeConfig->getFormat(), dwio::common::FileFormat::PARQUET);
  ASSERT_EQ(writeConfig->getFileRollingIntervalMillis(), 120'000);
  ASSERT_EQ(writeConfig->getFileRollingSize(), 2'048);
  ASSERT_EQ(writeConfig->getPartitionCommitTrigger(), "partition-time");
  ASSERT_EQ(writeConfig->getPartitionCommitPolicy(), "success-file");
  ASSERT_EQ(writeConfig->getPartitionCommitDelayMillis(), 3'000);
  ASSERT_EQ(writeConfig->getPartitionTimeExtractPattern(), "$dt $hour:00:00");
  ASSERT_EQ(
      writeConfig->getFileCompressionType(), common::CompressionKind_SNAPPY);
}

TEST_F(FileSystemConnectorTest, testWriteConfigCompressionValues) {
  ASSERT_EQ(
      makeWriteConfig(
          {
              {FileSystemWriteConfig::kFormat, "parquet"},
              {FileSystemWriteConfig::kParquetCompressionCodec, "snappy"},
          })
          ->getFileCompressionType(),
      common::CompressionKind_SNAPPY);
  ASSERT_EQ(
      makeWriteConfig(
          {
              {FileSystemWriteConfig::kFormat, "parquet"},
              {FileSystemWriteConfig::kParquetCompressionCodec, "lz4"},
          })
          ->getFileCompressionType(),
      common::CompressionKind_LZ4);
  ASSERT_EQ(
      makeWriteConfig(
          {
              {FileSystemWriteConfig::kFormat, "parquet"},
              {FileSystemWriteConfig::kParquetCompressionCodec, "zstd"},
          })
          ->getFileCompressionType(),
      common::CompressionKind_ZSTD);
  ASSERT_EQ(
      makeWriteConfig(
          {
              {FileSystemWriteConfig::kFormat, "parquet"},
              {FileSystemWriteConfig::kParquetCompressionCodec, "SNAPPY"},
          })
          ->getFileCompressionType(),
      common::CompressionKind_SNAPPY);
  ASSERT_EQ(
      makeWriteConfig({
                          {FileSystemWriteConfig::kFormat, "parquet"},
                          {FileSystemWriteConfig::kFileCompression, "zstd"},
                      })
          ->getFileCompressionType(),
      common::CompressionKind_ZSTD);
}

TEST_F(FileSystemConnectorTest, testWriteConfigInvalidValues) {
  VELOX_ASSERT_THROW(
      makeWriteConfig({{FileSystemWriteConfig::kFormat, "json"}})->getFormat(),
      "Format json not supported for filesystem sink.");

  VELOX_ASSERT_THROW(
      makeWriteConfig({{FileSystemWriteConfig::kFileRollingInterval, "1"}})
          ->getFileRollingIntervalMillis(),
      "Missing unit in duration '1'");

  VELOX_ASSERT_THROW(
      makeWriteConfig(
          {{FileSystemWriteConfig::kPartitionCommitDelay, "minutes"}})
          ->getPartitionCommitDelayMillis(),
      "Invalid duration 'minutes'");

  VELOX_ASSERT_THROW(
      makeWriteConfig({{FileSystemWriteConfig::kFileRollingSize, "1XB"}})
          ->getFileRollingSize(),
      "Invalid capacity unit 'XB'");

  VELOX_ASSERT_THROW(
      makeWriteConfig(
          {{FileSystemWriteConfig::kPartitionCommitTrigger, "event-time"}})
          ->getPartitionCommitTrigger(),
      "Unsupported sink.partition-commit.trigger 'event-time'");

  VELOX_ASSERT_THROW(
      makeWriteConfig(
          {
              {FileSystemWriteConfig::kFormat, "parquet"},
              {FileSystemWriteConfig::kParquetCompressionCodec, "gzip"},
          })
          ->getFileCompressionType(),
      "Unsupported parquet compression codec 'gzip'");
  VELOX_ASSERT_THROW(
      makeWriteConfig(
          {
              {FileSystemWriteConfig::kFormat, "csv"},
              {FileSystemWriteConfig::kParquetCompressionCodec, "snappy"},
          })
          ->getFileCompressionType(),
      "File compression 'snappy' is only supported for parquet filesystem sink.");
}

TEST_F(FileSystemConnectorTest, testWriteNonPartitionedTable) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({}, {}),
      queryCtx.get(),
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
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}),
      queryCtx.get(),
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

TEST_F(
    FileSystemConnectorTest,
    testCheckpointCommitDoesNotIncludePostBarrierWrites) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({}, {}),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  auto* fsDataSink = reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);

  fsDataSink->appendData(createSingleInputRow());
  fsDataSink->snapshot(1);
  const auto checkpoint1TargetPath =
      fsDataSink->getWriteInfos()[0]->writerParameters.targetDirectory() + "/" +
      fsDataSink->getWriteInfos()[0]->writerParameters.targetFileName();

  fsDataSink->appendData(createSingleInputRow());
  const auto postBarrierTargetPath =
      fsDataSink->getWriteInfos()[0]->writerParameters.targetDirectory() + "/" +
      fsDataSink->getWriteInfos()[0]->writerParameters.targetFileName();
  ASSERT_NE(checkpoint1TargetPath, postBarrierTargetPath);

  const std::vector<std::string> committed = fsDataSink->commit(1);
  ASSERT_EQ(committed.size(), 1);
  ASSERT_EQ(committed[0], checkpoint1TargetPath);

  auto fs =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  ASSERT_TRUE(fs->exists(checkpoint1TargetPath));
  ASSERT_FALSE(fs->exists(postBarrierTargetPath));

  fsDataSink->snapshot(2);
  const std::vector<std::string> committed2 = fsDataSink->commit(2);
  ASSERT_EQ(committed2.size(), 1);
  ASSERT_EQ(committed2[0], postBarrierTargetPath);
  ASSERT_TRUE(fs->exists(postBarrierTargetPath));

  fs->remove(checkpoint1TargetPath);
  fs->remove(postBarrierTargetPath);
}

TEST_F(FileSystemConnectorTest, testNonPartitionedFileCommit) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({}, {}),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);
  const RowVectorPtr inputData = createSingleInputRow();
  fsDataSink->appendData(inputData);
  const std::vector<std::string> committed = snapshotAndCommit(fsDataSink, 0);
  ASSERT_EQ(committed.size(), 1);
  const auto writerParams = fsDataSink->getWriteInfos()[0]->writerParameters;
  const std::string targetFilePath =
      writerParams.targetDirectory() + "/" + writerParams.targetFileName();
  ASSERT_EQ(committed[0], targetFilePath);
  auto fs_ =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  ASSERT_TRUE(fs_->exists(targetFilePath));
  fs_->remove(targetFilePath);
}

TEST_F(FileSystemConnectorTest, testHdfsNonPartitionedFileCommit) {
  filesystems::registerHdfsFileSystem();
  std::unique_ptr<filesystems::test::HdfsMiniCluster> miniCluster;
  try {
    miniCluster = std::make_unique<filesystems::test::HdfsMiniCluster>();
  } catch (...) {
    GTEST_SKIP() << "Skipping HDFS minicluster test: hadoop is unavailable";
  }
  if (!miniCluster->hasMiniclusterJar()) {
    GTEST_SKIP() << "Skipping HDFS minicluster test: minicluster jar is "
                    "unavailable";
  }
  miniCluster->start();
#ifdef VELOX_ENABLE_PARQUET
  parquet::registerParquetWriterFactory();
#endif

  const std::string hdfsDataPath =
      fmt::format("{}/filesystem_connector_test", miniCluster->url());
  std::unordered_map<std::string, std::string> tableParams;
  tableParams[connector::filesystem::FileSystemWriteConfig::kPath] =
      hdfsDataPath;
#ifdef VELOX_ENABLE_PARQUET
  tableParams[connector::filesystem::FileSystemWriteConfig::kFormat] =
      "parquet";
#endif

  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({}, {}, tableParams),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  auto* fsDataSink = reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);

  fsDataSink->appendData(createSingleInputRow());
  const std::vector<std::string> committed = snapshotAndCommit(fsDataSink, 0);
  ASSERT_EQ(committed.size(), 1);

  const auto writerParams = fsDataSink->getWriteInfos()[0]->writerParameters;
  const std::string targetFilePath =
      writerParams.targetDirectory() + "/" + writerParams.targetFileName();
  ASSERT_EQ(committed[0], targetFilePath);

  auto hdfs =
      filesystems::getFileSystem(hdfsDataPath, fsConnector->connectorConfig());
  const std::string writeFilePath =
      writerParams.writeDirectory() + "/" + writerParams.writeFileName();
  ASSERT_FALSE(hdfs->exists(writeFilePath));
  ASSERT_TRUE(hdfs->exists(targetFilePath));
  hdfs->remove(targetFilePath);
#ifdef VELOX_ENABLE_PARQUET
  parquet::unregisterParquetWriterFactory();
#endif
}

TEST_F(FileSystemConnectorTest, testHdfsPartitionedFileCommit) {
  filesystems::registerHdfsFileSystem();
  std::unique_ptr<filesystems::test::HdfsMiniCluster> miniCluster;
  try {
    miniCluster = std::make_unique<filesystems::test::HdfsMiniCluster>();
  } catch (...) {
    GTEST_SKIP() << "Skipping HDFS minicluster test: hadoop is unavailable";
  }
  if (!miniCluster->hasMiniclusterJar()) {
    GTEST_SKIP() << "Skipping HDFS minicluster test: minicluster jar is "
                    "unavailable";
  }
  miniCluster->start();
#ifdef VELOX_ENABLE_PARQUET
  parquet::registerParquetWriterFactory();
#endif

  const std::string hdfsDataPath =
      fmt::format("{}/filesystem_connector_partitioned", miniCluster->url());
  std::unordered_map<std::string, std::string> tableParams;
  tableParams[connector::filesystem::FileSystemWriteConfig::kPath] =
      hdfsDataPath;
  tableParams
      [connector::filesystem::FileSystemWriteConfig::kPartitionCommitDelay] =
          "0 sec";
#ifdef VELOX_ENABLE_PARQUET
  tableParams[connector::filesystem::FileSystemWriteConfig::kFormat] =
      "parquet";
#endif

  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  auto* fsDataSink = reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);

  fsDataSink->appendData(createInputRow());
  const std::vector<std::string> committed = snapshotAndCommit(fsDataSink, 0);
  ASSERT_EQ(committed.size(), 2);

  auto hdfs =
      filesystems::getFileSystem(hdfsDataPath, fsConnector->connectorConfig());
  for (const auto& writerInfo : fsDataSink->getWriteInfos()) {
    const auto writerParams = writerInfo->writerParameters;
    const std::optional<std::string> partitionName =
        writerParams.partitionName();
    ASSERT_TRUE(partitionName.has_value());
    ASSERT_EQ(
        std::count(committed.begin(), committed.end(), partitionName.value()),
        1);
    const std::string writeFilePath =
        writerParams.writeDirectory() + "/" + writerParams.writeFileName();
    const std::string targetFilePath =
        writerParams.targetDirectory() + "/" + writerParams.targetFileName();
    ASSERT_FALSE(hdfs->exists(writeFilePath));
    ASSERT_TRUE(hdfs->exists(targetFilePath));
    hdfs->remove(targetFilePath);
  }
#ifdef VELOX_ENABLE_PARQUET
  parquet::unregisterParquetWriterFactory();
#endif
}

TEST_F(FileSystemConnectorTest, testFileRolling) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unordered_map<std::string, std::string> tableParams;
  tableParams["sink.rolling-policy.file-size"] = "1 B";
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      queryCtx.get(),
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
  std::unordered_map<std::string, std::string> tableParams;
  tableParams[FileSystemWriteConfig::kPartitionCommitDelay] = "0 sec";
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);
  const RowVectorPtr inputData = createInputRow();
  fsDataSink->appendData(inputData);
  const std::vector<std::string> committed = snapshotAndCommit(fsDataSink, 0);
  ASSERT_EQ(committed.size(), 2);
  const std::vector<std::shared_ptr<FsWriterInfo>> writerInfos =
      fsDataSink->getWriteInfos();
  ASSERT_TRUE(writerInfos.size() == 2);
  auto fs_ =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  for (size_t i = 0; i < writerInfos.size(); ++i) {
    const std::shared_ptr<FsWriterInfo> writerInfo = writerInfos[i];
    auto writerParams = writerInfo->writerParameters;
    const std::optional<std::string> partitionName =
        writerParams.partitionName();
    ASSERT_TRUE(partitionName.has_value());
    ASSERT_EQ(
        std::count(committed.begin(), committed.end(), partitionName.value()),
        1);
    const std::string writeFilePath =
        writerParams.writeDirectory() + "/" + writerParams.writeFileName();
    const std::string targetFilePath =
        writerParams.targetDirectory() + "/" + writerParams.targetFileName();
    ASSERT_TRUE(!fs_->exists(writeFilePath));
    ASSERT_TRUE(fs_->exists(targetFilePath));
    fs_->remove(targetFilePath);
  }
}

TEST_F(FileSystemConnectorTest, testProcessTimeCommitWaitsForDelay) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unordered_map<std::string, std::string> tableParams;
  tableParams[FileSystemWriteConfig::kPartitionCommitTrigger] = "process-time";
  tableParams[FileSystemWriteConfig::kPartitionCommitDelay] = "2 sec";
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);

  fsDataSink->appendData(createInputRow());
  fsDataSink->snapshot(0);
  ASSERT_TRUE(fsDataSink->commit(0).empty());
  ASSERT_EQ(fsDataSink->getPendingWriterInfosSize(), 2);

  std::this_thread::sleep_for(std::chrono::seconds(3));
  const std::vector<std::string> committed = fsDataSink->commit(1);
  ASSERT_EQ(committed.size(), 2);

  auto fs =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  const std::vector<std::string> partitions = {
      "dt=2025-07-11/hm=14", "dt=2025-07-11/hm=15"};
  for (const auto& partition : partitions) {
    const auto partitionDir = fmt::format("{}/{}", dataPath, partition);
    for (const auto& file : fs->list(partitionDir)) {
      fs->remove(fmt::format("{}/{}", partitionDir, file));
    }
  }
}

TEST_F(FileSystemConnectorTest, testPartitionTimeCommitWaitsForWatermark) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unordered_map<std::string, std::string> tableParams;
  tableParams[FileSystemWriteConfig::kPartitionCommitTrigger] =
      "partition-time";
  tableParams[FileSystemWriteConfig::kPartitionCommitDelay] = "0 sec";
  tableParams[FileSystemWriteConfig::kPartitionTimeExtractPattern] =
      "$dt $hm:00:00";
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);

  fsDataSink->appendData(createInputRow());
  const std::vector<std::shared_ptr<FsWriterInfo>> writerInfos =
      fsDataSink->getWriteInfos();
  ASSERT_EQ(writerInfos.size(), 2);

  fsDataSink->setWatermark(1'752'242'400'000L - 1);
  fsDataSink->snapshot(0);
  std::vector<std::string> committed = fsDataSink->commit(0);
  ASSERT_TRUE(committed.empty());
  ASSERT_EQ(fsDataSink->getPendingWriterInfosSize(), 2);

  auto fs =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  for (const auto& writerInfo : writerInfos) {
    const auto writerParams = writerInfo->writerParameters;
    const std::string writeFilePath =
        writerParams.writeDirectory() + "/" + writerParams.writeFileName();
    const std::string targetFilePath =
        writerParams.targetDirectory() + "/" + writerParams.targetFileName();
    ASSERT_TRUE(!fs->exists(writeFilePath));
    ASSERT_TRUE(fs->exists(targetFilePath));
  }

  fsDataSink->setWatermark(1'752'242'400'000L);
  committed = fsDataSink->commit(1);
  ASSERT_EQ(committed.size(), 1);
  ASSERT_EQ(committed[0], "dt=2025-07-11/hm=14");
  ASSERT_EQ(fsDataSink->getPendingWriterInfosSize(), 1);

  fsDataSink->setWatermark(1'752'242'400'000L - 1);
  committed = fsDataSink->commit(2);
  ASSERT_TRUE(committed.empty());
  ASSERT_EQ(fsDataSink->getPendingWriterInfosSize(), 1);

  fsDataSink->setWatermark(1'752'246'000'000L + 1);
  committed = fsDataSink->commit(3);
  ASSERT_EQ(committed.size(), 1);
  ASSERT_EQ(committed[0], "dt=2025-07-11/hm=15");
  ASSERT_EQ(fsDataSink->getPendingWriterInfosSize(), 0);

  std::vector<std::string> allCommitted = {"dt=2025-07-11/hm=14"};
  allCommitted.insert(allCommitted.end(), committed.begin(), committed.end());
  for (const auto& writerInfo : writerInfos) {
    const auto writerParams = writerInfo->writerParameters;
    const std::optional<std::string> partitionName =
        writerParams.partitionName();
    ASSERT_TRUE(partitionName.has_value());
    ASSERT_EQ(
        std::count(
            allCommitted.begin(), allCommitted.end(), partitionName.value()),
        1);

    const std::string targetFilePath =
        writerParams.targetDirectory() + "/" + writerParams.targetFileName();
    ASSERT_TRUE(fs->exists(targetFilePath));
    fs->remove(targetFilePath);
  }
}

TEST_F(FileSystemConnectorTest, testPartitionTimeCommitLateDataRecommit) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unordered_map<std::string, std::string> tableParams;
  tableParams[FileSystemWriteConfig::kPartitionCommitTrigger] =
      "partition-time";
  tableParams[FileSystemWriteConfig::kPartitionCommitDelay] = "0 sec";
  tableParams[FileSystemWriteConfig::kPartitionTimeExtractPattern] =
      "$dt $hm:00:00";
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);

  fsDataSink->appendData(createInputRow());
  fsDataSink->setWatermark(1'752'246'000'000L + 1);
  std::vector<std::string> committed = snapshotAndCommit(fsDataSink, 0);
  ASSERT_EQ(committed.size(), 2);

  fsDataSink->appendData(createInputRow());
  committed = snapshotAndCommit(fsDataSink, 1);
  ASSERT_EQ(committed.size(), 2);

  auto fs =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  const std::vector<std::string> partitions = {
      "dt=2025-07-11/hm=14", "dt=2025-07-11/hm=15"};
  for (const auto& partition : partitions) {
    const auto partitionDir = fmt::format("{}/{}", dataPath, partition);
    for (const auto& file : fs->list(partitionDir)) {
      fs->remove(fmt::format("{}/{}", partitionDir, file));
    }
  }
}

TEST_F(FileSystemConnectorTest, testPartitionTimeCommitWithSessionTimezone) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unordered_map<std::string, std::string> tableParams;
  tableParams[FileSystemWriteConfig::kPartitionCommitTrigger] =
      "partition-time";
  tableParams[FileSystemWriteConfig::kPartitionCommitDelay] = "0 sec";
  tableParams[FileSystemWriteConfig::kPartitionTimeExtractPattern] =
      "$dt $hm:00:00";
  auto queryCtx = createQueryCtx("Asia/Shanghai", true);
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);

  fsDataSink->appendData(createSingleInputRow());
  // 2025-07-11 14:00:00 in Asia/Shanghai is 2025-07-11 06:00:00 UTC.
  constexpr int64_t kPartitionTimestampSecondsUtc = 1'752'213'600L;
  fsDataSink->setWatermark(kPartitionTimestampSecondsUtc * 1'000L - 1);
  fsDataSink->snapshot(0);
  ASSERT_TRUE(fsDataSink->commit(0).empty());

  fsDataSink->setWatermark(kPartitionTimestampSecondsUtc * 1'000L + 1);
  const std::vector<std::string> committed = fsDataSink->commit(1);
  ASSERT_EQ(committed.size(), 1);
  ASSERT_EQ(committed[0], "dt=2025-07-11/hm=14");

  auto fs =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  const auto writerParams = fsDataSink->getWriteInfos()[0]->writerParameters;
  const std::string targetFilePath =
      writerParams.targetDirectory() + "/" + writerParams.targetFileName();
  ASSERT_TRUE(fs->exists(targetFilePath));
  fs->remove(targetFilePath);
}

TEST_F(FileSystemConnectorTest, testPartitionTimeCommitWithRolling) {
  std::shared_ptr<connector::Connector> fsConnector =
      getConnector(fileSystemConnectorId);
  std::unordered_map<std::string, std::string> tableParams;
  tableParams[FileSystemWriteConfig::kPartitionCommitTrigger] =
      "partition-time";
  tableParams[FileSystemWriteConfig::kPartitionCommitDelay] = "0 sec";
  tableParams[FileSystemWriteConfig::kPartitionTimeExtractPattern] =
      "$dt $hm:00:00";
  tableParams[FileSystemWriteConfig::kFileRollingSize] = "1B";
  auto queryCtx = createQueryCtx();
  std::unique_ptr<DataSink> dataSink = fsConnector->createDataSink(
      inputType,
      createFsTableHandle({0, 1}, {"dt", "hm"}, tableParams),
      queryCtx.get(),
      CommitStrategy::kNoCommit);
  FileSystemDataSink* fsDataSink =
      reinterpret_cast<FileSystemDataSink*>(dataSink.get());
  ASSERT_TRUE(fsDataSink != nullptr);

  fsDataSink->appendData(createInputRow());
  fsDataSink->appendData(createInputRow());
  ASSERT_GT(fsDataSink->getPendingWriterInfosSize(), 0);

  fsDataSink->setWatermark(1'752'246'000'000L + 1);
  const std::vector<std::string> committed = snapshotAndCommit(fsDataSink, 0);
  ASSERT_EQ(committed.size(), 2);
  ASSERT_EQ(fsDataSink->getPendingWriterInfosSize(), 0);

  auto fs =
      filesystems::getFileSystem(dataPath, fsConnector->connectorConfig());
  for (const auto& writerInfo : fsDataSink->getWriteInfos()) {
    const auto writerParams = writerInfo->writerParameters;
    const std::string writeFilePath =
        writerParams.writeDirectory() + "/" + writerParams.writeFileName();
    const std::string targetFilePath =
        writerParams.targetDirectory() + "/" + writerParams.targetFileName();
    ASSERT_FALSE(fs->exists(writeFilePath));
    ASSERT_TRUE(fs->exists(targetFilePath));
    fs->remove(targetFilePath);
  }
}

} // namespace facebook::velox::connector::filesystem::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}
