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

#include <optional>
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/filesystem/FileSystemConfig.h"
#include "velox/connectors/filesystem/FileSystemInsertTableHandle.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/PartitionIdGenerator.h"
#include "boost/algorithm/string.hpp"
#include "folly/container/F14Map.h"

namespace facebook::velox::connector::filesystem {

using FileSystemWriteConfigPtr = std::shared_ptr<FileSystemWriteConfig>;
using InsertTableHandlePtr =
    std::shared_ptr<connector::ConnectorInsertTableHandle>;
using MemoryPoolPtr = std::shared_ptr<memory::MemoryPool>;

class FsWriterId : public hive::HiveWriterId {
 public:
  FsWriterId(std::optional<uint32_t> _partitionId)
      : hive::HiveWriterId(_partitionId, std::nullopt) {}
};

struct FsWriterIdHasher : public hive::HiveWriterIdHasher {};
struct FsWriterIdEq : public hive::HiveWriterIdEq {};
struct FsWriterParameters : public hive::HiveWriterParameters {
  FsWriterParameters(
      std::optional<std::string> partitionName,
      std::string targetFileName,
      std::string targetDirectory,
      std::optional<std::string> writeFileName = std::nullopt,
      std::optional<std::string> writeDirectory = std::nullopt)
      : hive::HiveWriterParameters(
            hive::HiveWriterParameters::UpdateMode::kAppend,
            partitionName,
            targetFileName,
            targetDirectory,
            writeFileName,
            writeDirectory) {}
};

struct FsWriterInfo : public hive::HiveWriterInfo {
  FsWriterInfo(
      FsWriterParameters parameters,
      MemoryPoolPtr _writerPool,
      MemoryPoolPtr _sinkPool,
      const time_t createTime)
      : hive::HiveWriterInfo(parameters, _writerPool, _sinkPool, nullptr),
        createTime_(createTime) {}

  bool shouldUpdateWriter(const FileSystemWriteConfigPtr& writeConfig_) {
    if (std::time(nullptr) - createTime_ >
        writeConfig_->getFileRollingIntervalMinutes() * 60) {
      return true;
    } else if (inputSizeInBytes > writeConfig_->getFileRollingSize()) {
      return true;
    }
    return false;
  }

  void setCommitted(bool committed) {
    committed_ = committed;
  }

  bool isCommitted() {
    return committed_;
  }

 private:
  const time_t createTime_;
  bool committed_ = false;
};

using FsWriterInfoPtr = std::shared_ptr<FsWriterInfo>;

class FsFileNameGenerator {
 public:
  FsFileNameGenerator(
      const std::string& prefix,
      const std::string& suffix,
      const std::string& taskId)
      : prefix_(prefix), suffix_(suffix), taskId_(taskId) {}

  const std::pair<std::string, std::string> gen() const;

 private:
  const std::string prefix_;
  const std::string suffix_;
  const std::string taskId_;
  mutable uint64_t partCounter_ = 0;
};

class FsPartitionIdGenerator : public hive::PartitionIdGenerator {
 public:
  FsPartitionIdGenerator(
      const RowTypePtr& inputType,
      std::vector<column_index_t> partitionChannels,
      const std::vector<std::string>& partitionKeys,
      uint32_t maxPartitions,
      memory::MemoryPool* pool,
      bool partitionPathAsLowerCase)
      : hive::PartitionIdGenerator(
            inputType,
            partitionChannels,
            maxPartitions,
            pool,
            partitionPathAsLowerCase),
        partitionKeys_(partitionKeys) {}

  const std::string fsPartitionName(const uint32_t& partitionId) const {
    std::string partitionName =
        hive::PartitionIdGenerator::partitionName(partitionId);
    std::vector<std::string> partitionNames;
    boost::algorithm::split(
        partitionNames, partitionName, boost::algorithm::is_any_of("/"));
    VELOX_CHECK(partitionNames.size() == partitionKeys_.size());
    std::vector<std::pair<std::string, std::string>> partitionKVs;
    for (size_t i = 0; i < partitionNames.size(); ++i) {
      std::vector<std::string> partitionKV;
      boost::algorithm::split(
          partitionKV, partitionNames[i], boost::algorithm::is_any_of("="));
      VELOX_CHECK(partitionKV.size() == 2);
      std::pair<std::string, std::string> kv;
      kv.first = partitionKeys_[i];
      kv.second = partitionKV[1];
      partitionKVs.emplace_back(kv);
    }
    std::stringstream ss;
    for (const auto& [k, v] : partitionKVs) {
      ss << k << "=" << v << "/";
    }
    std::string result = ss.str();
    return result.substr(0, result.size() - 1);
  }

 private:
  const std::vector<std::string> partitionKeys_;
};

class FileSystemDataSink : public DataSink {
 public:
  FileSystemDataSink(
      const RowTypePtr& inputType,
      const std::shared_ptr<FileSystemInsertTableHandle>& insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      const FileSystemWriteConfigPtr& writeConfig,
      const std::vector<uint32_t>& partitionIndexes = {},
      const std::vector<std::string>& partitionKeys = {});

  enum class State {
    /// The data sink accepts new append data in this state.
    kRunning = 0,
    /// The data sink flushes any buffered data to the underlying file writer
    /// but no more data can be appended.
    kFinishing = 1,
    /// The data sink is aborted on error and no more data can be appended.
    kAborted = 2,
    /// The data sink is closed on error and no more data can be appended.
    kClosed = 3
  };

  static const std::string stateString(State state) {
    switch (state) {
      case State::kRunning:
        return "RUNNING";
      case State::kFinishing:
        return "FLUSHING";
      case State::kClosed:
        return "CLOSED";
      case State::kAborted:
        return "ABORTED";
      default:
        VELOX_UNREACHABLE("BAD STATE: {}", static_cast<int>(state));
    }
  }

  void appendData(RowVectorPtr input) override;

  bool finish() override;

  std::vector<std::string> close() override;

  void abort() override;

  connector::DataSink::Stats stats() const override;

  std::vector<std::string> commit(int64_t id) override;

  // For test.
  const std::vector<FsWriterInfoPtr>& getWriteInfos() {
    return writerInfo_;
  }

  // For test.
  const size_t getPartitionNums() {
    return partitionIdGenerator_->numPartitions();
  }

  // For test
  const size_t getPendingWriterInfosSize() {
    return pendingWriterInfo_.size();
  }

 private:
  const RowTypePtr inputType_;
  const std::shared_ptr<FileSystemInsertTableHandle> insertTableHandle_;
  const ConnectorQueryCtx* queryCtx_;
  const FileSystemWriteConfigPtr writeConfig_;
  const std::unordered_map<std::string, dwio::common::FileFormat> fileFormats_;

  // Below are structures for partitions from all inputs. writerInfo_ and
  // writers_ are both indexed by partitionId.
  const uint32_t maxOpenWriters_;
  // Below are structures updated when processing current input. partitionIds_
  // are indexed by the row of input_. partitionRows_, rawPartitionRows_ and
  // partitionSizes_ are indexed by partitionId.
  const std::vector<column_index_t> partitionChannels_;
  const std::vector<std::string> partitionKeys_;

  State state_{State::kRunning};
  const std::unique_ptr<FsPartitionIdGenerator> partitionIdGenerator_;

  // The map from writer id to the writer index in 'writers_' and 'writerInfo_'.
  folly::F14FastMap<FsWriterId, uint32_t, FsWriterIdHasher, FsWriterIdEq>
      writerIndexMap_;

  std::vector<FsWriterInfoPtr> writerInfo_;
  std::vector<FsWriterInfoPtr> pendingWriterInfo_;
  std::vector<std::unique_ptr<dwio::common::Writer>> writers_;
  // IO statistics collected for each writer.
  std::vector<std::shared_ptr<io::IoStatistics>> ioStats_;
  const int64_t watermark_ = 0;

  // Indices of dataChannel are stored in ascending order
  const std::vector<column_index_t> dataChannels_;
  raw_vector<uint64_t> partitionIds_;
  std::vector<BufferPtr> partitionRows_;
  std::vector<vector_size_t*> rawPartitionRows_;
  std::vector<vector_size_t> partitionSizes_;
  const std::shared_ptr<dwio::common::WriterFactory> writerFactory_;
  const std::shared_ptr<const FsFileNameGenerator> fileNameGenerator_;
  std::shared_ptr<filesystems::FileSystem> fs_;

  void write(size_t index, RowVectorPtr input);
  // Compute the partition id for each row in 'input'.
  void computePartitionIds(const RowVectorPtr& input);
  void splitInputRowsAndEnsureWriters();

  const std::unique_ptr<dwio::common::Writer> createWriter(
      const std::string& writePath,
      const FsWriterInfoPtr& writeInfo,
      const std::shared_ptr<io::IoStatistics>& ioStats);

  // Appends a new writer for the given 'id'. The function returns the index of
  // the newly created writer in 'writers_'.
  uint32_t appendWriter(const FsWriterId& id);

  uint32_t updateWriter(const FsWriterId& id);

  FOLLY_ALWAYS_INLINE void checkRunning() const {
    VELOX_CHECK_EQ(
        stateString(state_), "RUNNING", "FileSystem data sink is not running");
  }

  // Returns true if the table is partitioned.
  FOLLY_ALWAYS_INLINE bool isPartitioned() const {
    return partitionIdGenerator_ != nullptr;
  }

  // Makes sure to create one writer for the given writer id. The function
  // returns the corresponding index in 'writers_'.
  uint32_t ensureWriter(const FsWriterId& id);

  // Gets write and target file names for a writer based on the table commit
  // strategy as well as table partitioned type. If commit is not required, the
  // write file and target file has the same name. If not, add a temp file
  // prefix to the target file for write file name. The coordinator (or driver
  // for Presto on spark) will rename the write file to target file to commit
  // the table write when update the metadata store.
  const std::pair<std::string, std::string> getWriterFileNames() const;

  FsWriterParameters getWriterParameters(
      const std::optional<std::string>& partition) const;

  // Get the HiveWriter corresponding to the row from partitionIds.
  FOLLY_ALWAYS_INLINE FsWriterId getWriterId(size_t row) const;

  // Validates the state transition from 'oldState' to 'newState'.
  void checkStateTransition(State oldState, State newState);
  void setState(State newState);

  void closeInternal();

  std::shared_ptr<memory::MemoryPool> createWriterPool(
      const FsWriterId& writerId);

  const int64_t extractTimestampFromPartitionName(
      const std::string& partitionName);
};

} // namespace facebook::velox::connector::filesystem