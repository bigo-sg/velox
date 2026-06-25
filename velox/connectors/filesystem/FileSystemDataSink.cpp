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

#include "velox/connectors/filesystem/FileSystemDataSink.h"
#include <common/compression/Compression.h>
#include <regex>
#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"
#include "velox/common/base/Fs.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::connector::filesystem {
#define WRITER_NON_RECLAIMABLE_SECTION_GUARD(index)       \
  memory::NonReclaimableSectionGuard nonReclaimableGuard( \
      writerInfo_[(index)]->nonReclaimableSectionHolder.get())

// Returns the type of non-partition data columns.
RowTypePtr getNonPartitionTypes(
    const std::vector<column_index_t>& dataCols,
    const RowTypePtr& inputType) {
  std::vector<std::string> childNames;
  std::vector<TypePtr> childTypes;
  const auto& dataSize = dataCols.size();
  childNames.reserve(dataSize);
  childTypes.reserve(dataSize);
  for (const auto& dataCol : dataCols) {
    childNames.push_back(inputType->nameOf(dataCol));
    childTypes.push_back(inputType->childAt(dataCol));
  }

  return ROW(std::move(childNames), std::move(childTypes));
}

// Returns the column indices of non-partition data columns.
std::vector<column_index_t> getNonPartitionChannels(
    const std::vector<column_index_t>& partitionChannels,
    const column_index_t childrenSize) {
  std::vector<column_index_t> dataChannels;
  dataChannels.reserve(childrenSize - partitionChannels.size());

  for (column_index_t i = 0; i < childrenSize; i++) {
    if (std::find(partitionChannels.cbegin(), partitionChannels.cend(), i) ==
        partitionChannels.cend()) {
      dataChannels.push_back(i);
    }
  }
  return dataChannels;
}

FileSystemDataSink::FileSystemDataSink(
    const RowTypePtr& inputType,
    const std::shared_ptr<FileSystemInsertTableHandle>& insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    const FileSystemWriteConfigPtr& writeConfig,
    const std::vector<uint32_t>& partitionIndexes,
    const std::vector<std::string>& partitionKeys)
    : inputType_(std::move(inputType)),
      insertTableHandle_(std::move(insertTableHandle)),
      queryCtx_(connectorQueryCtx),
      writeConfig_(writeConfig),
      maxOpenWriters_(writeConfig_->maxPartitionsPerWriter()),
      partitionChannels_(partitionIndexes),
      partitionKeys_(partitionKeys),
      partitionIdGenerator_(
          !partitionChannels_.empty()
              ? std::make_unique<FsPartitionIdGenerator>(
                    inputType_,
                    partitionChannels_,
                    partitionKeys_,
                    maxOpenWriters_,
                    queryCtx_->memoryPool(),
                    writeConfig_->isPartitionPathAsLowerCase())
              : nullptr),
      dataChannels_(
          getNonPartitionChannels(partitionChannels_, inputType_->size())),
      writerFactory_(dwio::common::getWriterFactory(writeConfig_->getFormat())),
      fileNameGenerator_(std::make_shared<const FsFileNameGenerator>(
          queryCtx_->sessionProperties()->get<std::string>("query_uuid", ""),
          "",
          queryCtx_->sessionProperties()->get<std::string>(
              "task_index",
              "0"))) {}

const std::unique_ptr<dwio::common::Writer> FileSystemDataSink::createWriter(
    const std::string& writePath,
    const FsWriterInfoPtr& writerInfo,
    const std::shared_ptr<io::IoStatistics>& ioStats) {
  std::shared_ptr<dwio::common::WriterOptions> options =
      writerFactory_->createWriterOptions();
  const auto* connectorSessionProperties = queryCtx_->sessionProperties();
  // Only overwrite options in case they were not already provided.
  if (options->schema == nullptr) {
    options->schema = getNonPartitionTypes(dataChannels_, inputType_);
  }
  if (options->memoryPool == nullptr) {
    options->memoryPool = writerInfo->writerPool.get();
  }

  if (options->nonReclaimableSection == nullptr) {
    options->nonReclaimableSection =
        writerInfo->nonReclaimableSectionHolder.get();
  }

  if (options->memoryReclaimerFactory == nullptr ||
      options->memoryReclaimerFactory() == nullptr) {
    options->memoryReclaimerFactory = []() {
      return exec::MemoryReclaimer::create();
    };
  }
  // options->sessionTimezoneName = queryCtx_->sessionTimezone();
  options->adjustTimestampToTimezone = queryCtx_->adjustTimestampToTimezone();
  options->processConfigs(*writeConfig_->config(), *connectorSessionProperties);
  // Prevents the memory allocation during the writer creation.
  return writerFactory_->createWriter(
      dwio::common::FileSink::create(
          writePath,
          {
              .bufferWrite = false,
              // .fileCreateConfig = writeConfig_->config(),
              .pool = writerInfo->sinkPool.get(),
              .metricLogger = dwio::common::MetricsLog::voidLog(),
              .stats = ioStats.get(),
          }),
      options);
}

const std::pair<std::string, std::string>
FileSystemDataSink::getWriterFileNames() const {
  return fileNameGenerator_->gen();
}

std::string makePartitionDirectory(
    const std::string& tableDirectory,
    const std::optional<std::string>& partitionSubdirectory) {
  if (partitionSubdirectory.has_value()) {
    return fs::path(tableDirectory) / partitionSubdirectory.value();
  }
  return tableDirectory;
}

FsWriterParameters FileSystemDataSink::getWriterParameters(
    const std::optional<std::string>& partition) const {
  auto [targetFileName, writeFileName] = getWriterFileNames();
  return FsWriterParameters{
      partition,
      targetFileName,
      makePartitionDirectory(writeConfig_->getPath(), partition),
      writeFileName,
      makePartitionDirectory(writeConfig_->getPath(), partition)};
}

std::shared_ptr<memory::MemoryPool> FileSystemDataSink::createWriterPool(
    const FsWriterId& writerId) {
  auto* connectorPool = queryCtx_->connectorMemoryPool();
  return connectorPool->addAggregateChild(
      fmt::format("{}.{}", connectorPool->name(), writerId.toString()));
}

std::shared_ptr<memory::MemoryPool> createSinkPool(
    const std::shared_ptr<memory::MemoryPool>& writerPool) {
  return writerPool->addLeafChild(fmt::format("{}.sink", writerPool->name()));
}

uint32_t FileSystemDataSink::appendWriter(const FsWriterId& id) {
  // Check max open writers.
  VELOX_USER_CHECK_LE(
      writers_.size(), maxOpenWriters_, "Exceeded open writer limit");
  VELOX_CHECK_EQ(writers_.size(), writerInfo_.size());
  VELOX_CHECK_EQ(writerIndexMap_.size(), writerInfo_.size());

  std::optional<std::string> partitionName;
  if (isPartitioned()) {
    partitionName =
        partitionIdGenerator_->fsPartitionName(id.partitionId.value());
  }

  // Without explicitly setting flush policy, the default memory based flush
  // policy is used.
  auto writerParameters = getWriterParameters(partitionName);
  const auto writePath = fs::path(writerParameters.writeDirectory()) /
      writerParameters.writeFileName();
  auto writerPool = createWriterPool(id);
  auto sinkPool = createSinkPool(writerPool);
  writerInfo_.emplace_back(std::make_shared<FsWriterInfo>(
      std::move(writerParameters),
      std::move(writerPool),
      std::move(sinkPool),
      std::time(nullptr)));
  ioStats_.emplace_back(std::make_shared<io::IoStatistics>());
  // setMemoryReclaimers(writerInfo_.back().get(), ioStats_.back().get());
  auto writer = createWriter(writePath, writerInfo_.back(), ioStats_.back());
  writers_.emplace_back(std::move(writer));
  // Extends the buffer used for partition rows calculations.
  partitionSizes_.emplace_back(0);
  partitionRows_.emplace_back(nullptr);
  rawPartitionRows_.emplace_back(nullptr);

  writerIndexMap_.emplace(id, writers_.size() - 1);
  return writerIndexMap_[id];
}

uint32_t FileSystemDataSink::updateWriter(const FsWriterId& id) {
  VELOX_USER_CHECK_LE(
      writers_.size(), maxOpenWriters_, "Exceeded open writer limit");
  VELOX_CHECK_EQ(writers_.size(), writerInfo_.size());
  VELOX_CHECK_EQ(writerIndexMap_.size(), writerInfo_.size());
  auto it = writerIndexMap_.find(id);
  VELOX_CHECK(it != writerIndexMap_.end());
  uint32_t index = it->second;
  const auto writerInfo = writerInfo_[index];
  const std::optional<std::string> partitionName =
      writerInfo->writerParameters.partitionName();
  auto writerParameters = getWriterParameters(partitionName);
  if (!writerInfo_[index]->isCommitted()) {
    pendingWriterInfo_.emplace_back(writerInfo_[index]);
  }
  writerInfo_[index] = std::make_shared<FsWriterInfo>(
      std::move(writerParameters),
      writerInfo_[index]->writerPool,
      writerInfo_[index]->sinkPool,
      std::time(nullptr));
  const auto writePath = fs::path(writerParameters.writeDirectory()) /
      writerParameters.writeFileName();
  auto writer = createWriter(writePath, writerInfo_[index], ioStats_[index]);
  writers_[index] = std::move(writer);
  return index;
}

uint32_t FileSystemDataSink::ensureWriter(const FsWriterId& id) {
  auto it = writerIndexMap_.find(id);
  if (it != writerIndexMap_.end()) {
    uint32_t index = it->second;
    if (writerInfo_[index]->shouldUpdateWriter(writeConfig_)) {
      if (!writeConfig_->flushOnWrite()) {
        writers_[index]->flush();
      }
      writers_[index]->close();
      return updateWriter(id);
    } else {
      return index;
    }
  }
  return appendWriter(id);
}

RowVectorPtr makeDataInput(
    const std::vector<column_index_t>& dataCols,
    const RowVectorPtr& input) {
  std::vector<VectorPtr> childVectors;
  childVectors.reserve(dataCols.size());
  for (int dataCol : dataCols) {
    childVectors.push_back(input->childAt(dataCol));
  }

  return std::make_shared<RowVector>(
      input->pool(),
      getNonPartitionTypes(dataCols, asRowType(input->type())),
      input->nulls(),
      input->size(),
      std::move(childVectors),
      input->getNullCount());
}

void FileSystemDataSink::write(size_t index, RowVectorPtr input) {
  WRITER_NON_RECLAIMABLE_SECTION_GUARD(index);
  auto dataInput = makeDataInput(dataChannels_, input);
  writers_[index]->write(dataInput);
  if (writeConfig_->flushOnWrite()) {
    writers_[index]->flush();
  }
  if (writeConfig_->getFileCompressionType() == common::CompressionKind_NONE) {
    writerInfo_[index]->inputSizeInBytes += dataInput->inMemoryBytes();
  } else {
    writerInfo_[index]->inputSizeInBytes += dataInput->estimateFlatSize();
  }
  writerInfo_[index]->numWrittenRows += dataInput->size();
}

void FileSystemDataSink::computePartitionIds(const RowVectorPtr& input) {
  VELOX_CHECK(isPartitioned());
  if (isPartitioned()) {
    if (!writeConfig_->allowNullPartitionKeys()) {
      // Check that there are no nulls in the partition keys.
      for (auto& partitionIdx : partitionChannels_) {
        auto col = input->childAt(partitionIdx);
        if (col->mayHaveNulls()) {
          for (auto i = 0; i < col->size(); ++i) {
            VELOX_USER_CHECK(
                !col->isNullAt(i),
                "Partition key must not be null: {}",
                input->type()->asRow().nameOf(partitionIdx));
          }
        }
      }
    }
    partitionIdGenerator_->run(input, partitionIds_);
  }
}

FsWriterId FileSystemDataSink::getWriterId(size_t row) const {
  std::optional<int32_t> partitionId;
  if (isPartitioned()) {
    VELOX_CHECK_LT(partitionIds_[row], std::numeric_limits<uint32_t>::max());
    partitionId = static_cast<uint32_t>(partitionIds_[row]);
  }
  return FsWriterId{partitionId};
}

void FileSystemDataSink::splitInputRowsAndEnsureWriters() {
  VELOX_CHECK(isPartitioned());

  std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);

  const auto numRows = isPartitioned() ? partitionIds_.size() : 0;
  for (auto row = 0; row < numRows; ++row) {
    const auto id = getWriterId(row);
    const uint32_t index = ensureWriter(id);

    VELOX_DCHECK_LT(index, partitionSizes_.size());
    VELOX_DCHECK_EQ(partitionSizes_.size(), partitionRows_.size());
    VELOX_DCHECK_EQ(partitionRows_.size(), rawPartitionRows_.size());
    if (FOLLY_UNLIKELY(partitionRows_[index] == nullptr) ||
        (partitionRows_[index]->capacity() < numRows * sizeof(vector_size_t))) {
      partitionRows_[index] = allocateIndices(numRows, queryCtx_->memoryPool());
      rawPartitionRows_[index] =
          partitionRows_[index]->asMutable<vector_size_t>();
    }
    rawPartitionRows_[index][partitionSizes_[index]] = row;
    ++partitionSizes_[index];
  }

  for (uint32_t i = 0; i < partitionSizes_.size(); ++i) {
    if (partitionSizes_[i] != 0) {
      VELOX_CHECK_NOT_NULL(partitionRows_[i]);
      partitionRows_[i]->setSize(partitionSizes_[i] * sizeof(vector_size_t));
    }
  }
}

void FileSystemDataSink::appendData(RowVectorPtr input) {
  checkRunning();
  // Write to unpartitioned table.
  if (!isPartitioned()) {
    const FsWriterId writerId{0};
    const auto index = ensureWriter(writerId);
    write(index, input);
    return;
  }
  // Compute partition numbers.
  computePartitionIds(input);

  // Lazy load all the input columns.
  for (column_index_t i = 0; i < input->childrenSize(); ++i) {
    input->childAt(i)->loadedVector();
  }
  // All inputs belong to a single non-bucketed partition. The partition id
  // must be zero.
  if (partitionIdGenerator_->numPartitions() == 1) {
    const auto index = ensureWriter(FsWriterId{0});
    write(index, input);
    return;
  }
  splitInputRowsAndEnsureWriters();
  for (auto index = 0; index < writers_.size(); ++index) {
    const vector_size_t partitionSize = partitionSizes_[index];
    if (partitionSize == 0) {
      continue;
    }

    RowVectorPtr writerInput = partitionSize == input->size()
        ? input
        : exec::wrap(partitionSize, partitionRows_[index], input);
    write(index, writerInput);
  }
}

void FileSystemDataSink::setState(State newState) {
  checkStateTransition(state_, newState);
  state_ = newState;
}

/// Validates the state transition from 'oldState' to 'newState'.
void FileSystemDataSink::checkStateTransition(State oldState, State newState) {
  switch (oldState) {
    case State::kRunning:
      if (newState == State::kAborted || newState == State::kFinishing) {
        return;
      }
      break;
    case State::kFinishing:
      if (newState == State::kAborted || newState == State::kClosed ||
          // The finishing state is reentry state if we yield in the middle of
          // finish processing if a single run takes too long.
          newState == State::kFinishing) {
        return;
      }
      [[fallthrough]];
    case State::kAborted:
    case State::kClosed:
    default:
      break;
  }
  VELOX_FAIL(
      "Unexpected state transition from {} to {}",
      stateString(oldState),
      stateString(newState));
}

bool FileSystemDataSink::finish() {
  // Flush is reentry state.
  setState(State::kFinishing);
  return true;
}

void FileSystemDataSink::abort() {
  setState(State::kAborted);
  closeInternal();
}

void FileSystemDataSink::closeInternal() {
  VELOX_CHECK_NE(stateString(state_), "RUNNING");
  VELOX_CHECK_NE(stateString(state_), "FLUSHING");

  if (state_ == State::kClosed) {
    for (int i = 0; i < writers_.size(); ++i) {
      WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
      writers_[i]->close();
    }
  } else {
    for (int i = 0; i < writers_.size(); ++i) {
      WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
      writers_[i]->abort();
    }
  }
}

std::vector<std::string> FileSystemDataSink::commit(int64_t id) {
  for (const auto& writerInfo : writerInfo_) {
    if (!writerInfo->isCommitted()) {
      pendingWriterInfo_.emplace_back(writerInfo);
    }
  }
  std::vector<std::string> committed;
  for (const auto& writerInfo : pendingWriterInfo_) {
    if (writerInfo->isCommitted()) {
      continue;
    }
    const auto writerParams = writerInfo->writerParameters;
    const auto writeFileName =
        writerParams.writeDirectory() + "/" + writerParams.writeFileName();
    const auto targetFileName =
        writerParams.targetDirectory() + "/" + writerParams.targetFileName();
    if (!fs_) {
      fs_ = filesystems::getFileSystem(writeFileName, writeConfig_->config());
    }
    try {
      fs_->rename(writeFileName, targetFileName);
    } catch (const std::exception& e) {
      VELOX_FAIL(
          "Failed to rename file {} to target {}, exception: {}",
          writeFileName,
          targetFileName,
          e.what());
    }
    std::optional<std::string> partitionName = writerParams.partitionName();
    int64_t partitionTimestamp = 0;
    if (partitionName.has_value() &&
        writeConfig_->getPartitionCommitTrigger() == "partition-time") {
      partitionTimestamp =
          extractTimestampFromPartitionName(partitionName.value());
    }
    int64_t commitDelaySeconds =
        writeConfig_->getPartitionCommitDelayMinutes() * 60;
    if (partitionTimestamp != 0 &&
        watermark_ > partitionTimestamp + commitDelaySeconds) {
      committed.emplace_back(partitionName.value());
    } else {
      committed.emplace_back(partitionName.value());
    }
    writerInfo->setCommitted(true);
  }
  pendingWriterInfo_.clear();
  return committed;
}

connector::DataSink::Stats FileSystemDataSink::stats() const {
  Stats stats;
  if (state_ == State::kAborted) {
    return stats;
  }

  int64_t numWrittenBytes{0};
  int64_t writeIOTimeUs{0};
  for (const auto& ioStats : ioStats_) {
    numWrittenBytes += ioStats->rawBytesWritten();
    writeIOTimeUs += ioStats->writeIOTimeUs();
  }
  stats.numWrittenBytes = numWrittenBytes;
  stats.writeIOTimeUs = writeIOTimeUs;

  if (state_ != State::kClosed) {
    return stats;
  }

  stats.numWrittenFiles = writers_.size();
  for (int i = 0; i < writerInfo_.size(); ++i) {
    const auto& info = writerInfo_.at(i);
    VELOX_CHECK_NOT_NULL(info);
    const auto spillStats = info->spillStats->rlock();
    if (!spillStats->empty()) {
      stats.spillStats += *spillStats;
    }
  }
  return stats;
}

std::vector<std::string> FileSystemDataSink::close() {
  setState(State::kClosed);
  closeInternal();
  return {};
}

const int64_t FileSystemDataSink::extractTimestampFromPartitionName(
    const std::string& partitionName) {
  std::string extractPattern = writeConfig_->getPartitionTimeExtractPattern();
  if (extractPattern.empty()) {
    return 0;
  }
  std::string pName = partitionName;
  std::regex urlEncodedPattern("%[0-9A-Fa-f]{2}");
  if (std::regex_search(pName, urlEncodedPattern)) {
    std::string result;
    for (size_t i = 0; i < pName.size(); ++i) {
      if (pName[i] == '%') {
        if (i + 2 < pName.size()) {
          int hexValue;
          std::istringstream hexStream(pName.substr(i + 1, 2));
          if (hexStream >> std::hex >> hexValue) {
            result += static_cast<char>(hexValue);
            i += 2;
          } else {
            result += pName[i];
          }
        } else {
          result += pName[i];
        }
      } else if (pName[i] == '+') {
        result += ' ';
      } else {
        result += pName[i];
      }
    }
    pName = result;
  }
  std::vector<std::string> partitionKVs;
  boost::algorithm::split(
      partitionKVs, pName, boost::algorithm::is_any_of("/"));
  VELOX_CHECK(partitionKVs.size() == partitionKeys_.size());
  for (size_t i = 0; i < partitionKVs.size(); ++i) {
    boost::algorithm::replace_all(partitionKVs[i], partitionKeys_[i] + "=", "");
    boost::algorithm::replace_all(
        extractPattern, "$" + partitionKeys_[i], partitionKVs[i]);
  }
  const auto timestamp =
      util::fromTimestampString(
          extractPattern.data(),
          extractPattern.size(),
          util::TimestampParseMode::kLegacyCast)
          .thenOrThrow(folly::identity, [&](const Status& status) {
            VELOX_FAIL("error while parse timestamp: {}", status.message());
          });
  return timestamp.getSeconds();
}

const std::pair<std::string, std::string> FsFileNameGenerator::gen() const {
  std::pair<std::string, std::string> fileNames;
  std::stringstream targetFileName;
  std::stringstream writeFileName;
  targetFileName << "part-" << prefix_ << "-" << taskId_ << "-" << partCounter_
                 << suffix_;
  boost::uuids::random_generator generator;
  boost::uuids::uuid uuid = generator();
  writeFileName << "." << targetFileName.str() << ".inprogress."
                << to_string(uuid);
  fileNames.first = targetFileName.str();
  fileNames.second = writeFileName.str();
  partCounter_++;
  return fileNames;
}

} // namespace facebook::velox::connector::filesystem
