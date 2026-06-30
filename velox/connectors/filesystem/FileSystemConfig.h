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

#include <unordered_map>
#include "velox/common/compression/Compression.h"
#include "velox/common/config/Config.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox::connector::filesystem {

using ConfigPtr = std::shared_ptr<const config::ConfigBase>;

class FileSystemWriteConfig {
 public:
  FileSystemWriteConfig(const ConfigPtr& config) : config_(config) {}

  static constexpr const char* kPath = "path";
  /// The config key fo format
  static constexpr const char* kFormat = "format";
  static constexpr const char* kFileRollingInterval =
      "sink.rolling-policy.rollover-interval";
  static constexpr const char* kFileRollingSize =
      "sink.rolling-policy.file-size";
  static constexpr const char* kPartitionCommitTrigger =
      "sink.partition-commit.trigger";
  /// Partition commit policy (e.g. "metastore", "success-file"). Passed through
  /// from Flink for configuration parity; Velox does not act on this value.
  /// Writing _SUCCESS markers and metastore updates are handled by Flink after
  /// it consumes commit() return values.
  static constexpr const char* kPartitionCommitPolicy =
      "sink.partition-commit.policy.kind";
  static constexpr const char* kPartitionCommitDelay =
      "sink.partition-commit.delay";
  static constexpr const char* kPartitionTimeExtractPattern =
      "partition.time-extractor.timestamp-pattern";
  static constexpr const char* kFileCompression = "sink.file.compression";
  static constexpr const char* kParquetCompressionCodec =
      "parquet.compression-codec";
  /// The default value of max partitions per writer.
  static constexpr const int32_t defaultMaxPartitionsPerWriter = 65535;
  /// The supported file format to write
  const std::unordered_map<std::string, dwio::common::FileFormat>
      supportedFileFormats = {
          {"csv", dwio::common::FileFormat::TEXT},
          {"parquet", dwio::common::FileFormat::PARQUET},
          {"orc", dwio::common::FileFormat::ORC}};

  const std::string getPath();
  const dwio::common::FileFormat getFormat();
  const bool allowNullPartitionKeys() {
    return false;
  }
  const int32_t maxPartitionsPerWriter() {
    return defaultMaxPartitionsPerWriter;
  }
  const bool isPartitionPathAsLowerCase() {
    return true;
  }
  const int32_t getFileRollingIntervalMillis();
  const int64_t getFileRollingSize();
  const std::string getPartitionCommitTrigger();
  /// Returns sink.partition-commit.policy.kind for Flink-side use only.
  const std::string getPartitionCommitPolicy();
  const int32_t getPartitionCommitDelayMillis();
  const std::string getPartitionTimeExtractPattern();
  const common::CompressionKind getFileCompressionType();
  const bool flushOnWrite() {
    return true;
  }
  const bool exists(const std::string& configKey) {
    return config_ && config_->valueExists(configKey);
  }

  const ConfigPtr& config() {
    return config_;
  }

  template <typename T>
  const std::shared_ptr<T> updateAndGetAllConfigs(
      const std::unordered_map<std::string, std::string>& configs) const {
    std::unordered_map<std::string, std::string> rawConfigs =
        config_->rawConfigsCopy();
    rawConfigs.insert(configs.begin(), configs.end());
    ConfigPtr newConfig =
        std::make_shared<const config::ConfigBase>(std::move(rawConfigs));
    return std::make_shared<T>(newConfig);
  }

 private:
  ConfigPtr config_;

  template <typename T, bool throwException>
  const T checkAndGetConfigValue(
      const std::string& configKey,
      const T& defaultValue) const;
};
} // namespace facebook::velox::connector::filesystem
