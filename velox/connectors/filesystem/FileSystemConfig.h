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
#include "velox/common/compression/Compression.h"

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
  static constexpr const char* kPartitionCommitPolicy =
      "sink.partition-commit.policy.kind";
  static constexpr const char* kPartitionCommitDelay =
      "sink.partition-commit.delay";
  static constexpr const char* kPartitionTimeExtractPattern =
      "partition.time-extractor.timestamp-pattern";
  /// The default value of max partitions per writer.
  static constexpr const int32_t defaultMaxPartitionsPerWriter = 65535;

  const std::string getPath();
  const std::string getFormat();
  const bool allowNullPartitionKeys() {
    return false;
  }
  const int32_t maxPartitionsPerWriter() {
    return defaultMaxPartitionsPerWriter;
  }
  const bool isPartitionPathAsLowerCase() {
    return true;
  }
  const int32_t getFileRollingIntervalMinutes();
  const int32_t getFileRollingSize();
  const std::string getPartitionCommitTrigger();
  const int32_t getPartitionCommitDelayMinutes();
  const std::string getPartitionTimeExtractPattern();
  const common::CompressionKind getFileCompressionType() {
    return common::CompressionKind_NONE;
  }
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
  const std::shared_ptr<T> setConfigs(
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
  const T checkAndGetConfigValue(const std::string& configKey, T defaultValue)
      const;
};
} // namespace facebook::velox::connector::filesystem
