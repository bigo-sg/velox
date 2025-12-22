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

#include "velox/connectors/filesystem/FileSystemConfig.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox::connector::filesystem {

template <typename T, bool throwException>
const T FileSystemConfig::checkAndGetConfigValue(
    const std::string& configKey,
    const T& defaultValue) const {
  std::optional<T> configValue =
      static_cast<std::optional<T>>(config_->get<T>(configKey));
  if constexpr (throwException) {
    VELOX_CHECK_EQ(
        configValue.has_value(),
        true,
        "FileSystem config {} has no specified value.",
        configKey);
  }
  if (configValue.has_value()) {
    return configValue.value();
  } else {
    return defaultValue;
  }
}

const dwio::common::FileFormat FileSystemConfig::getFormat() {
  const std::string format = checkAndGetConfigValue<std::string, false>(kFormat, "");
  if (supportedFileFormats.find(format) != supportedFileFormats.end()) {
    return supportedFileFormats.at(format);
  } else {
    VELOX_FAIL("Format {} not supported for filesystem sink.", format);
  }
}

const std::string FileSystemConfig::getPath() {
  return checkAndGetConfigValue<std::string, false>(kPath, "");
}

const int32_t FileSystemWriteConfig::getFileRollingIntervalMinutes() {
  const std::string configVal =
      checkAndGetConfigValue<std::string, false>(kFileRollingInterval, "1min");
  const std::string intVal = configVal.substr(0, configVal.size() - 3);
  return std::stoi(intVal);
}

const std::string FileSystemWriteConfig::getPartitionCommitTrigger() {
  return checkAndGetConfigValue<std::string, false>(
      kPartitionCommitTrigger, "process-time");
}

const int32_t FileSystemWriteConfig::getPartitionCommitDelayMinutes() {
  const std::string configVal =
      checkAndGetConfigValue<std::string, false>(kPartitionCommitDelay, "1min");
  const std::string intVal = configVal.substr(0, configVal.size() - 3);
  return std::stoi(intVal);
}

const std::string FileSystemWriteConfig::getPartitionTimeExtractPattern() {
  return checkAndGetConfigValue<std::string, false>(
      kPartitionTimeExtractPattern, "");
}

const int32_t FileSystemWriteConfig::getFileRollingSize() {
  const std::string configVal =
      checkAndGetConfigValue<std::string, false>(kFileRollingSize, "128MB");
  const std::string sizeUnit =
      configVal.substr(configVal.size() - 2, configVal.size());
  std::string intVal = configVal.substr(0, configVal.size() - 2);
  if (sizeUnit == "GB") {
    return std::stoi(intVal) * 1024 * 1024 * 1024;
  } else if (sizeUnit == "MB") {
    return std::stoi(intVal) * 1024 * 1024;
  } else if (sizeUnit == "KB") {
    return std::stoi(intVal) * 1024;
  } else {
    const std::string byteUnit =
        configVal.substr(configVal.size() - 1, configVal.size());
    if (byteUnit == "B" && ((sizeUnit.at(0) >= '0' && sizeUnit.at(0) <= '9') ||
                            sizeUnit.at(0) == ' ')) {
      intVal = configVal.substr(0, configVal.size() - 1);
      return std::stoi(intVal);
    } else {
      VELOX_UNSUPPORTED(
          "The unit for config {} only support GB/MB/KB/B", kFileRollingSize);
    }
  }
}

const std::string FileSystemReadConfig::getFieldDelimiter() {
  return checkAndGetConfigValue<std::string, false>(
      kTextFormatFieldDelimiter, defaultTextFormatFieldDelimiter);
}

const uint64_t FileSystemReadConfig::getMaxReadRows() {
  return checkAndGetConfigValue<uint64_t, false>(
      kMaxReadRows, defaultMaxReadRows);
}

const uint64_t FileSystemReadConfig::getMaxReadBytes() {
  return checkAndGetConfigValue<uint64_t, false>(
      kMaxReadBytes, defaultMaxReadBytes);
}

} // namespace facebook::velox::connector::filesystem