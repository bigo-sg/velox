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

#include <algorithm>
#include <cctype>
#include <chrono>
#include <limits>
#include <unordered_map>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::connector::filesystem {

namespace {

std::string normalizeDurationUnit(std::string unit) {
  std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);

  static const std::unordered_map<std::string, std::string> aliases = {
      {"min", "m"},          {"mins", "m"},          {"minute", "m"},
      {"minutes", "m"},      {"sec", "s"},           {"secs", "s"},
      {"second", "s"},       {"seconds", "s"},       {"hr", "h"},
      {"hrs", "h"},          {"hour", "h"},          {"hours", "h"},
      {"day", "d"},          {"days", "d"},          {"millis", "ms"},
      {"millisecond", "ms"}, {"milliseconds", "ms"}, {"micro", "us"},
      {"micros", "us"},      {"microsecond", "us"},  {"microseconds", "us"},
      {"nano", "ns"},        {"nanos", "ns"},        {"nanosecond", "ns"},
      {"nanoseconds", "ns"},
  };

  if (auto it = aliases.find(unit); it != aliases.end()) {
    return it->second;
  }
  return unit;
}

std::string normalizeDurationConfigValue(const std::string& value) {
  size_t pos = 0;
  while (pos < value.size() && std::isspace(value[pos])) {
    ++pos;
  }
  const size_t numberStart = pos;
  while (pos < value.size() &&
         (std::isdigit(value[pos]) || value[pos] == '.')) {
    ++pos;
  }
  VELOX_USER_CHECK_GT(pos, numberStart, "Invalid duration '{}'", value);
  const std::string number = value.substr(numberStart, pos - numberStart);
  while (pos < value.size() && std::isspace(value[pos])) {
    ++pos;
  }
  VELOX_USER_CHECK_LT(
      pos, value.size(), "Missing unit in duration '{}'", value);
  const std::string unit = normalizeDurationUnit(value.substr(pos));
  return number + unit;
}

int32_t parseDurationConfigToMillis(
    const std::string& configVal,
    const char* configKey) {
  const auto duration =
      config::toDuration(normalizeDurationConfigValue(configVal));
  const auto millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  VELOX_USER_CHECK_LE(
      millis,
      std::numeric_limits<int32_t>::max(),
      "The duration for config '{}' must fit in int32. Got {} ms.",
      configKey,
      millis);
  return static_cast<int32_t>(millis);
}

} // namespace

template <typename T, bool throwException>
const T FileSystemWriteConfig::checkAndGetConfigValue(
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

const dwio::common::FileFormat FileSystemWriteConfig::getFormat() {
  const std::string format =
      checkAndGetConfigValue<std::string, false>(kFormat, "");
  if (supportedFileFormats.find(format) != supportedFileFormats.end()) {
    return supportedFileFormats.at(format);
  } else {
    VELOX_FAIL("Format {} not supported for filesystem sink.", format);
  }
}

const std::string FileSystemWriteConfig::getPath() {
  return checkAndGetConfigValue<std::string, false>(kPath, "");
}

const int32_t FileSystemWriteConfig::getFileRollingIntervalMillis() {
  const std::string configVal =
      checkAndGetConfigValue<std::string, false>(kFileRollingInterval, "1min");
  return parseDurationConfigToMillis(configVal, kFileRollingInterval);
}

const std::string FileSystemWriteConfig::getPartitionCommitTrigger() {
  const std::string trigger = checkAndGetConfigValue<std::string, false>(
      kPartitionCommitTrigger, "process-time");
  if (trigger == "process-time" || trigger == "partition-time") {
    return trigger;
  }
  VELOX_USER_FAIL(
      "Unsupported {} '{}'. Supported values are 'process-time' and 'partition-time'.",
      kPartitionCommitTrigger,
      trigger);
}

const std::string FileSystemWriteConfig::getPartitionCommitPolicy() {
  return checkAndGetConfigValue<std::string, false>(
      kPartitionCommitPolicy, "metastore");
}

const int32_t FileSystemWriteConfig::getPartitionCommitDelayMillis() {
  const std::string configVal =
      checkAndGetConfigValue<std::string, false>(kPartitionCommitDelay, "1min");
  return parseDurationConfigToMillis(configVal, kPartitionCommitDelay);
}

const std::string FileSystemWriteConfig::getPartitionTimeExtractPattern() {
  return checkAndGetConfigValue<std::string, false>(
      kPartitionTimeExtractPattern, "");
}

const common::CompressionKind FileSystemWriteConfig::getFileCompressionType() {
  std::string compression = checkAndGetConfigValue<std::string, false>(
      kParquetCompressionCodec,
      checkAndGetConfigValue<std::string, false>(kFileCompression, "none"));
  std::transform(
      compression.begin(), compression.end(), compression.begin(), ::tolower);

  const common::CompressionKind compressionKind =
      common::stringToCompressionKind(compression);
  if (compressionKind == common::CompressionKind_NONE) {
    return compressionKind;
  }

  VELOX_USER_CHECK_EQ(
      getFormat(),
      dwio::common::FileFormat::PARQUET,
      "File compression '{}' is only supported for parquet filesystem sink.",
      compression);
  VELOX_USER_CHECK(
      compressionKind == common::CompressionKind_SNAPPY ||
          compressionKind == common::CompressionKind_LZ4 ||
          compressionKind == common::CompressionKind_ZSTD,
      "Unsupported parquet compression codec '{}'. Supported values are none, snappy, lz4 and zstd.",
      compression);
  return compressionKind;
}

const int64_t FileSystemWriteConfig::getFileRollingSize() {
  const std::string configVal =
      checkAndGetConfigValue<std::string, false>(kFileRollingSize, "128MB");
  const auto sizeInBytes =
      config::toCapacity(configVal, config::CapacityUnit::BYTE);
  VELOX_USER_CHECK_LE(
      sizeInBytes,
      std::numeric_limits<int64_t>::max(),
      "The file rolling size for config '{}' must fit in int64. Got {} bytes.",
      kFileRollingSize,
      sizeInBytes);
  return static_cast<int64_t>(sizeInBytes);
}

} // namespace facebook::velox::connector::filesystem
