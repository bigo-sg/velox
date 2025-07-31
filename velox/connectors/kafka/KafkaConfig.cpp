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

#include "velox/connectors/kafka/KafkaConfig.h"

namespace facebook::velox::connector::kafka {

template <typename T, bool throwException>
const T KafkaConfig::checkAndGetConfigValue(
    const std::string& configKey,
    T defaultValue) const {
  std::optional<T> configValue =
      static_cast<std::optional<T>>(config_->get<T>(configKey));
  if constexpr (throwException) {
    VELOX_CHECK_EQ(
        configValue.has_value(),
        true,
        "Kafka config {} has no specified value.",
        configKey);
  }
  if (configValue.has_value()) {
    return configValue.value();
  } else {
    return defaultValue;
  }
}

const std::string ConnectionConfig::getBootstrapServers() const {
  return checkAndGetConfigValue<std::string, true>(kBootstrapServers, "");
}

const std::string ConnectionConfig::getTopic() const {
  return checkAndGetConfigValue<std::string, true>(kTopic, "");
}

const std::string ConnectionConfig::getGroupId() const {
  return checkAndGetConfigValue<std::string, true>(kGroupId, "");
}

const std::string ConnectionConfig::getClientId() const {
  return checkAndGetConfigValue<std::string, true>(kClientId, "");
}

const std::string ConnectionConfig::getFormat() const {
  return checkAndGetConfigValue<std::string, true>(kFormat, "");
}

const std::string ConnectionConfig::getAutoOffsetReset() const {
  return checkAndGetConfigValue<std::string, false>(
      kAutoResetOffset, defaultAutoOffsetRest);
}

const uint32_t ConnectionConfig::getQueuedMinMessages() const {
  return checkAndGetConfigValue<uint32_t, false>(
      kQueueMinMessages, defaultQueuedMinMessages);
}

const bool ConnectionConfig::getEnableAutoCommit() const {
  return checkAndGetConfigValue<std::string, false>(kEnableAutoCommit, "true") ==
                 "true"
             ? true
             : false;
}

const bool ConnectionConfig::getEnablePartitionEof() const {
  return checkAndGetConfigValue<bool, false>(kEnablePartitionEof, false);
}

const uint32_t ConnectionConfig::getPollMaxBatchSize() const {
  return checkAndGetConfigValue<uint32_t, false>(
      kPollMaxBatchSize, defaultPollMaxBatchSize);
}

const bool ConnectionConfig::getEnableAccumulateDataBatch() const {
  return checkAndGetConfigValue<std::string, false>(kEnableAccumulateDataBatch, "true") ==
                 "true"
             ? true
             : false;
}

const uint32_t ConnectionConfig::getPollTimeoutMills() const {
  return checkAndGetConfigValue<uint32_t, false>(
      kPollTimeoutMills, defaultPollTimeoutMills);
}

const std::string ConnectionConfig::getStartupMode() const {
  return checkAndGetConfigValue<std::string, false>(
      kStartupMode, defaultConsumeStartupMode);
}

cppkafka::Configuration ConnectionConfig::getCppKafkaConfiguration() const {
  cppkafka::Configuration conf;
  conf.set("metadata.broker.list", getBootstrapServers());
  conf.set("group.id", getGroupId());
  conf.set("client.id", getClientId());
  conf.set("client.software.name", defaultClientSoftwareName);
  conf.set("client.software.version", defaultClientSoftwareVersion);
  conf.set("auto.offset.reset", getAutoOffsetReset());
  conf.set("queued.min.messages", getQueuedMinMessages());
  conf.set("enable.auto.commit", getEnableAutoCommit());
  conf.set("auto.commit.interval.ms", 2000);
  conf.set("enable.partition.eof", getEnablePartitionEof());
  return conf;
}
} // namespace facebook::velox::connector::kafka