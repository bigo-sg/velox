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
#include <cppkafka/cppkafka.h>

namespace facebook::velox::connector::kafka {

using ConfigPtr = std::shared_ptr<const config::ConfigBase>;

/// Kafka config base class.
class KafkaConfig {
 public:
  KafkaConfig(const ConfigPtr& config) : config_(config) {}

  const ConfigPtr& getConfig() const {
    return config_;
  }

  const bool exists(const std::string& configKey) const {
    return config_ && config_->valueExists(configKey);
  }

  const bool empty() const {
    if (!config_) {
      return true;
    } else {
      return config_->rawConfigs().empty();
    }
  }

  template <typename T>
  const std::shared_ptr<T> updateAndGetAllConfigs(
      const std::unordered_map<std::string, std::string>& configs) const {
    std::unordered_map<std::string, std::string> rawConfigs = config_->rawConfigsCopy();
    rawConfigs.insert(configs.begin(), configs.end());
    ConfigPtr newConfig =
        std::make_shared<const config::ConfigBase>(std::move(rawConfigs));
    return std::make_shared<T>(newConfig);
  }

 protected:
  ConfigPtr config_;
  template <typename T, bool throwException>
  const T checkAndGetConfigValue(const std::string& configKey, const T& defaultValue) const;
};

/// Kafka connector config.
class ConnectionConfig : public KafkaConfig {
 public:
  ConnectionConfig(const ConfigPtr& config) : KafkaConfig(config) {}
  /// The config key of bootstrap servers
  static constexpr const char* kBootstrapServers = "bootstrap.servers";
  /// The config key of topic
  static constexpr const char* kTopic = "topic";
  /// The config key of group id
  static constexpr const char* kGroupId = "group.id";
  /// The config key of client id
  static constexpr const char* kClientId = "client.id";
  /// The config key fo format
  static constexpr const char* kFormat = "format";
  /// The config key of auto offset reset
  static constexpr const char* kAutoResetOffset = "auto.offset.reset";
  /// The config key of minimum number of messages of the queue buffer
  static constexpr const char* kQueueMinMessages = "queued.min.messages";
  /// The config key of whether to enable auto commit kafka coffset
  static constexpr const char* kEnableAutoCommit = "enable.auto.commit";
  /// The config key of whether to ignore partition eof
  static constexpr const char* kEnablePartitionEof = "enable.partition.eof";
  /// The config key of max batch size to poll kafka messages.
  static constexpr const char* kDataBatchSize = "data.batch.size";
  /// The config key of timeout milliseconds to poll kafka messages.
  static constexpr const char* kPollTimeoutMills = "poll.timeout.mills";
  /// The config key of queue buffer size of cppkafka client
  static constexpr const char* kConsumeMessageQueueSize = "consume.queue.size";
  /// The startup mode of kafka consumer, its value canbe `group-offsets`,
  /// `latest-offsets`, `earliest-offsets`, `timestamp`.
  static constexpr const char* kStartupMode = "scan.startup.mode";
  /// The config of kafka client, to define the default value of minimum
  /// messages size of kafka client queue.
  static constexpr const uint32_t defaultQueuedMinMessages = 1000000;
  /// The config of the kafka client, to define the default software name of
  /// client.
  static constexpr const char* defaultClientSoftwareName = "velox";
  /// The config of the kafka client, to define the default version of client.
  static constexpr const char* defaultClientSoftwareVersion = "***";
  /// Define the default batch size of a data process.
  static constexpr const uint32_t defaultDataBatchSize = 500;
  /// Define the default poll batch size of kafka client.
  static constexpr const uint32_t defaultPollBatchSize = 500;
  /// The config of the kafka client, to define the default timeout millseconds
  /// of a single consumption.
  static constexpr const uint32_t defaultPollTimeoutMills = 100;
  /// The config of kafka client, to define the default value of config
  /// `auto.offset.reset`
  static constexpr const char* defaultAutoOffsetRest = "latest";
  /// The config of kafka client, to define the default value of config
  /// `scan.startup.mode`
  static constexpr const char* defaultConsumeStartupMode = "group-offsets";

  const std::string getBootstrapServers() const;
  const std::string getTopic() const;
  const std::string getGroupId() const;
  const std::string getClientId() const;
  const std::string getFormat() const;
  const std::string getAutoOffsetReset() const;
  const uint32_t getQueuedMinMessages() const;
  const bool getEnableAutoCommit() const;
  const bool getEnablePartitionEof() const;
  const uint32_t getDataBatchSize() const;
  const uint32_t getPollTimeoutMills() const;
  const uint32_t getConsumeQueueSize() const;
  const std::string getStartupMode() const;
  /// Get the configuration for kafka client to consume.
  cppkafka::Configuration getCppKafkaConfiguration() const;
};

/// The json format config for kafka.
class JSONFormatConfig : public KafkaConfig {
 public:
  JSONFormatConfig(const ConfigPtr& config) : KafkaConfig(config) {}
};

/// The csv format config for kafka.
class CSVFormatConfig : public KafkaConfig {};

using KafkaConfigPtr = std::shared_ptr<KafkaConfig>;
using ConnectionConfigPtr = std::shared_ptr<ConnectionConfig>;
using JSONFormatConfigPtr = std::shared_ptr<JSONFormatConfig>;
using CSVFormatConfigPtr = std::shared_ptr<CSVFormatConfig>;

} // namespace facebook::velox::connector::kafka
