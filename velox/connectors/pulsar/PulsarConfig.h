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
#include <pulsar/ClientConfiguration.h>
#include <pulsar/ConsumerConfiguration.h>

namespace facebook::velox::connector::pulsar {

using ConfigPtr = std::shared_ptr<const config::ConfigBase>;

class PulsarConfig {
 public:
  explicit PulsarConfig(const ConfigPtr& config) : config_(config) {}

  const ConfigPtr& getConfig() const {
    return config_;
  }

  bool exists(const std::string& configKey) const {
    return config_ && config_->valueExists(configKey);
  }

  template <typename T>
  std::shared_ptr<T> updateAndGetAllConfigs(
      const std::unordered_map<std::string, std::string>& configs) const {
    std::unordered_map<std::string, std::string> rawConfigs =
        config_ ? config_->rawConfigsCopy()
                : std::unordered_map<std::string, std::string>();
    rawConfigs.insert(configs.begin(), configs.end());
    ConfigPtr newConfig =
        std::make_shared<const config::ConfigBase>(std::move(rawConfigs));
    return std::make_shared<T>(newConfig);
  }

 protected:
  ConfigPtr config_;

  template <typename T, bool throwException>
  T checkAndGetConfigValue(
      const std::string& configKey,
      const T& defaultValue) const;
};

class ConnectionConfig : public PulsarConfig {
 public:
  explicit ConnectionConfig(const ConfigPtr& config) : PulsarConfig(config) {}

  static constexpr const char* kServiceUrl = "service.url";
  static constexpr const char* kTopic = "topic";
  static constexpr const char* kSubscriptionName = "subscription.name";
  static constexpr const char* kConsumerName = "consumer.name";
  static constexpr const char* kFormat = "format";
  static constexpr const char* kSubscriptionType = "subscription.type";
  static constexpr const char* kInitialPosition = "initial.position";
  static constexpr const char* kReceiverQueueSize = "receiver.queue.size";
  static constexpr const char* kDataBatchSize = "data.batch.size";
  static constexpr const char* kReceiveTimeoutMills = "receive.timeout.mills";
  static constexpr const char* kAcknowledgeMessages = "acknowledge.messages";

  static constexpr const char* defaultSubscriptionType = "exclusive";
  static constexpr const char* defaultInitialPosition = "latest";
  static constexpr const uint32_t defaultReceiverQueueSize = 1000;
  static constexpr const uint32_t defaultDataBatchSize = 500;
  static constexpr const uint32_t defaultReceiveTimeoutMills = 100;

  std::string getServiceUrl() const;
  std::string getTopic() const;
  std::string getSubscriptionName() const;
  std::string getConsumerName() const;
  std::string getFormat() const;
  std::string getSubscriptionType() const;
  std::string getInitialPosition() const;
  uint32_t getReceiverQueueSize() const;
  uint32_t getDataBatchSize() const;
  uint32_t getReceiveTimeoutMills() const;
  bool getAcknowledgeMessages() const;

  ::pulsar::ClientConfiguration getPulsarClientConfiguration() const;
  ::pulsar::ConsumerConfiguration getPulsarConsumerConfiguration() const;
};

using PulsarConfigPtr = std::shared_ptr<PulsarConfig>;
using ConnectionConfigPtr = std::shared_ptr<ConnectionConfig>;

} // namespace facebook::velox::connector::pulsar
