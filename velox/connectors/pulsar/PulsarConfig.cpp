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

#include "velox/connectors/pulsar/PulsarConfig.h"
#include "velox/common/base/Exceptions.h"
#include <algorithm>
#include <cctype>

namespace facebook::velox::connector::pulsar {

template <typename T, bool throwException>
T PulsarConfig::checkAndGetConfigValue(
    const std::string& configKey,
    const T& defaultValue) const {
  std::optional<T> configValue;
  if (config_) {
    configValue = static_cast<std::optional<T>>(config_->get<T>(configKey));
  }
  if constexpr (throwException) {
    VELOX_CHECK(
        configValue.has_value(),
        "Pulsar config {} has no specified value.",
        configKey);
  }
  return configValue.value_or(defaultValue);
}

std::string ConnectionConfig::getServiceUrl() const {
  return checkAndGetConfigValue<std::string, true>(kServiceUrl, "");
}

std::string ConnectionConfig::getTopic() const {
  return checkAndGetConfigValue<std::string, true>(kTopic, "");
}

std::string ConnectionConfig::getSubscriptionName() const {
  return checkAndGetConfigValue<std::string, true>(kSubscriptionName, "");
}

std::string ConnectionConfig::getConsumerName() const {
  return checkAndGetConfigValue<std::string, false>(kConsumerName, "");
}

std::string ConnectionConfig::getFormat() const {
  return checkAndGetConfigValue<std::string, true>(kFormat, "");
}

std::string ConnectionConfig::getSubscriptionType() const {
  return checkAndGetConfigValue<std::string, false>(
      kSubscriptionType, defaultSubscriptionType);
}

std::string ConnectionConfig::getInitialPosition() const {
  return checkAndGetConfigValue<std::string, false>(
      kInitialPosition, defaultInitialPosition);
}

uint32_t ConnectionConfig::getReceiverQueueSize() const {
  return checkAndGetConfigValue<uint32_t, false>(
      kReceiverQueueSize, defaultReceiverQueueSize);
}

uint32_t ConnectionConfig::getDataBatchSize() const {
  return checkAndGetConfigValue<uint32_t, false>(
      kDataBatchSize, defaultDataBatchSize);
}

uint32_t ConnectionConfig::getReceiveTimeoutMills() const {
  return checkAndGetConfigValue<uint32_t, false>(
      kReceiveTimeoutMills, defaultReceiveTimeoutMills);
}

bool ConnectionConfig::getAcknowledgeMessages() const {
  const auto value =
      checkAndGetConfigValue<std::string, false>(kAcknowledgeMessages, "true");
  std::string lowerValue;
  lowerValue.resize(value.size());
  std::transform(value.begin(), value.end(), lowerValue.begin(), ::tolower);
  if (lowerValue == "true" || lowerValue == "1") {
    return true;
  }
  if (lowerValue == "false" || lowerValue == "0") {
    return false;
  }
  VELOX_FAIL("Invalid Pulsar acknowledge messages config: {}", value);
}

} // namespace facebook::velox::connector::pulsar
