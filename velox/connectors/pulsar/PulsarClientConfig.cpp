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
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <pulsar/Authentication.h>
#include <pulsar/ConsumerType.h>
#include <pulsar/InitialPosition.h>

namespace facebook::velox::connector::pulsar {

::pulsar::ClientConfiguration
ConnectionConfig::getPulsarClientConfiguration() const {
  ::pulsar::ClientConfiguration conf;
  auto token = getAuthToken();
  if (token.empty() && !getAuthTokenFile().empty()) {
    VELOX_CHECK(
        folly::readFile(getAuthTokenFile().c_str(), token),
        "Failed to read Pulsar token file: {}",
        getAuthTokenFile());
    token = folly::trimWhitespace(token).str();
  }
  if (!token.empty()) {
    conf.setAuth(::pulsar::AuthToken::createWithToken(token));
  }
  return conf;
}

::pulsar::ConsumerConfiguration
ConnectionConfig::getPulsarConsumerConfiguration() const {
  ::pulsar::ConsumerConfiguration conf;
  const auto subscriptionType = getSubscriptionType();
  if (subscriptionType == "exclusive") {
    conf.setConsumerType(::pulsar::ConsumerExclusive);
  } else if (subscriptionType == "shared") {
    conf.setConsumerType(::pulsar::ConsumerShared);
  } else if (subscriptionType == "failover") {
    conf.setConsumerType(::pulsar::ConsumerFailover);
  } else if (subscriptionType == "key_shared" ||
             subscriptionType == "key-shared") {
    conf.setConsumerType(::pulsar::ConsumerKeyShared);
  } else {
    VELOX_FAIL("Unsupported Pulsar subscription type: {}", subscriptionType);
  }

  const auto initialPosition = getInitialPosition();
  if (initialPosition == "earliest") {
    conf.setSubscriptionInitialPosition(::pulsar::InitialPositionEarliest);
  } else if (initialPosition == "latest") {
    conf.setSubscriptionInitialPosition(::pulsar::InitialPositionLatest);
  } else {
    VELOX_FAIL("Unsupported Pulsar initial position: {}", initialPosition);
  }

  conf.setReceiverQueueSize(getReceiverQueueSize());
  conf.setStartMessageIdInclusive(getStartMessageIdInclusive());
  if (!getConsumerName().empty()) {
    conf.setConsumerName(getConsumerName());
  }
  return conf;
}

} // namespace facebook::velox::connector::pulsar
