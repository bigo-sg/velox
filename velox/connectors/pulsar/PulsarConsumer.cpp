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

#include "velox/connectors/pulsar/PulsarConsumer.h"
#include "velox/common/base/Exceptions.h"
#include <pulsar/Message.h>
#include <pulsar/Result.h>

namespace facebook::velox::connector::pulsar {

PulsarConsumer::PulsarConsumer(
    const ConnectionConfigPtr& config,
    uint32_t receiveTimeoutMillis,
    uint32_t batchSize)
    : client_(
          config->getServiceUrl(),
          config->getPulsarClientConfiguration()),
      receiveTimeoutMillis_(receiveTimeoutMillis),
      batchSize_(batchSize),
      topic_(config->getTopic()),
      subscriptionName_(config->getSubscriptionName()) {
  auto result = client_.subscribe(
      topic_,
      subscriptionName_,
      config->getPulsarConsumerConfiguration(),
      consumer_);
  VELOX_CHECK(
      result == ::pulsar::ResultOk,
      "Failed to subscribe Pulsar topic {} with subscription {}: {}",
      topic_,
      subscriptionName_,
      ::pulsar::strResult(result));
}

PulsarConsumer::~PulsarConsumer() {
  consumer_.close();
  client_.close();
}

void PulsarConsumer::consumeBatch(
    std::vector<std::string>& messages,
    size_t& messageBytes,
    bool acknowledgeMessages) {
  for (uint32_t i = 0; i < batchSize_; ++i) {
    ::pulsar::Message message;
    auto result = consumer_.receive(message, receiveTimeoutMillis_.count());
    if (result == ::pulsar::ResultTimeout) {
      break;
    }
    VELOX_CHECK(
        result == ::pulsar::ResultOk,
        "Failed to receive Pulsar message from topic {}: {}",
        topic_,
        ::pulsar::strResult(result));
    std::string payload = message.getDataAsString();
    messageBytes += payload.size();
    messages.emplace_back(std::move(payload));
    if (acknowledgeMessages) {
      auto ackResult = consumer_.acknowledge(message);
      VELOX_CHECK(
          ackResult == ::pulsar::ResultOk,
          "Failed to acknowledge Pulsar message from topic {}: {}",
          topic_,
          ::pulsar::strResult(ackResult));
    }
  }
}

} // namespace facebook::velox::connector::pulsar
