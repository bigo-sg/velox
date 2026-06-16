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
#include "velox/connectors/pulsar/PulsarPartitionUtils.h"
#include <folly/String.h>
#include <pulsar/Message.h>
#include <pulsar/MessageIdBuilder.h>
#include <pulsar/Result.h>

namespace facebook::velox::connector::pulsar {

std::optional<::pulsar::MessageId> PulsarConsumer::parseMessageId(
    const std::string& value,
    int32_t partitionIndex) {
  if (value.empty()) {
    return std::nullopt;
  }
  if (value == "earliest") {
    return ::pulsar::MessageId::earliest();
  }
  if (value == "latest") {
    return ::pulsar::MessageId::latest();
  }

  std::vector<std::string> parts;
  folly::split(':', value, parts);
  VELOX_CHECK(
      parts.size() >= 2 && parts.size() <= 4,
      "Invalid Pulsar message id '{}'. Expected earliest, latest, or ledgerId:entryId[:batchIndex[:partition]].",
      value);

  auto builder = ::pulsar::MessageIdBuilder()
                     .ledgerId(folly::to<int64_t>(parts[0]))
                     .entryId(folly::to<int64_t>(parts[1]));
  if (parts.size() >= 3 && !parts[2].empty()) {
    builder.batchIndex(folly::to<int32_t>(parts[2]));
  }
  if (parts.size() >= 4 && !parts[3].empty()) {
    builder.partition(folly::to<int32_t>(parts[3]));
  } else if (partitionIndex >= 0) {
    builder.partition(partitionIndex);
  }
  return builder.build();
}

PulsarConsumer::PulsarConsumer(
    const ConnectionConfigPtr& config,
    uint32_t receiveTimeoutMillis,
    uint32_t batchSize)
    : client_(
          config->getServiceUrl(),
          config->getPulsarClientConfiguration()),
      receiveTimeoutMillis_(receiveTimeoutMillis),
      batchSize_(batchSize),
      topic_(
          partitionedTopicName(config->getTopic(), config->getPartitionIndex())),
      subscriptionName_(config->getSubscriptionName()),
      endMessageId_(
          parseMessageId(config->getEndMessageId(), config->getPartitionIndex())) {
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

  const auto startMessageId =
      parseMessageId(config->getStartMessageId(), config->getPartitionIndex());
  if (startMessageId.has_value()) {
    auto seekResult = consumer_.seek(startMessageId.value());
    VELOX_CHECK(
        seekResult == ::pulsar::ResultOk,
        "Failed to seek Pulsar topic {} to start message id {}: {}",
        topic_,
        config->getStartMessageId(),
        ::pulsar::strResult(seekResult));
  }
}

PulsarConsumer::~PulsarConsumer() {
  close();
}

void PulsarConsumer::close() {
  if (closed_.exchange(true)) {
    return;
  }
  consumer_.close();
  client_.close();
}

void PulsarConsumer::consumeBatch(
    std::vector<PulsarMessage>& messages,
    size_t& messageBytes) {
  for (uint32_t i = 0; i < batchSize_; ++i) {
    ::pulsar::Message message;
    auto result = consumer_.receive(message, receiveTimeoutMillis_.count());
    if (result == ::pulsar::ResultTimeout) {
      ++stats_.receiveTimeouts;
      break;
    }
    if (closed_.load() && (result == ::pulsar::ResultAlreadyClosed ||
                           result == ::pulsar::ResultInterrupted)) {
      reachedEnd_ = true;
      break;
    }
    VELOX_CHECK(
        result == ::pulsar::ResultOk,
        "Failed to receive Pulsar message from topic {}: {}",
        topic_,
        ::pulsar::strResult(result));

    if (endMessageId_.has_value() &&
        message.getMessageId() > endMessageId_.value()) {
      // This message belongs to a later split. Nack it so Pulsar can redeliver
      // it to that split's consumer; bounded split readers should expect the
      // first message after endMessageId to be negatively acknowledged.
      consumer_.negativeAcknowledge(message.getMessageId());
      reachedEnd_ = true;
      ++stats_.negativelyAcknowledgedMessages;
      ++stats_.skippedMessagesAfterEnd;
      break;
    }

    std::string payload = message.getDataAsString();
    messageBytes += payload.size();
    stats_.receivedBytes += payload.size();
    ++stats_.receivedMessages;
    messages.push_back({std::move(payload), std::move(message)});
  }
}

void PulsarConsumer::acknowledge(
    const ::pulsar::Message& message,
    bool cumulative) {
  if (closed_.load()) {
    return;
  }
  const auto result = cumulative
      ? consumer_.acknowledgeCumulative(message.getMessageId())
      : consumer_.acknowledge(message);
  if (closed_.load() && (result == ::pulsar::ResultAlreadyClosed ||
                         result == ::pulsar::ResultInterrupted)) {
    return;
  }
  VELOX_CHECK(
      result == ::pulsar::ResultOk,
      "Failed to acknowledge Pulsar message from topic {}: {}",
      topic_,
      ::pulsar::strResult(result));
  ++stats_.acknowledgedMessages;
}

void PulsarConsumer::negativeAcknowledge(const ::pulsar::Message& message) {
  consumer_.negativeAcknowledge(message.getMessageId());
  ++stats_.negativelyAcknowledgedMessages;
}

} // namespace facebook::velox::connector::pulsar
