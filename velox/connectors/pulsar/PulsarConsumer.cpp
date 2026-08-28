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
#include <fmt/format.h>
#include <folly/String.h>
#include <pulsar/Message.h>
#include <pulsar/MessageIdBuilder.h>
#include <pulsar/Result.h>
#include "velox/common/base/Exceptions.h"
#include "velox/connectors/pulsar/PulsarPartitionUtils.h"

namespace facebook::velox::connector::pulsar {

namespace {

std::string messageIdString(const ::pulsar::MessageId& messageId) {
  return fmt::format(
      "{}:{}:{}:{}",
      messageId.ledgerId(),
      messageId.entryId(),
      messageId.batchIndex(),
      messageId.partition());
}

} // namespace

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
    std::vector<TopicPartitionOffset> topicPartitionOffsets)
    : client_(config->getServiceUrl(), config->getPulsarClientConfiguration()),
      receiveTimeoutMillis_(config->getReceiveTimeoutMills()),
      batchSize_(config->getDataBatchSize()),
      subscriptionName_(config->getSubscriptionName()) {
  VELOX_CHECK_GT(
      topicPartitionOffsets.size(),
      0,
      "Pulsar consumer requires at least one topic partition offset.");
  consumers_.reserve(topicPartitionOffsets.size());
  partitionedTopics_.reserve(topicPartitionOffsets.size());
  for (auto& topicPartitionOffset : topicPartitionOffsets) {
    auto consumerConfig = config->getPulsarConsumerConfiguration();
    consumerConfig.setStartMessageIdInclusive(
        topicPartitionOffset.startMessageIdInclusive);

    TopicPartitionConsumer topicPartitionConsumer{
        std::move(topicPartitionOffset), "", ::pulsar::Consumer()};
    topicPartitionConsumer.topic =
        topicPartitionConsumer.topicPartitionOffset.partitionedTopic;
    auto result = client_.subscribe(
        topicPartitionConsumer.topic,
        subscriptionName_,
        consumerConfig,
        topicPartitionConsumer.consumer);
    VELOX_CHECK(
        result == ::pulsar::ResultOk,
        "Failed to subscribe Pulsar topic {} with subscription {}: {}",
        topicPartitionConsumer.topic,
        subscriptionName_,
        ::pulsar::strResult(result));

    const auto parsedMessageId = parseMessageId(
        topicPartitionConsumer.topicPartitionOffset.messageId, -1);
    if (parsedMessageId.has_value()) {
      // startMessageIdInclusive is applied via ConsumerConfiguration before
      // subscribe because Pulsar C++ client 3.3 has no seek(id, inclusive) API.
      auto seekResult =
          topicPartitionConsumer.consumer.seek(parsedMessageId.value());
      VELOX_CHECK(
          seekResult == ::pulsar::ResultOk,
          "Failed to seek Pulsar topic {} to start message id {}: {}",
          topicPartitionConsumer.topic,
          topicPartitionConsumer.topicPartitionOffset.messageId,
          ::pulsar::strResult(seekResult));
    }
    const auto partitionedTopic = topicPartitionConsumer.topic;
    partitionedTopics_.push_back(partitionedTopic);
    auto [_, inserted] =
        consumers_.emplace(partitionedTopic, std::move(topicPartitionConsumer));
    VELOX_CHECK(
        inserted, "Duplicate Pulsar partitioned topic {}.", partitionedTopic);
  }
  topic_ = currentConsumer().topic;
}

PulsarConsumer::~PulsarConsumer() {
  try {
    close();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to close Pulsar consumer in destructor: " << e.what();
  } catch (...) {
    LOG(ERROR)
        << "Failed to close Pulsar consumer in destructor: unknown error";
  }
}

void PulsarConsumer::close() {
  if (closed_.exchange(true)) {
    return;
  }
  for (auto& [_, consumer] : consumers_) {
    consumer.consumer.close();
  }
  client_.close();
}

void PulsarConsumer::consumeBatch(
    std::vector<PulsarMessage>& messages,
    size_t& messageBytes) {
  size_t consecutiveTimeouts = 0;
  while (messages.size() < batchSize_ && !closed_.load()) {
    auto& topicPartitionConsumer = currentConsumer();
    ::pulsar::Messages batch;
    auto result = topicPartitionConsumer.consumer.batchReceive(batch);
    if (closed_.load() &&
        (result == ::pulsar::ResultAlreadyClosed ||
         result == ::pulsar::ResultInterrupted)) {
      break;
    }
    if (result == ::pulsar::ResultTimeout || batch.empty()) {
      ++stats_.receiveTimeouts;
      if (!messages.empty()) {
        advanceConsumer();
        break;
      }
      if (!advanceConsumer() ||
          ++consecutiveTimeouts >= partitionedTopics_.size()) {
        break;
      }
      continue;
    }
    consecutiveTimeouts = 0;
    VELOX_CHECK(
        result == ::pulsar::ResultOk,
        "Failed to batch receive Pulsar messages from topic {}: {}",
        topicPartitionConsumer.topic,
        ::pulsar::strResult(result));

    bool receivedMessageInBatch = false;
    for (auto& message : batch) {
      std::string payload = message.getDataAsString();
      messageBytes += payload.size();
      stats_.receivedBytes += payload.size();
      ++stats_.receivedMessages;
      messages.push_back({std::move(payload), std::move(message)});
      receivedMessageInBatch = true;
    }

    if (receivedMessageInBatch) {
      break;
    }
  }
}

bool PulsarConsumer::advanceConsumer() {
  if (partitionedTopics_.empty()) {
    return false;
  }
  currentConsumerIndex_ =
      (currentConsumerIndex_ + 1) % partitionedTopics_.size();
  topic_ = currentConsumer().topic;
  return true;
}

PulsarConsumer::TopicPartitionConsumer& PulsarConsumer::currentConsumer() {
  VELOX_CHECK_LT(
      currentConsumerIndex_,
      partitionedTopics_.size(),
      "Pulsar consumer has no active topic partition consumer.");
  return consumerForPartitionedTopic(partitionedTopics_[currentConsumerIndex_]);
}

PulsarConsumer::TopicPartitionConsumer& PulsarConsumer::consumerForMessage(
    const ::pulsar::Message& message) {
  return consumerForPartitionedTopic(message.getTopicName());
}

PulsarConsumer::TopicPartitionConsumer&
PulsarConsumer::consumerForPartitionedTopic(
    const std::string& partitionedTopic) {
  auto it = consumers_.find(partitionedTopic);
  VELOX_CHECK(
      it != consumers_.end(),
      "Failed to find Pulsar consumer for partitioned topic {}",
      partitionedTopic);
  return it->second;
}

TopicPartitionOffset PulsarConsumer::topicPartitionOffset(
    const ::pulsar::Message& message) const {
  return {
      message.getTopicName(), messageIdString(message.getMessageId()), false};
}

void PulsarConsumer::acknowledge(
    const ::pulsar::Message& message,
    bool cumulative) {
  if (closed_.load()) {
    return;
  }
  auto& consumer = consumerForMessage(message);
  const auto result = cumulative
      ? consumer.consumer.acknowledgeCumulative(message.getMessageId())
      : consumer.consumer.acknowledge(message);
  if (closed_.load() &&
      (result == ::pulsar::ResultAlreadyClosed ||
       result == ::pulsar::ResultInterrupted)) {
    return;
  }
  VELOX_CHECK(
      result == ::pulsar::ResultOk,
      "Failed to acknowledge Pulsar message from topic {}: {}",
      consumer.topic,
      ::pulsar::strResult(result));
  ++stats_.acknowledgedMessages;
}

void PulsarConsumer::acknowledge(
    const TopicPartitionOffset& topicPartitionOffset,
    bool cumulative) {
  if (closed_.load()) {
    return;
  }
  const auto messageId = parseMessageId(topicPartitionOffset.messageId, -1);
  VELOX_CHECK(
      messageId.has_value(),
      "Cannot acknowledge Pulsar message for partitioned topic {} without message id.",
      topicPartitionOffset.partitionedTopic);
  auto& consumer =
      consumerForPartitionedTopic(topicPartitionOffset.partitionedTopic);
  const auto result = cumulative
      ? consumer.consumer.acknowledgeCumulative(messageId.value())
      : consumer.consumer.acknowledge(messageId.value());
  if (closed_.load() &&
      (result == ::pulsar::ResultAlreadyClosed ||
       result == ::pulsar::ResultInterrupted)) {
    return;
  }
  VELOX_CHECK(
      result == ::pulsar::ResultOk,
      "Failed to acknowledge Pulsar message from topic {}: {}",
      consumer.topic,
      ::pulsar::strResult(result));
  ++stats_.acknowledgedMessages;
}

} // namespace facebook::velox::connector::pulsar
