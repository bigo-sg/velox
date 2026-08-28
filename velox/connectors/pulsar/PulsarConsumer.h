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

#include <pulsar/Client.h>
#include <pulsar/Consumer.h>
#include <pulsar/Message.h>
#include <pulsar/MessageId.h>
#include <atomic>
#include <optional>
#include <unordered_map>
#include <vector>
#include "velox/connectors/pulsar/PulsarConfig.h"
#include "velox/connectors/pulsar/PulsarConnectorSplit.h"

namespace facebook::velox::connector::pulsar {

struct PulsarMessage {
  std::string payload;
  ::pulsar::Message message;
};

struct PulsarConsumerStats {
  uint64_t receivedMessages{0};
  uint64_t receivedBytes{0};
  uint64_t receiveTimeouts{0};
  uint64_t acknowledgedMessages{0};
};

class PulsarConsumer {
 public:
  PulsarConsumer(
      const ConnectionConfigPtr& config,
      std::vector<TopicPartitionOffset> topicPartitionOffsets);

  ~PulsarConsumer();

  void close();

  void consumeBatch(std::vector<PulsarMessage>& messages, size_t& messageBytes);

  void acknowledge(const ::pulsar::Message& message, bool cumulative);

  void acknowledge(
      const TopicPartitionOffset& topicPartitionOffset,
      bool cumulative);

  const std::string& topic() const {
    return topic_;
  }

  TopicPartitionOffset topicPartitionOffset(
      const ::pulsar::Message& message) const;

  const std::string& subscriptionName() const {
    return subscriptionName_;
  }

  const PulsarConsumerStats& stats() const {
    return stats_;
  }

  bool closed() const {
    return closed_.load();
  }

 private:
  static std::optional<::pulsar::MessageId> parseMessageId(
      const std::string& value,
      int32_t partitionIndex);

  struct TopicPartitionConsumer {
    TopicPartitionOffset topicPartitionOffset;
    std::string topic;
    ::pulsar::Consumer consumer;
  };

  bool advanceConsumer();
  TopicPartitionConsumer& currentConsumer();
  TopicPartitionConsumer& consumerForMessage(const ::pulsar::Message& message);
  TopicPartitionConsumer& consumerForPartitionedTopic(
      const std::string& partitionedTopic);

  ::pulsar::Client client_;
  std::unordered_map<std::string, TopicPartitionConsumer> consumers_;
  std::vector<std::string> partitionedTopics_;
  size_t currentConsumerIndex_{0};
  std::chrono::milliseconds receiveTimeoutMillis_;
  uint32_t batchSize_;
  std::string topic_;
  std::string subscriptionName_;
  std::atomic_bool closed_{false};
  PulsarConsumerStats stats_;
};

using PulsarConsumerPtr = std::shared_ptr<PulsarConsumer>;

} // namespace facebook::velox::connector::pulsar
