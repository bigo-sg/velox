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

#include "cppkafka/cppkafka.h"
#include "cppkafka/topic_partition_list.h"
#include "velox/connectors/kafka/KafkaConnectorSplit.h"
#include "velox/connectors/kafka/KafkaRecordDeserializer.h"

namespace facebook::velox::connector::kafka {

using CppKafkaConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

/// Class for consume records from kafka topic
class KafkaConsumer {
 public:
  KafkaConsumer(
      const CppKafkaConsumerPtr& consumer,
      const uint32_t pollTimeOut,
      const uint32_t pollBatchSize)
      : consumer_(consumer),
        pollTimeOutMillis_(pollTimeOut),
        pollBatchSize_(pollBatchSize) {}

  ~KafkaConsumer() {}

  /// Get the parititions of the given `topic`, and get the offset accroding to
  /// the given `startupMode`.
  const cppkafka::TopicPartitionList getTopicPartitions(
      const std::string& topic,
      const std::string& startupMode);
  /// Set topic partitions offset according to the given `startupMode`.
  const void setTopicPartitionsOffset(
      cppkafka::TopicPartitionList& tps,
      const std::string& startupMode);
  /// Subscribe to the kafka topics.
  void subscribe(const std::vector<std::string>& topics);
  /// Assign the consumer to the given topic partitions.
  void assign(const cppkafka::TopicPartitionList& tps);
  /// Consume a batch of messages.
  const void consumeBatch(std::vector<std::string>& msgs, size_t& msgBytes);
  /// For test, Get the kafka topics that already subscribed.
  const std::vector<std::string> getSubscribedTopics();
  /// For test, Get the kafka assigned topic and partitions.
  const cppkafka::TopicPartitionList getAssignedTopicPartitions();

 private:
  /// The kafka consume handle by using `cppkafka`.
  CppKafkaConsumerPtr consumer_;
  /// The timeout milliseconds for consuming kafka.
  std::chrono::milliseconds pollTimeOutMillis_;
  /// The batch size for consuming kafka.
  uint32_t pollBatchSize_;
};

using KafkaConsumerPtr = std::shared_ptr<KafkaConsumer>;
} // namespace facebook::velox::connector::kafka