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

#include "velox/connectors/Connector.h"
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>
#include <folly/dynamic.h>

namespace facebook::velox::connector::kafka {

/// The split information for kafka connector.
struct KafkaConnectorSplit : public ConnectorSplit {
  std::string bootstrapServers_;
  std::string groupId_;
  std::string clientId_;
  std::string format_;
  bool enableAutoCommit_;
  std::string autoResetOffset_;
  std::unordered_map<std::string, std::vector<std::pair<uint32_t, int64_t>>>
      topicPartitions_;

  explicit KafkaConnectorSplit(
      const std::string& connectorId,
      const std::string& bootstrapServers,
      const std::string& groupId,
      const std::string& format,
      const bool enableAutoCommit,
      const std::string& autoResetOffset,
      std::unordered_map<std::string, std::vector<std::pair<uint32_t, int64_t>>>&
          tps)
      : ConnectorSplit(connectorId),
        bootstrapServers_(bootstrapServers),
        groupId_(groupId),
        clientId_(connectorId),
        format_(format),
        enableAutoCommit_(enableAutoCommit),
        autoResetOffset_(autoResetOffset),
        topicPartitions_(tps) {}

  cppkafka::TopicPartitionList getCppKafkaTopicPartitions() const {
    cppkafka::TopicPartitionList topicPartitions;
    for (const auto& p : topicPartitions_) {
      std::string topic = p.first;
      for (const auto& partition : p.second) {
        cppkafka::TopicPartition topicPartition(
            topic, static_cast<int>(partition.first));
        if (partition.second >= 0) {
          topicPartition.set_offset(partition.second);
        }
        topicPartitions.emplace_back(topicPartition);
      }
    }
    return topicPartitions;
  }

  static std::string topicPartitonsToString(const cppkafka::TopicPartitionList& tps);

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static std::shared_ptr<KafkaConnectorSplit> create(const folly::dynamic& obj);

  static void registerSerDe();
};
} // namespace facebook::velox::connector::kafka