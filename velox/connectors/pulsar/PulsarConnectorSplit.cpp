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

#include "velox/connectors/pulsar/PulsarConnectorSplit.h"
#include <folly/dynamic.h>
#include <sstream>

namespace facebook::velox::connector::pulsar {

namespace {

std::string topicPartitionsToString(
    const std::vector<TopicPartitionOffset>& topicPartitions) {
  if (topicPartitions.empty()) {
    return "";
  }
  std::stringstream out;
  bool first = true;
  for (const auto& topicPartition : topicPartitions) {
    if (!first) {
      out << ",";
    }
    first = false;
    out << "[" << topicPartition.partitionedTopic << ","
        << topicPartition.messageId << "]";
  }
  return out.str();
}

} // namespace

std::string PulsarConnectorSplit::toString() const {
  return fmt::format(
      "Pulsar connector split, connectorId: {}, service url: {}, topic: {}, subscription: {}, topic partitions: {}",
      connectorId,
      serviceUrl_,
      topic_,
      subscriptionName_,
      topicPartitionsToString(topicPartitions_));
}

folly::dynamic PulsarConnectorSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "PulsarConnectorSplit";
  obj["connectorId"] = connectorId;
  obj["serviceUrl"] = serviceUrl_;
  obj["topic"] = topic_;
  obj["subscriptionName"] = subscriptionName_;
  obj["format"] = format_;
  folly::dynamic topicPartitions = folly::dynamic::array;
  for (const auto& topicPartitionOffset : topicPartitions_) {
    folly::dynamic topicPartition = folly::dynamic::object;
    topicPartition["partitionedTopic"] = topicPartitionOffset.partitionedTopic;
    topicPartition["messageId"] = topicPartitionOffset.messageId;
    topicPartition["startMessageIdInclusive"] =
        topicPartitionOffset.startMessageIdInclusive;
    topicPartitions.push_back(topicPartition);
  }
  obj["topicPartitions"] = topicPartitions;
  return obj;
}

std::shared_ptr<PulsarConnectorSplit> PulsarConnectorSplit::create(
    const folly::dynamic& obj) {
  std::vector<TopicPartitionOffset> topicPartitions;
  if (obj.count("topicPartitions") && obj["topicPartitions"].isArray()) {
    for (const auto& topicPartition : obj["topicPartitions"]) {
      if (topicPartition.isObject()) {
        topicPartitions.emplace_back(
            topicPartition["partitionedTopic"].asString(),
            topicPartition.getDefault("messageId", "").asString(),
            topicPartition.getDefault("startMessageIdInclusive", true)
                .asBool());
      }
    }
  }
  return std::make_shared<PulsarConnectorSplit>(
      obj["connectorId"].asString(),
      obj["serviceUrl"].asString(),
      obj["topic"].asString(),
      obj["subscriptionName"].asString(),
      obj["format"].asString(),
      std::move(topicPartitions));
}

void PulsarConnectorSplit::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("PulsarConnectorSplit", PulsarConnectorSplit::create);
}

} // namespace facebook::velox::connector::pulsar
