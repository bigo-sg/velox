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

#include "velox/connectors/kafka/KafkaConnectorSplit.h"
#include <folly/dynamic.h>
#include <sstream>

namespace facebook::velox::connector::kafka {

std::string KafkaConnectorSplit::topicPartitonsToString(
    const cppkafka::TopicPartitionList& tps) {
  if (tps.empty()) {
    return "";
  }
  std::stringstream s;
  for (size_t i = 0; i < tps.size() - 1; ++i) {
    s << "[" << tps[i].get_topic() << "," << tps[i].get_partition() << "]";
    s << ",";
  }
  s << "[" << tps[tps.size() - 1].get_topic() << ","
    << tps[tps.size() - 1].get_partition() << "]";
  return s.str();
}

std::string KafkaConnectorSplit::toString() const {
  return fmt::format(
      "Kafka connector split, connectorId: {}, bootstrap servers:{}, topic partitions:{}, group id:{}",
      clientId_,
      bootstrapServers_,
      KafkaConnectorSplit::topicPartitonsToString(getCppKafkaTopicPartitions()),
      groupId_);
}

folly::dynamic KafkaConnectorSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["connectorId"] = clientId_;
  obj["bootstrapServers"] = bootstrapServers_;
  obj["groupId"] = groupId_;
  obj["format"] = format_;
  obj["enableAutoCommit"] = enableAutoCommit_;
  obj["autoResetOffset"] = autoResetOffset_;
  folly::dynamic tpObjs = folly::dynamic::array;
  size_t j = 0;
  for (const auto& tp : topicPartitions_) {
    std::string topic = tp.first;
    for (size_t i = 0; i < tp.second.size(); ++i) {
      folly::dynamic d = folly::dynamic::object;
      d["topic"] = topic;
      d["partition"] = tp.second[i].first;
      d["offset"] = tp.second[i].second;
      tpObjs[j] = d;
      j++;
    }
  }
  obj["topicPartitions"] = tpObjs;
  return obj;
}

std::shared_ptr<KafkaConnectorSplit> KafkaConnectorSplit::create(
    const folly::dynamic& obj) {
  std::unordered_map<std::string, std::vector<std::pair<uint32_t, int64_t>>> topics;
  if (obj["topicPartitions"].isArray()) {
    const auto tpObjs = obj["topicPartitions"];
    for (const auto& tp : tpObjs) {
      if (tp.isObject()) {
        const auto topic = tp["topic"];
        const auto partition = tp["partition"];
        const auto offset = tp["offset"];
        const auto it = topics.find(topic.asString());
        std::pair<uint32_t, int64_t> p(
            static_cast<uint32_t>(partition.asInt()),
            static_cast<int64_t>(offset.asInt()));
        if (it != topics.end()) {
          std::vector<std::pair<uint32_t, int64_t>>& partitionOffset =
              it->second;
          partitionOffset.emplace_back(p);
        } else {
          std::vector<std::pair<uint32_t, int64_t>> partitionOffset;
          partitionOffset.emplace_back(p);
          topics[topic.asString()] = partitionOffset;
        }
      }
    }
  }
  return std::make_shared<KafkaConnectorSplit>(
      obj["connectorId"].asString(),
      obj["bootstrapServers"].asString(),
      obj["groupId"].asString(),
      obj["format"].asString(),
      obj["enableAutoCommit"].asBool(),
      obj["autoResetOffset"].asString(),
      topics);
}

void KafkaConnectorSplit::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("KafkaConnectorSplit", KafkaConnectorSplit::create);
}

} // namespace facebook::velox::connector::kafka
