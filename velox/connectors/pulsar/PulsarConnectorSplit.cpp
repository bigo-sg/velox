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

namespace facebook::velox::connector::pulsar {

std::string PulsarConnectorSplit::toString() const {
  return fmt::format(
      "Pulsar connector split, connectorId: {}, service url: {}, topic: {}, subscription: {}, partition: {}, start message id: {}, end message id: {}",
      connectorId,
      serviceUrl_,
      topic_,
      subscriptionName_,
      partitionIndex_,
      startMessageId_,
      endMessageId_);
}

folly::dynamic PulsarConnectorSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "PulsarConnectorSplit";
  obj["connectorId"] = connectorId;
  obj["serviceUrl"] = serviceUrl_;
  obj["topic"] = topic_;
  obj["subscriptionName"] = subscriptionName_;
  obj["format"] = format_;
  obj["partitionIndex"] = partitionIndex_;
  obj["startMessageId"] = startMessageId_;
  obj["endMessageId"] = endMessageId_;
  obj["startMessageIdInclusive"] = startMessageIdInclusive_;
  return obj;
}

std::shared_ptr<PulsarConnectorSplit> PulsarConnectorSplit::create(
    const folly::dynamic& obj) {
  return std::make_shared<PulsarConnectorSplit>(
      obj["connectorId"].asString(),
      obj["serviceUrl"].asString(),
      obj["topic"].asString(),
      obj["subscriptionName"].asString(),
      obj["format"].asString(),
      obj.getDefault("partitionIndex", -1).asInt(),
      obj.getDefault("startMessageId", "").asString(),
      obj.getDefault("endMessageId", "").asString(),
      obj.getDefault("startMessageIdInclusive", true).asBool());
}

void PulsarConnectorSplit::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("PulsarConnectorSplit", PulsarConnectorSplit::create);
}

} // namespace facebook::velox::connector::pulsar
