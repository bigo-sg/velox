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

#include "velox/connectors/from_elements/FromElementsConnectorSplit.h"
#include <sstream>

namespace facebook::velox::connector::from_elements {

std::string FromElementsConnectorSplit::toString() const {
  std::stringstream ss;
  ss << "ConnectorId: " << connectorId;
  return ss.str();
}

folly::dynamic FromElementsConnectorSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["connectorId"] = connectorId;
  return obj;
}

std::shared_ptr<FromElementsConnectorSplit> FromElementsConnectorSplit::create(
    const folly::dynamic& obj) {
  const std::string connectorId = obj["connectorId"].asString();
  return std::make_shared<FromElementsConnectorSplit>(connectorId);
}

void FromElementsConnectorSplit::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("FromElementsConnectorSplit", FromElementsConnectorSplit::create);
}
} // namespace facebook::velox::connector::from_elements