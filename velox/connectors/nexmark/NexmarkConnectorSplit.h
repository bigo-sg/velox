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
#include "velox/connectors/nexmark/GeneratorConfig.h"

namespace facebook::velox::connector::nexmark {

struct NexmarkConnectorSplit : public connector::ConnectorSplit {
  explicit NexmarkConnectorSplit(
      const std::string& connectorId,
      GeneratorConfig config)
      : ConnectorSplit(connectorId), config(std::move(config)) {}

  GeneratorConfig config;

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "NexmarkConnectorSplit";
    obj["connectorId"] = connectorId;
    obj["config"] = config.serialize();
    return obj;
  }

  static std::shared_ptr<NexmarkConnectorSplit> create(
      const folly::dynamic& obj) {
    const auto connectorId = obj["connectorId"].asString();
    auto config = GeneratorConfig::deserialize(obj["config"]);
    return std::make_shared<NexmarkConnectorSplit>(
        connectorId, std::move(config));
  }

  static void registerSerDe() {
    auto& registry = DeserializationRegistryForSharedPtr();
    registry.Register("NexmarkConnectorSplit", NexmarkConnectorSplit::create);
  }
};

} // namespace facebook::velox::connector::nexmark

template <>
struct fmt::formatter<facebook::velox::connector::nexmark::NexmarkConnectorSplit>
    : formatter<std::string> {
  auto format(
      facebook::velox::connector::nexmark::NexmarkConnectorSplit s,
      format_context& ctx) const {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<
    std::shared_ptr<facebook::velox::connector::nexmark::NexmarkConnectorSplit>>
    : formatter<std::string> {
  auto format(
      std::shared_ptr<facebook::velox::connector::nexmark::NexmarkConnectorSplit>
          s,
      format_context& ctx) const {
    return formatter<std::string>::format(s->toString(), ctx);
  }
};
