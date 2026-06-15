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
#include <folly/dynamic.h>

namespace facebook::velox::connector::pulsar {

struct PulsarConnectorSplit : public ConnectorSplit {
  std::string serviceUrl_;
  std::string topic_;
  std::string subscriptionName_;
  std::string format_;

  explicit PulsarConnectorSplit(
      const std::string& connectorId,
      std::string serviceUrl,
      std::string topic,
      std::string subscriptionName,
      std::string format)
      : ConnectorSplit(connectorId),
        serviceUrl_(std::move(serviceUrl)),
        topic_(std::move(topic)),
        subscriptionName_(std::move(subscriptionName)),
        format_(std::move(format)) {}

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static std::shared_ptr<PulsarConnectorSplit> create(
      const folly::dynamic& obj);

  static void registerSerDe();
};

} // namespace facebook::velox::connector::pulsar
