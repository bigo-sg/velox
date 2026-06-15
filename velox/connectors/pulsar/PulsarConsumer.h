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

#include "velox/connectors/pulsar/PulsarConfig.h"
#include <pulsar/Client.h>
#include <pulsar/Consumer.h>

namespace facebook::velox::connector::pulsar {

class PulsarConsumer {
 public:
  PulsarConsumer(
      const ConnectionConfigPtr& config,
      uint32_t receiveTimeoutMillis,
      uint32_t batchSize);

  ~PulsarConsumer();

  void consumeBatch(
      std::vector<std::string>& messages,
      size_t& messageBytes,
      bool acknowledgeMessages);

  const std::string& topic() const {
    return topic_;
  }

  const std::string& subscriptionName() const {
    return subscriptionName_;
  }

 private:
  ::pulsar::Client client_;
  ::pulsar::Consumer consumer_;
  std::chrono::milliseconds receiveTimeoutMillis_;
  uint32_t batchSize_;
  std::string topic_;
  std::string subscriptionName_;
};

using PulsarConsumerPtr = std::shared_ptr<PulsarConsumer>;

} // namespace facebook::velox::connector::pulsar
