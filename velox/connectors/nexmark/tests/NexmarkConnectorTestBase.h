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

#include <folly/init/Init.h>

#include "velox/connectors/nexmark/NexmarkConnector.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::velox::connector::nexmark::test {

class NexmarkConnectorTestBase : public exec::test::OperatorTestBase {
 public:
  const std::string kNexmarkConnectorId = "test-nexmark";

  void SetUp() override {
    OperatorTestBase::SetUp();
    connector::registerConnectorFactory(
        std::make_shared<connector::nexmark::NexmarkConnectorFactory>());
    std::shared_ptr<const config::ConfigBase> config;
    auto nexmarkConnector =
        connector::getConnectorFactory(
            connector::nexmark::NexmarkConnectorFactory::kNexmarkConnectorName)
            ->newConnector(kNexmarkConnectorId, config);
    connector::registerConnector(nexmarkConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kNexmarkConnectorId);
    connector::unregisterConnectorFactory(
        connector::nexmark::NexmarkConnectorFactory::kNexmarkConnectorName);
    OperatorTestBase::TearDown();
  }

  exec::Split makeNexmarkSplit(size_t numRows) const {
    return exec::Split(
        std::make_shared<NexmarkConnectorSplit>(kNexmarkConnectorId, numRows));
  }

  std::vector<exec::Split> makeNexmarkSplits(
      size_t rowsPerSplit,
      size_t numSplits) const {
    std::vector<exec::Split> splits;
    splits.reserve(numSplits);

    for (size_t i = 0; i < numSplits; ++i) {
      splits.emplace_back(makeNexmarkSplit(rowsPerSplit));
    }
    return splits;
  }

  std::shared_ptr<NexmarkTableHandle> makeNexmarkTableHandle(int64_t maxEvents) const {
    NexmarkConfiguration configuration;
    configuration.bidProportion = 46;
    auto baseTime = std::chrono::system_clock::now().time_since_epoch() /
        std::chrono::milliseconds(1);
    GeneratorConfig config{std::move(configuration), baseTime, 1, maxEvents, 1};
    return std::make_shared<NexmarkTableHandle>(kNexmarkConnectorId, std::move(config));
  }
};

} // namespace facebook::velox::connector::nexmark::test
