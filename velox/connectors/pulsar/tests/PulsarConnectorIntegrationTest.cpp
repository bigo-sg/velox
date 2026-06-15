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

#include "velox/connectors/pulsar/PulsarConnector.h"
#include "velox/connectors/pulsar/PulsarConnectorSplit.h"
#include "velox/connectors/pulsar/PulsarTableHandle.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"
#include <cstdlib>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <pulsar/MessageBuilder.h>
#include <pulsar/Producer.h>
#include <unistd.h>

namespace facebook::velox::connector::pulsar::test {

namespace {

std::string getEnvOrDefault(const char* name, const std::string& defaultValue) {
  const char* value = std::getenv(name);
  return value == nullptr || std::string(value).empty() ? defaultValue : value;
}

class ConnectorCleanup {
 public:
  explicit ConnectorCleanup(std::string connectorId)
      : connectorId_(std::move(connectorId)) {}

  ~ConnectorCleanup() {
    connector::unregisterConnector(connectorId_);
    connector::unregisterConnectorFactory(
        PulsarConnectorFactory::kPulsarConnectorName);
  }

 private:
  std::string connectorId_;
};

} // namespace

TEST(PulsarConnectorIntegrationTest, rawMessagesFromStandalone) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = getEnvOrDefault(
      "PULSAR_TEST_TOPIC",
      fmt::format("persistent://public/default/velox-pulsar-it-{}", getpid()));
  const auto subscription = fmt::format("velox-sub-{}", getpid());
  const auto connectorId = "test-pulsar";
  const auto outputType = ROW({"payload"}, {VARCHAR()});
  auto pool = memory::memoryManager()->addLeafPool();

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  std::unordered_map<std::string, std::string> configMap;
  configMap[ConnectionConfig::kServiceUrl] = serviceUrl;
  configMap[ConnectionConfig::kTopic] = topic;
  configMap[ConnectionConfig::kSubscriptionName] = subscription;
  configMap[ConnectionConfig::kFormat] = "raw";
  configMap[ConnectionConfig::kInitialPosition] = "earliest";
  configMap[ConnectionConfig::kDataBatchSize] = "2";
  configMap[ConnectionConfig::kReceiveTimeoutMills] = "1000";
  configMap[ConnectionConfig::kAcknowledgeMessages] = "true";

  auto connectorConfig =
      std::make_shared<const config::ConfigBase>(std::move(configMap));
  auto connector =
      connector::getConnectorFactory(PulsarConnectorFactory::kPulsarConnectorName)
          ->newConnector(connectorId, connectorConfig);
  connector::registerConnector(connector);

  connector::ConnectorQueryCtx queryCtx(
      pool.get(),
      nullptr,
      connectorConfig.get(),
      nullptr,
      common::PrefixSortConfig(),
      nullptr,
      nullptr,
      "query.Pulsar",
      "task.Pulsar",
      "planNodeId.Pulsar",
      0,
      "");
  auto tableHandle =
      std::make_shared<PulsarTableHandle>(connectorId, topic, outputType);
  std::unordered_map<std::string, std::shared_ptr<ColumnHandle>> columnHandles;
  auto source = connector->createDataSource(
      outputType, tableHandle, columnHandles, &queryCtx);
  source->addSplit(std::make_shared<PulsarConnectorSplit>(
      connectorId, serviceUrl, topic, subscription, "raw"));

  ::pulsar::Client client(serviceUrl);
  ::pulsar::Producer producer;
  auto result = client.createProducer(topic, producer);
  ASSERT_EQ(result, ::pulsar::ResultOk) << ::pulsar::strResult(result);
  for (const auto& payload : {"first", "second"}) {
    ::pulsar::MessageId messageId;
    result = producer.send(
        ::pulsar::MessageBuilder().setContent(payload).build(), messageId);
    ASSERT_EQ(result, ::pulsar::ResultOk) << ::pulsar::strResult(result);
  }
  producer.close();
  client.close();

  ContinueFuture future{folly::Unit{}};
  std::optional<RowVectorPtr> resultVector;
  for (int i = 0; i < 10 && !resultVector.has_value(); ++i) {
    resultVector = source->next(0, future);
  }
  ASSERT_TRUE(resultVector.has_value());
  ASSERT_EQ(resultVector.value()->size(), 2);
  auto payloads =
      resultVector.value()->childAt(0)->as<FlatVector<StringView>>();
  ASSERT_EQ(payloads->valueAt(0).str(), "first");
  ASSERT_EQ(payloads->valueAt(1).str(), "second");

  const auto stats = source->runtimeStats();
  ASSERT_EQ(stats.at("pulsarReceivedMessages").value, 2);
  ASSERT_EQ(stats.at("pulsarAcknowledgedMessages").value, 2);
}

} // namespace facebook::velox::connector::pulsar::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  facebook::velox::memory::MemoryManager::testingSetInstance({});
  return RUN_ALL_TESTS();
}
