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
#include "velox/connectors/pulsar/PulsarDataSource.h"
#include "velox/connectors/pulsar/PulsarTableHandle.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"
#include <chrono>
#include <cstdlib>
#include <future>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <pulsar/MessageBuilder.h>
#include <pulsar/Producer.h>
#include <thread>
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

std::shared_ptr<const config::ConfigBase> makeRawConfig(
    const std::string& serviceUrl,
    const std::string& topic,
    const std::string& subscription,
    const std::string& receiveTimeoutMillis = "1000",
    const std::string& dataBatchSize = "2",
    const std::string& startMessageIdInclusive = "true",
    const std::string& ackMode = "individual",
    const std::string& format = "raw") {
  std::unordered_map<std::string, std::string> configMap;
  configMap[ConnectionConfig::kServiceUrl] = serviceUrl;
  configMap[ConnectionConfig::kTopic] = topic;
  configMap[ConnectionConfig::kSubscriptionName] = subscription;
  configMap[ConnectionConfig::kFormat] = format;
  configMap[ConnectionConfig::kInitialPosition] = "earliest";
  configMap[ConnectionConfig::kDataBatchSize] = dataBatchSize;
  configMap[ConnectionConfig::kReceiveTimeoutMills] = receiveTimeoutMillis;
  configMap[ConnectionConfig::kAcknowledgeMessages] = "true";
  configMap[ConnectionConfig::kAckMode] = ackMode;
  configMap[ConnectionConfig::kStartMessageIdInclusive] =
      startMessageIdInclusive;
  return std::make_shared<const config::ConfigBase>(std::move(configMap));
}

std::unique_ptr<DataSource> createRawDataSource(
    const std::shared_ptr<memory::MemoryPool>& pool,
    const std::shared_ptr<const config::ConfigBase>& connectorConfig,
    const std::string& connectorId,
    const std::string& serviceUrl,
    const std::string& topic,
    const std::string& subscription,
    const std::string& startMessageId = "",
    const std::string& endMessageId = "",
    int32_t partitionIndex = -1,
    const std::string& format = "raw",
    RowTypePtr outputType = nullptr) {
  if (outputType == nullptr) {
    outputType = ROW({"payload"}, {VARCHAR()});
  }
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
      connectorId,
      serviceUrl,
      topic,
      subscription,
      format,
      partitionIndex,
      startMessageId,
      endMessageId));
  return source;
}

std::string messageIdString(const ::pulsar::MessageId& messageId) {
  return fmt::format(
      "{}:{}:{}",
      messageId.ledgerId(),
      messageId.entryId(),
      messageId.batchIndex());
}

void produceRawMessages(
    const std::string& serviceUrl,
    const std::string& topic,
    std::vector<::pulsar::MessageId>& messageIds,
    const std::vector<std::string>& payloads = {"first", "second"}) {
  ::pulsar::Client client(serviceUrl);
  ::pulsar::Producer producer;
  auto result = client.createProducer(topic, producer);
  ASSERT_EQ(result, ::pulsar::ResultOk) << ::pulsar::strResult(result);
  for (const auto& payload : payloads) {
    ::pulsar::MessageId messageId;
    result = producer.send(
        ::pulsar::MessageBuilder().setContent(payload).build(), messageId);
    ASSERT_EQ(result, ::pulsar::ResultOk) << ::pulsar::strResult(result);
    messageIds.push_back(messageId);
  }
  producer.close();
  client.close();
}

void createPartitionedTopic(
    const std::string& topic,
    int32_t partitions) {
  const char* pulsarHome = std::getenv("PULSAR_STANDALONE_HOME");
  ASSERT_NE(pulsarHome, nullptr);
  const auto pulsarAdmin = fmt::format("{}/bin/pulsar-admin", pulsarHome);
  ASSERT_EQ(access(pulsarAdmin.c_str(), X_OK), 0);
  const auto command = fmt::format(
      "{} topics create-partitioned-topic {} -p {}",
      pulsarAdmin,
      topic,
      partitions);
  ASSERT_EQ(std::system(command.c_str()), 0);
}

std::optional<RowVectorPtr> readNextResult(DataSource* source) {
  ContinueFuture future{folly::Unit{}};
  std::optional<RowVectorPtr> resultVector;
  for (int i = 0; i < 10 && !resultVector.has_value(); ++i) {
    resultVector = source->next(0, future);
    if (!resultVector.has_value()) {
      EXPECT_TRUE(future.valid());
      EXPECT_TRUE(std::move(future).wait(std::chrono::seconds{5}));
      future = ContinueFuture{folly::Unit{}};
    }
  }
  return resultVector;
}

} // namespace

TEST(PulsarConnectorIntegrationTest, rawMessagesFromStandalone) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = getEnvOrDefault(
      "PULSAR_TEST_TOPIC",
      fmt::format("persistent://public/default/velox-pulsar-it-{}", getpid()));
  const auto subscription = fmt::format("velox-sub-{}", getpid());
  const auto connectorId = "test-pulsar";
  auto pool = memory::memoryManager()->addLeafPool();

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig = makeRawConfig(serviceUrl, topic, subscription);
  auto source = createRawDataSource(
      pool, connectorConfig, connectorId, serviceUrl, topic, subscription);

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(serviceUrl, topic, messageIds);

  auto resultVector = readNextResult(source.get());
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

TEST(PulsarConnectorIntegrationTest, jsonMessagesFromStandalone) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-json-it-{}", getpid());
  const auto subscription = fmt::format("velox-json-sub-{}", getpid());
  const auto connectorId = "test-pulsar-json";
  auto pool = memory::memoryManager()->addLeafPool();

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig = makeRawConfig(
      serviceUrl, topic, subscription, "100", "2", "true", "individual", "json");
  auto source = createRawDataSource(
      pool,
      connectorConfig,
      connectorId,
      serviceUrl,
      topic,
      subscription,
      "",
      "",
      -1,
      "json",
      ROW({"id", "name"}, {INTEGER(), VARCHAR()}));

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(
      serviceUrl,
      topic,
      messageIds,
      {R"({"id": 11, "name": "first"})", R"({"id": 12, "name": "second"})"});

  auto resultVector = readNextResult(source.get());
  ASSERT_TRUE(resultVector.has_value());
  ASSERT_NE(resultVector.value(), nullptr);
  ASSERT_EQ(resultVector.value()->size(), 2);
  auto ids = resultVector.value()->childAt(0)->as<FlatVector<int32_t>>();
  auto names =
      resultVector.value()->childAt(1)->as<FlatVector<StringView>>();
  ASSERT_EQ(ids->valueAt(0), 11);
  ASSERT_EQ(ids->valueAt(1), 12);
  ASSERT_EQ(names->valueAt(0).str(), "first");
  ASSERT_EQ(names->valueAt(1).str(), "second");
}

TEST(PulsarConnectorIntegrationTest, csvDeserializeFailureNacksMessage) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-csv-it-{}", getpid());
  const auto subscription = fmt::format("velox-csv-sub-{}", getpid());
  const auto connectorId = "test-pulsar-csv";
  auto pool = memory::memoryManager()->addLeafPool();

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig = makeRawConfig(
      serviceUrl, topic, subscription, "100", "1", "true", "individual", "csv");
  auto source = createRawDataSource(
      pool,
      connectorConfig,
      connectorId,
      serviceUrl,
      topic,
      subscription,
      "",
      "",
      -1,
      "csv");

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(serviceUrl, topic, messageIds, {"first,second"});

  ContinueFuture future{folly::Unit{}};
  VELOX_ASSERT_THROW(source->next(0, future), "Not implemented");
  const auto stats = source->runtimeStats();
  ASSERT_EQ(stats.at("pulsarNegativelyAcknowledgedMessages").value, 1);
  ASSERT_EQ(stats.at("pulsarAcknowledgedMessages").value, 0);
}

TEST(PulsarConnectorIntegrationTest, addSplitRejectsMismatchedConfig) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-split-config-it-{}",
      getpid());
  const auto subscription =
      fmt::format("velox-split-config-sub-{}", getpid());
  const auto connectorId = "test-pulsar-split-config";
  auto pool = memory::memoryManager()->addLeafPool();

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig = makeRawConfig(serviceUrl, topic, subscription);
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
  const auto outputType = ROW({"payload"}, {VARCHAR()});
  auto tableHandle =
      std::make_shared<PulsarTableHandle>(connectorId, topic, outputType);
  std::unordered_map<std::string, std::shared_ptr<ColumnHandle>> columnHandles;
  auto source = connector->createDataSource(
      outputType, tableHandle, columnHandles, &queryCtx);

  VELOX_ASSERT_THROW(
      source->addSplit(std::make_shared<PulsarConnectorSplit>(
          connectorId,
          serviceUrl,
          topic,
          fmt::format("{}-other", subscription),
          "raw")),
      "Pulsar split subscription name differs from data source config");
  VELOX_ASSERT_THROW(
      source->addSplit(std::make_shared<PulsarConnectorSplit>(
          connectorId, serviceUrl, topic, subscription, "json")),
      "Pulsar split format differs from data source config");
}

TEST(PulsarConnectorIntegrationTest, emptyTopicBlocksWithFuture) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-empty-it-{}", getpid());
  const auto subscription = fmt::format("velox-empty-sub-{}", getpid());
  const auto connectorId = "test-pulsar-empty";
  auto pool = memory::memoryManager()->addLeafPool();

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig = makeRawConfig(serviceUrl, topic, subscription, "50");
  auto source = createRawDataSource(
      pool, connectorConfig, connectorId, serviceUrl, topic, subscription);

  ContinueFuture future{folly::Unit{}};
  auto result = source->next(0, future);

  ASSERT_FALSE(result.has_value());
  ASSERT_TRUE(future.valid());
  ASSERT_FALSE(future.isReady());
  ASSERT_TRUE(std::move(future).wait(std::chrono::seconds{1}));
}

TEST(PulsarConnectorIntegrationTest, cancelUnblocksReceive) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-cancel-it-{}", getpid());
  const auto subscription = fmt::format("velox-cancel-sub-{}", getpid());
  const auto connectorId = "test-pulsar-cancel";
  auto pool = memory::memoryManager()->addLeafPool();

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig =
      makeRawConfig(serviceUrl, topic, subscription, "5000");
  auto source = createRawDataSource(
      pool, connectorConfig, connectorId, serviceUrl, topic, subscription);

  auto nextResult = std::async(std::launch::async, [&source]() {
    ContinueFuture future{folly::Unit{}};
    return source->next(0, future);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds{200});
  source->cancel();

  ASSERT_EQ(
      nextResult.wait_for(std::chrono::seconds{2}),
      std::future_status::ready);
  auto result = nextResult.get();
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), nullptr);
  auto* pulsarSource = dynamic_cast<PulsarDataSource*>(source.get());
  ASSERT_NE(pulsarSource, nullptr);
  ASSERT_TRUE(pulsarSource->getConsumer()->closed());
}

TEST(PulsarConnectorIntegrationTest, acknowledgeAfterCloseDoesNotThrow) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-ack-close-it-{}", getpid());
  const auto subscription = fmt::format("velox-ack-close-sub-{}", getpid());

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(serviceUrl, topic, messageIds, {"first"});
  auto config = std::make_shared<ConnectionConfig>(
      makeRawConfig(serviceUrl, topic, subscription, "1000", "1"));
  PulsarConsumer consumer(config, config->getReceiveTimeoutMills(), 1);

  std::vector<PulsarMessage> messages;
  size_t messageBytes = 0;
  consumer.consumeBatch(messages, messageBytes);
  ASSERT_EQ(messages.size(), 1);

  consumer.close();
  EXPECT_NO_THROW(consumer.acknowledge(messages[0].message, false));
  EXPECT_EQ(consumer.stats().acknowledgedMessages, 0);
}

TEST(PulsarConnectorIntegrationTest, multipleNextCallsDrainBatchesThenBlock) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-multi-next-it-{}",
      getpid());
  const auto subscription = fmt::format("velox-multi-next-sub-{}", getpid());
  const auto connectorId = "test-pulsar-multi-next";
  auto pool = memory::memoryManager()->addLeafPool();

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(
      serviceUrl, topic, messageIds, {"first", "second", "third"});
  ASSERT_EQ(messageIds.size(), 3);

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig =
      makeRawConfig(serviceUrl, topic, subscription, "10", "2");
  auto source = createRawDataSource(
      pool, connectorConfig, connectorId, serviceUrl, topic, subscription);

  auto firstBatch = readNextResult(source.get());
  ASSERT_TRUE(firstBatch.has_value());
  ASSERT_NE(firstBatch.value(), nullptr);
  ASSERT_EQ(firstBatch.value()->size(), 2);

  auto secondBatch = readNextResult(source.get());
  ASSERT_TRUE(secondBatch.has_value());
  ASSERT_NE(secondBatch.value(), nullptr);
  ASSERT_EQ(secondBatch.value()->size(), 1);

  ContinueFuture future{folly::Unit{}};
  auto empty = source->next(0, future);
  ASSERT_FALSE(empty.has_value());
  ASSERT_TRUE(future.valid());
  ASSERT_FALSE(future.isReady());
  ASSERT_TRUE(std::move(future).wait(std::chrono::seconds{5}));
}

TEST(PulsarConnectorIntegrationTest, cumulativeAckOncePerBatch) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-cumulative-ack-it-{}",
      getpid());
  const auto subscription = fmt::format("velox-cumulative-ack-sub-{}", getpid());
  const auto connectorId = "test-pulsar-cumulative-ack";
  auto pool = memory::memoryManager()->addLeafPool();

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(serviceUrl, topic, messageIds);
  ASSERT_EQ(messageIds.size(), 2);

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig = makeRawConfig(
      serviceUrl, topic, subscription, "100", "2", "true", "cumulative");
  auto source = createRawDataSource(
      pool, connectorConfig, connectorId, serviceUrl, topic, subscription);

  auto resultVector = readNextResult(source.get());
  ASSERT_TRUE(resultVector.has_value());
  ASSERT_NE(resultVector.value(), nullptr);
  ASSERT_EQ(resultVector.value()->size(), 2);

  const auto stats = source->runtimeStats();
  ASSERT_EQ(stats.at("pulsarReceivedMessages").value, 2);
  ASSERT_EQ(stats.at("pulsarAcknowledgedMessages").value, 1);
}

TEST(PulsarConnectorIntegrationTest, endMessageIdFinishesSplit) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-end-it-{}", getpid());
  const auto subscription = fmt::format("velox-end-sub-{}", getpid());
  const auto connectorId = "test-pulsar-end";
  auto pool = memory::memoryManager()->addLeafPool();

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(serviceUrl, topic, messageIds);
  ASSERT_EQ(messageIds.size(), 2);

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig =
      makeRawConfig(serviceUrl, topic, subscription, "100", "2");
  auto source = createRawDataSource(
      pool,
      connectorConfig,
      connectorId,
      serviceUrl,
      topic,
      subscription,
      "earliest",
      messageIdString(messageIds[0]));

  ContinueFuture future{folly::Unit{}};
  auto resultVector = readNextResult(source.get());
  ASSERT_TRUE(resultVector.has_value());
  ASSERT_NE(resultVector.value(), nullptr);
  ASSERT_EQ(resultVector.value()->size(), 1);
  auto payloads =
      resultVector.value()->childAt(0)->as<FlatVector<StringView>>();
  ASSERT_EQ(payloads->valueAt(0).str(), "first");

  auto end = source->next(0, future);
  ASSERT_TRUE(end.has_value());
  ASSERT_EQ(end.value(), nullptr);

  auto repeatedEnd = source->next(0, future);
  ASSERT_TRUE(repeatedEnd.has_value());
  ASSERT_EQ(repeatedEnd.value(), nullptr);

  const auto stats = source->runtimeStats();
  ASSERT_EQ(stats.at("pulsarReceivedMessages").value, 1);
  ASSERT_EQ(stats.at("pulsarNegativelyAcknowledgedMessages").value, 1);
  ASSERT_EQ(stats.at("pulsarSkippedMessagesAfterEnd").value, 1);
}

TEST(PulsarConnectorIntegrationTest, addSplitRecreatesConsumer) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-multi-split-it-{}",
      getpid());
  const auto subscription = fmt::format("velox-multi-split-sub-{}", getpid());
  const auto connectorId = "test-pulsar-multi-split";
  auto pool = memory::memoryManager()->addLeafPool();

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(
      serviceUrl, topic, messageIds, {"first", "second", "third"});
  ASSERT_EQ(messageIds.size(), 3);

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig =
      makeRawConfig(serviceUrl, topic, subscription, "100", "2");
  auto source = createRawDataSource(
      pool,
      connectorConfig,
      connectorId,
      serviceUrl,
      topic,
      subscription,
      "earliest",
      messageIdString(messageIds[0]));

  auto firstResult = readNextResult(source.get());
  ASSERT_TRUE(firstResult.has_value());
  ASSERT_NE(firstResult.value(), nullptr);
  ASSERT_EQ(firstResult.value()->size(), 1);
  auto payloads =
      firstResult.value()->childAt(0)->as<FlatVector<StringView>>();
  ASSERT_EQ(payloads->valueAt(0).str(), "first");

  ContinueFuture future{folly::Unit{}};
  auto firstEnd = source->next(0, future);
  ASSERT_TRUE(firstEnd.has_value());
  ASSERT_EQ(firstEnd.value(), nullptr);

  source->addSplit(std::make_shared<PulsarConnectorSplit>(
      connectorId,
      serviceUrl,
      topic,
      subscription,
      "raw",
      -1,
      messageIdString(messageIds[1]),
      messageIdString(messageIds[1])));

  auto secondResult = readNextResult(source.get());
  ASSERT_TRUE(secondResult.has_value());
  ASSERT_NE(secondResult.value(), nullptr);
  ASSERT_EQ(secondResult.value()->size(), 1);
  payloads = secondResult.value()->childAt(0)->as<FlatVector<StringView>>();
  ASSERT_EQ(payloads->valueAt(0).str(), "second");

  auto secondEnd = source->next(0, future);
  ASSERT_TRUE(secondEnd.has_value());
  ASSERT_EQ(secondEnd.value(), nullptr);
}

TEST(PulsarConnectorIntegrationTest, startMessageIdInclusiveIncludesStart) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-start-include-it-{}",
      getpid());
  const auto subscription = fmt::format("velox-start-include-sub-{}", getpid());
  const auto connectorId = "test-pulsar-start-include";
  auto pool = memory::memoryManager()->addLeafPool();

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(serviceUrl, topic, messageIds);
  ASSERT_EQ(messageIds.size(), 2);

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig =
      makeRawConfig(serviceUrl, topic, subscription, "100", "2", "true");
  auto source = createRawDataSource(
      pool,
      connectorConfig,
      connectorId,
      serviceUrl,
      topic,
      subscription,
      messageIdString(messageIds[0]));

  auto resultVector = readNextResult(source.get());
  ASSERT_TRUE(resultVector.has_value());
  ASSERT_NE(resultVector.value(), nullptr);
  ASSERT_EQ(resultVector.value()->size(), 2);
  auto payloads =
      resultVector.value()->childAt(0)->as<FlatVector<StringView>>();
  ASSERT_EQ(payloads->valueAt(0).str(), "first");
  ASSERT_EQ(payloads->valueAt(1).str(), "second");
}

TEST(PulsarConnectorIntegrationTest, startMessageIdInclusiveExcludesStart) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-start-exclude-it-{}",
      getpid());
  const auto subscription = fmt::format("velox-start-exclude-sub-{}", getpid());
  const auto connectorId = "test-pulsar-start-exclude";
  auto pool = memory::memoryManager()->addLeafPool();

  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(serviceUrl, topic, messageIds);
  ASSERT_EQ(messageIds.size(), 2);

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig =
      makeRawConfig(serviceUrl, topic, subscription, "100", "2", "false");
  auto source = createRawDataSource(
      pool,
      connectorConfig,
      connectorId,
      serviceUrl,
      topic,
      subscription,
      messageIdString(messageIds[0]));

  auto resultVector = readNextResult(source.get());
  ASSERT_TRUE(resultVector.has_value());
  ASSERT_NE(resultVector.value(), nullptr);
  ASSERT_EQ(resultVector.value()->size(), 1);
  auto payloads =
      resultVector.value()->childAt(0)->as<FlatVector<StringView>>();
  ASSERT_EQ(payloads->valueAt(0).str(), "second");
}

TEST(PulsarConnectorIntegrationTest, partitionIndexReadsPartitionedTopic) {
  const auto serviceUrl =
      getEnvOrDefault("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650");
  const auto topic = fmt::format(
      "persistent://public/default/velox-pulsar-ptopic-it-{}", getpid());
  const auto subscription = fmt::format("velox-partition-sub-{}", getpid());
  const auto connectorId = "test-pulsar-partition";
  auto pool = memory::memoryManager()->addLeafPool();

  createPartitionedTopic(topic, 2);
  const auto partitionOneTopic = fmt::format("{}-partition-1", topic);
  std::vector<::pulsar::MessageId> messageIds;
  produceRawMessages(serviceUrl, partitionOneTopic, messageIds, {"partition-1"});

  connector::registerConnectorFactory(
      std::make_shared<connector::pulsar::PulsarConnectorFactory>());
  ConnectorCleanup cleanup(connectorId);

  auto connectorConfig =
      makeRawConfig(serviceUrl, topic, subscription, "100", "1");
  auto source = createRawDataSource(
      pool,
      connectorConfig,
      connectorId,
      serviceUrl,
      topic,
      subscription,
      "",
      "",
      1);

  auto resultVector = readNextResult(source.get());
  ASSERT_TRUE(resultVector.has_value());
  ASSERT_NE(resultVector.value(), nullptr);
  ASSERT_EQ(resultVector.value()->size(), 1);
  auto payloads =
      resultVector.value()->childAt(0)->as<FlatVector<StringView>>();
  ASSERT_EQ(payloads->valueAt(0).str(), "partition-1");
}

} // namespace facebook::velox::connector::pulsar::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  facebook::velox::memory::MemoryManager::testingSetInstance({});
  return RUN_ALL_TESTS();
}
