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

#include <fmt/format.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <cstdio>
#include <fstream>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/pulsar/PulsarConfig.h"
#include "velox/connectors/pulsar/PulsarConnectorSplit.h"
#include "velox/connectors/pulsar/PulsarPartitionUtils.h"
#include "velox/connectors/pulsar/PulsarTableHandle.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::pulsar::test {

namespace {

std::shared_ptr<const config::ConfigBase> makeConfig(
    std::unordered_map<std::string, std::string> values) {
  return std::make_shared<const config::ConfigBase>(std::move(values));
}

} // namespace

TEST(PulsarConnectorTest, connectionConfigRequiredValues) {
  ConnectionConfig config(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kTopic, "persistent://public/default/topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "json"},
  }));

  EXPECT_EQ(config.getServiceUrl(), "pulsar://localhost:6650");
  EXPECT_EQ(config.getAdminUrl(), "http://localhost:8080");
  EXPECT_EQ(config.getTopic(), "persistent://public/default/topic");
  EXPECT_EQ(config.getSubscriptionName(), "sub");
  EXPECT_EQ(config.getFormat(), "json");
  EXPECT_EQ(config.getConsumerName(), "");
  EXPECT_EQ(config.getSubscriptionType(), "shared");
  EXPECT_EQ(config.getInitialPosition(), "latest");
  EXPECT_EQ(config.getAckMode(), "individual");
  EXPECT_EQ(config.getAuthToken(), "");
  EXPECT_EQ(config.getAuthTokenFile(), "");
  EXPECT_EQ(config.getReceiverQueueSize(), 1000);
  EXPECT_EQ(config.getDataBatchSize(), 500);
  EXPECT_EQ(config.getReceiveTimeoutMills(), 100);
  EXPECT_TRUE(config.getAcknowledgeMessages());
  EXPECT_FALSE(config.getCheckpointEnabled());
}

TEST(PulsarConnectorTest, connectionConfigOverrides) {
  ConnectionConfig config(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kConsumerName, "consumer"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kSubscriptionType, "key-shared"},
      {ConnectionConfig::kInitialPosition, "earliest"},
      {ConnectionConfig::kReceiverQueueSize, "10"},
      {ConnectionConfig::kDataBatchSize, "20"},
      {ConnectionConfig::kReceiveTimeoutMills, "30"},
      {ConnectionConfig::kAcknowledgeMessages, "false"},
      {ConnectionConfig::kCheckpointEnabled, "true"},
      {ConnectionConfig::kAckMode, "cumulative"},
      {ConnectionConfig::kAuthToken, "token"},
      {ConnectionConfig::kAuthTokenFile, "/tmp/token"},
  }));

  EXPECT_EQ(config.getAdminUrl(), "http://localhost:18080");
  EXPECT_EQ(config.getConsumerName(), "consumer");
  EXPECT_EQ(config.getSubscriptionType(), "key-shared");
  EXPECT_EQ(config.getInitialPosition(), "earliest");
  EXPECT_EQ(config.getReceiverQueueSize(), 10);
  EXPECT_EQ(config.getDataBatchSize(), 20);
  EXPECT_EQ(config.getReceiveTimeoutMills(), 30);
  EXPECT_FALSE(config.getAcknowledgeMessages());
  EXPECT_TRUE(config.getCheckpointEnabled());
  EXPECT_EQ(config.getAckMode(), "cumulative");
  EXPECT_EQ(config.getAuthToken(), "token");
  EXPECT_EQ(config.getAuthTokenFile(), "/tmp/token");
}

TEST(PulsarConnectorTest, missingRequiredConfigFails) {
  ConnectionConfig config(makeConfig({
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "json"},
  }));

  VELOX_ASSERT_THROW(config.getServiceUrl(), "has no specified value");
}

TEST(PulsarConnectorTest, invalidBooleanConfigFails) {
  ConnectionConfig acknowledgeConfig(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kAcknowledgeMessages, "maybe"},
  }));

  VELOX_ASSERT_THROW(
      acknowledgeConfig.getAcknowledgeMessages(),
      "Invalid Pulsar acknowledge messages config");

  ConnectionConfig checkpointConfig(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kCheckpointEnabled, "maybe"},
  }));

  VELOX_ASSERT_THROW(
      checkpointConfig.getCheckpointEnabled(),
      "Invalid Pulsar checkpoint enabled config");
}

TEST(PulsarConnectorTest, consumerConfigurationOverrides) {
  ConnectionConfig sharedConfig(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kConsumerName, "consumer"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kSubscriptionType, "shared"},
      {ConnectionConfig::kInitialPosition, "earliest"},
      {ConnectionConfig::kReceiverQueueSize, "7"},
      {ConnectionConfig::kDataBatchSize, "20"},
      {ConnectionConfig::kReceiveTimeoutMills, "30"},
  }));

  auto sharedConsumerConfig = sharedConfig.getPulsarConsumerConfiguration();
  EXPECT_EQ(sharedConsumerConfig.getConsumerType(), ::pulsar::ConsumerShared);
  EXPECT_EQ(
      sharedConsumerConfig.getSubscriptionInitialPosition(),
      ::pulsar::InitialPositionEarliest);
  EXPECT_EQ(sharedConsumerConfig.getReceiverQueueSize(), 7);
  EXPECT_EQ(
      sharedConsumerConfig.getBatchReceivePolicy().getMaxNumMessages(), 20);
  EXPECT_EQ(sharedConsumerConfig.getBatchReceivePolicy().getMaxNumBytes(), 0);
  EXPECT_EQ(sharedConsumerConfig.getBatchReceivePolicy().getTimeoutMs(), 30);
  EXPECT_EQ(sharedConsumerConfig.getConsumerName(), "consumer");

  ConnectionConfig failoverConfig(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kSubscriptionType, "failover"},
      {ConnectionConfig::kInitialPosition, "latest"},
  }));
  auto failoverConsumerConfig = failoverConfig.getPulsarConsumerConfiguration();
  EXPECT_EQ(
      failoverConsumerConfig.getConsumerType(), ::pulsar::ConsumerFailover);
  EXPECT_EQ(
      failoverConsumerConfig.getSubscriptionInitialPosition(),
      ::pulsar::InitialPositionLatest);

  ConnectionConfig keySharedConfig(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kSubscriptionType, "key_shared"},
  }));
  auto keySharedConsumerConfig =
      keySharedConfig.getPulsarConsumerConfiguration();
  EXPECT_EQ(
      keySharedConsumerConfig.getConsumerType(), ::pulsar::ConsumerKeyShared);
}

TEST(PulsarConnectorTest, invalidConsumerConfigurationFails) {
  ConnectionConfig subscriptionTypeConfig(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kSubscriptionType, "round-robin"},
  }));

  VELOX_ASSERT_THROW(
      subscriptionTypeConfig.getPulsarConsumerConfiguration(),
      "Unsupported Pulsar subscription type");

  ConnectionConfig initialPositionConfig(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kInitialPosition, "middle"},
  }));

  VELOX_ASSERT_THROW(
      initialPositionConfig.getPulsarConsumerConfiguration(),
      "Unsupported Pulsar initial position");
}

TEST(PulsarConnectorTest, tokenFileClientConfiguration) {
  const auto tokenFile =
      fmt::format("/tmp/velox-pulsar-token-test-{}.txt", getpid());
  {
    std::ofstream out(tokenFile);
    out << " test-token \n";
  }

  ConnectionConfig config(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kAuthTokenFile, tokenFile},
  }));

  auto clientConfig = config.getPulsarClientConfiguration();
  EXPECT_EQ(clientConfig.getAuth().getAuthMethodName(), "token");
  ::pulsar::AuthenticationDataPtr authData;
  ASSERT_EQ(clientConfig.getAuth().getAuthData(authData), ::pulsar::ResultOk);
  ASSERT_NE(authData, nullptr);
  EXPECT_TRUE(authData->hasDataFromCommand());
  EXPECT_EQ(authData->getCommandData(), "test-token");

  std::remove(tokenFile.c_str());
}

TEST(PulsarConnectorTest, missingTokenFileFails) {
  ConnectionConfig config(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
      {ConnectionConfig::kAdminUrl, "http://localhost:18080"},
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "raw"},
      {ConnectionConfig::kAuthTokenFile, "/tmp/velox-missing-pulsar-token"},
  }));

  VELOX_ASSERT_THROW(
      config.getPulsarClientConfiguration(),
      "Failed to read Pulsar token file");
}

TEST(PulsarConnectorTest, splitSerialization) {
  PulsarConnectorSplit split(
      "pulsar",
      "pulsar://localhost:6650",
      "persistent://public/default/topic",
      "sub",
      "json",
      {{"persistent://public/default/topic-partition-3", "1:2", false}});

  const auto serialized = split.serialize();
  const auto copy = PulsarConnectorSplit::create(serialized);

  EXPECT_EQ(copy->connectorId, "pulsar");
  EXPECT_EQ(copy->serviceUrl_, "pulsar://localhost:6650");
  EXPECT_EQ(copy->topic_, "persistent://public/default/topic");
  EXPECT_EQ(copy->subscriptionName_, "sub");
  EXPECT_EQ(copy->format_, "json");
  ASSERT_EQ(copy->topicPartitions_.size(), 1);
  EXPECT_EQ(
      copy->topicPartitions_[0].partitionedTopic,
      "persistent://public/default/topic-partition-3");
  EXPECT_EQ(copy->topicPartitions_[0].messageId, "1:2");
  EXPECT_FALSE(copy->topicPartitions_[0].startMessageIdInclusive);
  EXPECT_NE(
      copy->toString().find("persistent://public/default/topic"),
      std::string::npos);
}

TEST(PulsarConnectorTest, checkpointStateSplitSerialization) {
  PulsarConnectorSplit split(
      "pulsar",
      "pulsar://localhost:6650",
      "persistent://public/default/topic",
      "sub",
      "raw",
      {{"persistent://public/default/topic-partition-2", "10:20:0", false}});

  const auto serialized = split.serialize();

  EXPECT_EQ(serialized["name"].asString(), "PulsarConnectorSplit");
  EXPECT_EQ(serialized["connectorId"].asString(), "pulsar");
  EXPECT_EQ(serialized["serviceUrl"].asString(), "pulsar://localhost:6650");
  EXPECT_EQ(
      serialized["topic"].asString(), "persistent://public/default/topic");
  EXPECT_EQ(serialized["subscriptionName"].asString(), "sub");
  EXPECT_EQ(serialized["format"].asString(), "raw");
  ASSERT_EQ(serialized["topicPartitions"].size(), 1);
  EXPECT_EQ(
      serialized["topicPartitions"][0]["partitionedTopic"].asString(),
      "persistent://public/default/topic-partition-2");
  EXPECT_EQ(
      serialized["topicPartitions"][0]["messageId"].asString(), "10:20:0");
  EXPECT_FALSE(
      serialized["topicPartitions"][0]["startMessageIdInclusive"].asBool());
  EXPECT_EQ(serialized.count("messageId"), 0);
  EXPECT_EQ(serialized.count("startMessageIdInclusive"), 0);

  const auto copy = PulsarConnectorSplit::create(serialized);
  ASSERT_EQ(copy->topicPartitions_.size(), 1);
  EXPECT_EQ(copy->topicPartitions_[0].messageId, "10:20:0");
  EXPECT_FALSE(copy->topicPartitions_[0].startMessageIdInclusive);
}

TEST(PulsarConnectorTest, serdeRoundTripThroughRegistry) {
  Type::registerSerDe();
  PulsarConnectorSplit::registerSerDe();
  PulsarTableHandle::registerSerDe();

  PulsarConnectorSplit split(
      "pulsar",
      "pulsar://localhost:6650",
      "persistent://public/default/topic",
      "sub",
      "raw");
  const auto splitCopy =
      ISerializable::deserialize<ConnectorSplit>(split.serialize());
  ASSERT_NE(splitCopy, nullptr);
  EXPECT_NE(
      dynamic_cast<const PulsarConnectorSplit*>(splitCopy.get()), nullptr);

  const auto rowType = ROW({"payload"}, {VARCHAR()});
  PulsarTableHandle handle("pulsar", "topic", rowType);
  const auto handleCopy = ISerializable::deserialize<ConnectorTableHandle>(
      handle.serialize(), nullptr);
  ASSERT_NE(handleCopy, nullptr);
  EXPECT_NE(dynamic_cast<const PulsarTableHandle*>(handleCopy.get()), nullptr);
}

TEST(PulsarConnectorTest, splitSerializationDefaults) {
  folly::dynamic serialized = folly::dynamic::object;
  serialized["connectorId"] = "pulsar";
  serialized["serviceUrl"] = "pulsar://localhost:6650";
  serialized["topic"] = "persistent://public/default/topic";
  serialized["subscriptionName"] = "sub";
  serialized["format"] = "raw";

  const auto copy = PulsarConnectorSplit::create(serialized);

  EXPECT_TRUE(copy->topicPartitions_.empty());
}

TEST(PulsarConnectorTest, partitionedTopicName) {
  EXPECT_EQ(
      partitionedTopicName("persistent://public/default/topic", -1),
      "persistent://public/default/topic");
  EXPECT_EQ(
      partitionedTopicName("persistent://public/default/topic", 2),
      "persistent://public/default/topic-partition-2");
}

TEST(PulsarConnectorTest, tableHandleSerialization) {
  Type::registerSerDe();

  const auto rowType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  PulsarTableHandle handle(
      "pulsar",
      "topic",
      rowType,
      {
          {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
          {ConnectionConfig::kSubscriptionName, "sub"},
      });

  const auto serialized = handle.serialize();
  const auto copy = PulsarTableHandle::create(serialized, nullptr);
  const auto* pulsarCopy = dynamic_cast<const PulsarTableHandle*>(copy.get());

  ASSERT_NE(pulsarCopy, nullptr);
  EXPECT_EQ(pulsarCopy->connectorId(), "pulsar");
  EXPECT_EQ(pulsarCopy->tableName(), "topic");
  ASSERT_NE(pulsarCopy->dataColumns(), nullptr);
  EXPECT_EQ(pulsarCopy->dataColumns()->toString(), rowType->toString());
  EXPECT_EQ(
      pulsarCopy->tableParameters().at(ConnectionConfig::kServiceUrl),
      "pulsar://localhost:6650");
  EXPECT_EQ(
      pulsarCopy->tableParameters().at(ConnectionConfig::kSubscriptionName),
      "sub");
}

} // namespace facebook::velox::connector::pulsar::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
