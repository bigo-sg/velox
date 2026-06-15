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

#include "velox/connectors/pulsar/PulsarConfig.h"
#include "velox/connectors/pulsar/PulsarConnectorSplit.h"
#include "velox/connectors/pulsar/PulsarTableHandle.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Type.h"
#include <folly/init/Init.h>
#include <gtest/gtest.h>

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
  EXPECT_EQ(config.getTopic(), "persistent://public/default/topic");
  EXPECT_EQ(config.getSubscriptionName(), "sub");
  EXPECT_EQ(config.getFormat(), "json");
  EXPECT_EQ(config.getConsumerName(), "");
  EXPECT_EQ(config.getSubscriptionType(), "exclusive");
  EXPECT_EQ(config.getInitialPosition(), "latest");
  EXPECT_EQ(config.getReceiverQueueSize(), 1000);
  EXPECT_EQ(config.getDataBatchSize(), 500);
  EXPECT_EQ(config.getReceiveTimeoutMills(), 100);
  EXPECT_TRUE(config.getAcknowledgeMessages());
}

TEST(PulsarConnectorTest, connectionConfigOverrides) {
  ConnectionConfig config(makeConfig({
      {ConnectionConfig::kServiceUrl, "pulsar://localhost:6650"},
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
  }));

  EXPECT_EQ(config.getConsumerName(), "consumer");
  EXPECT_EQ(config.getSubscriptionType(), "key-shared");
  EXPECT_EQ(config.getInitialPosition(), "earliest");
  EXPECT_EQ(config.getReceiverQueueSize(), 10);
  EXPECT_EQ(config.getDataBatchSize(), 20);
  EXPECT_EQ(config.getReceiveTimeoutMills(), 30);
  EXPECT_FALSE(config.getAcknowledgeMessages());
}

TEST(PulsarConnectorTest, missingRequiredConfigFails) {
  ConnectionConfig config(makeConfig({
      {ConnectionConfig::kTopic, "topic"},
      {ConnectionConfig::kSubscriptionName, "sub"},
      {ConnectionConfig::kFormat, "json"},
  }));

  VELOX_ASSERT_THROW(config.getServiceUrl(), "has no specified value");
}

TEST(PulsarConnectorTest, splitSerialization) {
  PulsarConnectorSplit split(
      "pulsar",
      "pulsar://localhost:6650",
      "persistent://public/default/topic",
      "sub",
      "json");

  const auto serialized = split.serialize();
  const auto copy = PulsarConnectorSplit::create(serialized);

  EXPECT_EQ(copy->connectorId, "pulsar");
  EXPECT_EQ(copy->serviceUrl_, "pulsar://localhost:6650");
  EXPECT_EQ(copy->topic_, "persistent://public/default/topic");
  EXPECT_EQ(copy->subscriptionName_, "sub");
  EXPECT_EQ(copy->format_, "json");
  EXPECT_NE(
      copy->toString().find("persistent://public/default/topic"),
      std::string::npos);
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
