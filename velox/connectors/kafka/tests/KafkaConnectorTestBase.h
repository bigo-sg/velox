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

#include <fstream>
#include "velox/connectors/kafka/KafkaConnector.h"
#include "velox/connectors/kafka/KafkaConnectorSplit.h"
#include "velox/connectors/kafka/KafkaTableHandle.h"
#include "velox/connectors/kafka/KafkaConfig.h"
#include "velox/type/Type.h"
#include "velox/type/Filter.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

#include "cppkafka/cppkafka.h"

namespace facebook::velox::connector::kafka::test {

class KafkaConnectorTestBase : public exec::test::OperatorTestBase {
 public:
  std::string kafkaInstance = "localhost:9092";
  std::string kafkaTopic = "test_kafka";
  const std::string kafkaConsumeGroupId = "test_kafka_group_id";
  const std::string kafkaDataFormat = "json";
  const std::string kafkaClientId = "test_kafka_client";
  const std::string kafkaAutoOffsetReset = "latest";
  const std::string kafkaStartupMode = "latest-offsets";
  const std::string kKafkaConnectorId = "test-kafka";
  const std::shared_ptr<RowType> outputType = createOutputType();
  const std::shared_ptr<memory::MemoryPool> memoryPool = memory::memoryManager()->addLeafPool();

  void SetUp() override {
    OperatorTestBase::SetUp();
    init();
    connector::registerConnectorFactory(
        std::make_shared<connector::kafka::KafkaConnectorFactory>());
    std::unordered_map<std::string, std::string> configMap;
    configMap[connector::kafka::ConnectionConfig::kBootstrapServers] = kafkaInstance;
    configMap[connector::kafka::ConnectionConfig::kGroupId] = kafkaConsumeGroupId;
    configMap[connector::kafka::ConnectionConfig::kFormat] = kafkaDataFormat;
    configMap[connector::kafka::ConnectionConfig::kClientId] = kafkaClientId;
    configMap[connector::kafka::ConnectionConfig::kTopic] = kafkaTopic;
    configMap[connector::kafka::ConnectionConfig::kAutoResetOffset] = kafkaAutoOffsetReset;
    configMap[connector::kafka::ConnectionConfig::kStartupMode] = kafkaStartupMode;
    std::shared_ptr<const config::ConfigBase> config =
        std::make_shared<const config::ConfigBase>(std::move(configMap));
    auto kafkaConnector =
        connector::getConnectorFactory(
            connector::kafka::KafkaConnectorFactory::kKafkaConnectorName)
            ->newConnector(kKafkaConnectorId, config);
    connector::registerConnector(kafkaConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kKafkaConnectorId);
    connector::unregisterConnectorFactory(
        connector::kafka::KafkaConnectorFactory::kKafkaConnectorName);
    OperatorTestBase::TearDown();
  }

  void init() {
    char* currentPath;
    if ((currentPath = getcwd(NULL, 0)) == NULL) {
      VELOX_FAIL("Failed to get curent path");
    }
    std::string kafkaConfName = "kafka.conf";
    char kafkaConfPath[strlen(currentPath) + kafkaConfName.size() + 1];
    sprintf(kafkaConfPath, "%s%s%s", currentPath, "/", kafkaConfName.data());
    std::ifstream kafkaConfFile(kafkaConfPath);
    if (!kafkaConfFile.is_open()) {
      VELOX_FAIL("Failed to open kafka config file: {}", std::string(kafkaConfPath, strlen(kafkaConfPath)));
    }
    std::string kKafkaTestInstance = "kafka.test.instance";
    std::string kKafkaTestTopic = "kafka.test.topic";
    std::string confLine;
    while(getline(kafkaConfFile, confLine)) {
      if (confLine.empty()) {
        continue;
      }
      if (confLine.find(kKafkaTestInstance) != std::string::npos) {
        kafkaInstance = confLine.substr(kKafkaTestInstance.size() + 1);
      } else if (confLine.find(kKafkaTestTopic) != std::string::npos) {
        kafkaTopic = confLine.substr(kKafkaTestTopic.size() + 1);
      }
    }
    kafkaConfFile.close();
  }

  /// Row<event_type int, bid ROW<auction BIGINT, bidder BIGINT, price BIGINT,
  ///  channel  VARCHAR, url  VARCHAR, `dateTime`  TIMESTAMP(3), extra  VARCHAR>>> 
  static const std::shared_ptr<RowType> createOutputType() {
    std::vector<std::string> bidRowFieldNames = {"auction", "bidder", "price", "channel", "url", "dateTime", "extra"};
    std::vector<TypePtr> bidRowFieldTypes = {
      std::make_shared<const BigintType>(),
      std::make_shared<const BigintType>(),
      std::make_shared<const BigintType>(),
      std::make_shared<const VarcharType>(),
      std::make_shared<const VarcharType>(),
      std::make_shared<const TimestampType>(),
      std::make_shared<const VarcharType>()
    };
    std::shared_ptr<RowType> bidRowType = std::make_shared<RowType>(std::move(bidRowFieldNames), std::move(bidRowFieldTypes));
    std::vector<std::string> outputFieldNames = {"event_type", "bid"};
    std::vector<TypePtr> outputFieldTypes = {
      std::make_shared<const IntegerType>(),
      bidRowType
    };
    return std::make_shared<RowType>(std::move(outputFieldNames), std::move(outputFieldTypes));
  }

  const std::shared_ptr<connector::kafka::KafkaConnectorSplit> createKafkaSplit() {
    std::unordered_map<std::string, std::vector<std::pair<uint32_t, int64_t>>> topicPartitions;
    std::vector<std::pair<uint32_t, int64_t>> partitionOffsets { std::pair<uint32_t, int64_t>(0, 0) };
    topicPartitions[kafkaTopic] = partitionOffsets;
    return std::make_shared<connector::kafka::KafkaConnectorSplit>(
      kKafkaConnectorId,
      kafkaInstance,
      kafkaConsumeGroupId,
      kafkaDataFormat,
      false,
      "earliest",
      topicPartitions
    );
  }

  const std::shared_ptr<connector::kafka::KafkaTableHandle> createKafkaTableHandle() {
    return std::make_shared<connector::kafka::KafkaTableHandle>(
      kKafkaConnectorId,
      kafkaTopic,
      outputType);
  }

  const std::shared_ptr<connector::ConnectorQueryCtx> createQueryCtx() {
    const auto kafkaConnector = getConnector(kKafkaConnectorId);
    const auto connectorConfig = kafkaConnector->connectorConfig();
    std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx =
      std::make_shared<connector::ConnectorQueryCtx>(
          memoryPool.get(),
          nullptr,
          connectorConfig.get(),
          nullptr,
          common::PrefixSortConfig(),
          nullptr,
          nullptr,
          "query.Kafka",
          "task.Kafka",
          "planNodeId.Kafka",
          0,
          "");
    return connectorQueryCtx;
  }

  const std::unique_ptr<DataSource> createDataSource() {
    std::shared_ptr<connector::Connector> kafkaConnector = getConnector(kKafkaConnectorId);
    const std::shared_ptr<KafkaTableHandle> kafkaTableHandle = createKafkaTableHandle();
    std::unordered_map<std::string, std::shared_ptr<ColumnHandle>> columnHandles;
    return kafkaConnector->createDataSource(outputType, kafkaTableHandle, columnHandles, createQueryCtx().get());
  }

  const void sendMessageToKafka(const std::string & message) {
    cppkafka::Configuration config = {{"metadata.broker.list", kafkaInstance}};
    cppkafka::Producer producer(config);
    cppkafka::MessageBuilder builder(kafkaTopic);
    builder.partition(0);
    builder.payload(message);
    producer.produce(builder);
    producer.flush();
    sleep(5);
  }
};

} // namespace facebook::velox::connector::kafka::test
