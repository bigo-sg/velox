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

#include "velox/connectors/Connector.h"
#include "velox/connectors/kafka/KafkaDataSource.h"
#include "velox/connectors/kafka/format/StreamJSONRecordDeserializer.h"
#include "velox/connectors/kafka/tests/KafkaConnectorTestBase.h"
#include "velox/vector/ComplexVector.h"
#include "velox/type/Timestamp.h"
#include "velox/type/StringView.h"
#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::connector::kafka::test {

class KafkaConnectorTest : public KafkaConnectorTestBase {};

TEST_F(KafkaConnectorTest, testConfig) {
    std::shared_ptr<connector::Connector> kafkaConnector = getConnector(kKafkaConnectorId);
    ASSERT_TRUE(kafkaConnector != nullptr);
    const std::shared_ptr<const config::ConfigBase>& connectorConfig = kafkaConnector->connectorConfig();
    const std::shared_ptr<const KafkaConfig> kafkaConfig = std::make_shared<const KafkaConfig>(connectorConfig);
    ASSERT_TRUE(kafkaConfig->exists(connector::kafka::ConnectionConfig::kBootstrapServers));
    ASSERT_TRUE(kafkaConfig->exists(connector::kafka::ConnectionConfig::kTopic));
    ASSERT_TRUE(kafkaConfig->exists(connector::kafka::ConnectionConfig::kClientId));
    ASSERT_TRUE(kafkaConfig->exists(connector::kafka::ConnectionConfig::kGroupId));
    ASSERT_TRUE(kafkaConfig->exists(connector::kafka::ConnectionConfig::kFormat));
}

TEST_F(KafkaConnectorTest, testKafkaConsumer) {
  const std::unique_ptr<DataSource> dataSource = createDataSource();
  KafkaDataSource * kafkaDataSource = reinterpret_cast<KafkaDataSource *>(dataSource.get());
  ASSERT_TRUE(kafkaDataSource != nullptr);
  const auto & kafkaConsumer = kafkaDataSource->getConsumer();
  ASSERT_TRUE(kafkaConsumer != nullptr);
  std::vector<std::string> topics = kafkaConsumer->getSubscribedTopics();
  ASSERT_TRUE(topics.size() > 0);
  ASSERT_TRUE(topics[0] == kafkaTopic);
}

TEST_F(KafkaConnectorTest, testAssignPartitions) {
  const std::unique_ptr<DataSource> dataSource = createDataSource();
  KafkaDataSource* kafkaDataSource = reinterpret_cast<KafkaDataSource *>(dataSource.get());
  ASSERT_TRUE(kafkaDataSource != nullptr);
  kafkaDataSource->addSplit(createKafkaSplit());
  const auto & kafkaConsumer = kafkaDataSource->getConsumer();
  ASSERT_TRUE(kafkaConsumer != nullptr);
  const cppkafka::TopicPartitionList tps = kafkaConsumer->getAssignedTopicPartitions();
  ASSERT_TRUE(tps.size() > 0);
  const cppkafka::TopicPartition tp = tps[0];
  ASSERT_TRUE(tp.get_topic() == kafkaTopic);
  ASSERT_TRUE(tp.get_partition() == 0);
  ASSERT_TRUE(tp.get_offset() > 0);
}

TEST_F(KafkaConnectorTest, testConsumeMessages) {
  const std::unique_ptr<DataSource> dataSource = createDataSource();
  KafkaDataSource* kafkaDataSource = reinterpret_cast<KafkaDataSource *>(dataSource.get());
  ASSERT_TRUE(kafkaDataSource != nullptr);
  kafkaDataSource->addSplit(createKafkaSplit());
  std::string testMsg = "This is a test message!";
  sendMessageToKafka(testMsg);
  const auto & kafkaConsumer = kafkaDataSource->getConsumer();
  ASSERT_TRUE(kafkaConsumer != nullptr);
  std::vector<std::string> msgs;
  size_t msgBytes = 0;
  kafkaConsumer->consumeBatch(msgs, msgBytes);
  ASSERT_TRUE(msgs.size() == 1);
  ASSERT_TRUE(msgs[0] == testMsg);
  ASSERT_TRUE(msgBytes == testMsg.size());
}

TEST_F(KafkaConnectorTest, testCreateDeserializer) {
  const std::unique_ptr<DataSource> dataSource = createDataSource();
  KafkaDataSource* kafkaDataSource = reinterpret_cast<KafkaDataSource *>(dataSource.get());
  ASSERT_TRUE(kafkaDataSource != nullptr);
  const auto & deserializer = kafkaDataSource->getDeserializer();
  const std::shared_ptr<KafkaStreamJSONRecordDeserializer> jsonDeserializer =
      std::dynamic_pointer_cast<KafkaStreamJSONRecordDeserializer>(deserializer);
  ASSERT_TRUE(jsonDeserializer != nullptr);
}

TEST_F(KafkaConnectorTest, testDeserializeMessages) {
  const std::unique_ptr<DataSource> dataSource = createDataSource();
  KafkaDataSource* kafkaDataSource = reinterpret_cast<KafkaDataSource *>(dataSource.get());
  ASSERT_TRUE(kafkaDataSource != nullptr);
  const auto & deserializer = kafkaDataSource->getDeserializer();
  std::string msg = "{\"event_type\":1, \"bid\": {\"auction\":1, \"bidder\":222, \"price\":1113, \"channel\":\"OTS\", \"url\":\"http://testkafka/a/b/c\", \"dateTime\":\"2025-06-18 11:22:33\", \"extra\":\"xxxx\"}}";
  VectorPtr vec1 = RowVector::createEmpty(outputType, memoryPool.get());
  vec1->resize(1);
  deserializer->deserialize(msg, 0, vec1);
  std::shared_ptr<RowVector> rowVector1 = std::dynamic_pointer_cast<RowVector>(vec1);
  ASSERT_TRUE(rowVector1 != nullptr);
  ASSERT_TRUE(rowVector1->size() == 1);
  ASSERT_TRUE(rowVector1->children().size() == 2);
  std::shared_ptr<FlatVector<int32_t>> flat = std::dynamic_pointer_cast<FlatVector<int32_t>>(rowVector1->childAt(0));
  ASSERT_TRUE(flat != nullptr);
  ASSERT_TRUE(flat->size() == 1);
  ASSERT_TRUE(flat->valueAt(0) == 1);
  std::shared_ptr<RowVector> subRow = std::dynamic_pointer_cast<RowVector>(rowVector1->childAt(1));
  ASSERT_TRUE(subRow != nullptr);
  ASSERT_TRUE(subRow->size() == 1);
  ASSERT_TRUE(subRow->children().size() == 7);
  std::shared_ptr<FlatVector<int64_t>> f1 = std::dynamic_pointer_cast<FlatVector<int64_t>>(subRow->childAt(0));
  std::shared_ptr<FlatVector<facebook::velox::StringView>> f2 = 
    std::dynamic_pointer_cast<FlatVector<facebook::velox::StringView>>(subRow->childAt(3));
  std::shared_ptr<FlatVector<facebook::velox::Timestamp>> f3 =
    std::dynamic_pointer_cast<FlatVector<facebook::velox::Timestamp>>(subRow->childAt(5));
  ASSERT_TRUE(f1 != nullptr && f2 != nullptr && f3 != nullptr);
  ASSERT_TRUE(f1->valueAt(0) == 1);
  ASSERT_TRUE(f2->valueAt(0).str() == "OTS");
  ASSERT_TRUE(f3->valueAt(0).toMillis() == 1750245753000);
}

TEST_F(KafkaConnectorTest, testKafkaSourceNext) {
  const std::unique_ptr<DataSource> dataSource = createDataSource();
  KafkaDataSource* kafkaDataSource = reinterpret_cast<KafkaDataSource *>(dataSource.get());
  ASSERT_TRUE(kafkaDataSource != nullptr);
  kafkaDataSource->addSplit(createKafkaSplit());
  std::string msg1 = "{\"event_type\":1, \"bid\": {\"auction\":1, \"bidder\":222, \"price\":1113, \"channel\":\"OTS\", \"url\":\"http://testkafka/a/b/c\", \"dateTime\":\"2025-06-18 11:22:33\", \"extra\":\"xxxx\"}}";
  std::string msg2 = "{\"event_type\":2, \"bid\": {\"auction\":2, \"bidder\":223, \"price\":1114, \"channel\":\"OTS-1\", \"url\":\"http://testkafka/a/b/c\", \"dateTime\":\"2025-06-18 11:22:33\", \"extra\":\"xxxx\"}}";
  std::string msg3 = "{\"event_type\":3, \"bid\": {\"auction\":3, \"bidder\":225, \"price\":1115, \"channel\":\"OTS-2\", \"url\":\"http://testkafka/a/b/c\", \"dateTime\":\"2025-06-18 11:22:33\", \"extra\":\"xxxx\"}}";
  sendMessageToKafka(msg1);
  sendMessageToKafka(msg2);
  sendMessageToKafka(msg3);
  auto future = facebook::velox::ContinueFuture{folly::Unit{}};
  std::optional<RowVectorPtr> res = kafkaDataSource->next(0, future);
  ASSERT_TRUE(res.value() != nullptr);
  RowVectorPtr rowVector = res.value();
  ASSERT_TRUE(rowVector->size() == 3);
  std::shared_ptr<FlatVector<int32_t>> flat = std::dynamic_pointer_cast<FlatVector<int32_t>>(rowVector->childAt(0));
  std::shared_ptr<RowVector> subRow = std::dynamic_pointer_cast<RowVector>(rowVector->childAt(1));
  ASSERT_TRUE(flat != nullptr && flat->size() == 3);
  ASSERT_TRUE(subRow != nullptr && subRow->size() == 3);
  std::shared_ptr<FlatVector<int64_t>> subF1 = std::dynamic_pointer_cast<FlatVector<int64_t>>(subRow->childAt(0));
  ASSERT_TRUE(subF1 != nullptr && subF1->size() == 3);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(flat->valueAt(i) == i+1);
    ASSERT_TRUE(subF1->valueAt(i) == i+1);
  }
}

} // namespace facebook::velox::connector::kafka::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}