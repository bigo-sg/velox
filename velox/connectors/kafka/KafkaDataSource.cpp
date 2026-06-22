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

#include "velox/connectors/kafka/KafkaDataSource.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/connectors/kafka/KafkaConnectorSplit.h"
#include "velox/connectors/kafka/KafkaTableHandle.h"
#include "velox/connectors/kafka/format/CSVRecordDeserializer.h"
#include "velox/connectors/kafka/format/RawRecordDeserializer.h"
#include "velox/connectors/kafka/format/StreamJSONRecordDeserializer.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::connector::kafka {

namespace {
constexpr const char* kTaskIndex = "task_index";
constexpr const char* kTaskParallelism = "task_parallelism";
} // namespace

KafkaDataSource::KafkaDataSource(
    const RowTypePtr& outputType,
    const TableHandlePtr& tableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    const ConnectionConfigPtr& config)
    : queryCtx_(connectorQueryCtx),
      config_(config),
      outputType_(outputType),
      batchSize_(config_->getDataBatchSize()) {
  VELOX_CHECK(batchSize_ > 0, "Batch size config value must greater than 0.");
  const std::shared_ptr<KafkaTableHandle> kafkaTableHandle =
      std::dynamic_pointer_cast<KafkaTableHandle>(tableHandle);
  if (kafkaTableHandle) {
    const std::unordered_map<std::string, std::string>& tableParams =
        kafkaTableHandle->tableParameters();
    config_ = config_->updateAndGetAllConfigs<ConnectionConfig>(tableParams);
  } else {
    VELOX_FAIL(
        "The table handle {} is not supported for kafka data source.",
        tableHandle->connectorId());
  }
  applyTaskScopedClientId();
  if (consumerCanbeCreated()) {
    cppkafka::Configuration cppKafkaConfig =
        config_->getCppKafkaConfiguration();
    createConsumer(cppKafkaConfig);
  }
  createCachedQueue(batchSize_);
  createRecordDeserializer(config_->getFormat(), outputType_);
}

bool KafkaDataSource::consumerCanbeCreated() {
  return config_->exists(ConnectionConfig::kBootstrapServers) &&
      config_->exists(ConnectionConfig::kClientId) &&
      config_->exists(ConnectionConfig::kTopic) &&
      config_->exists(ConnectionConfig::kGroupId) &&
      config_->exists(ConnectionConfig::kFormat) && !consumer_.get();
}

void KafkaDataSource::createConsumer(cppkafka::Configuration& config) {
  VELOX_CHECK_NULL(
      consumer_.get(),
      "Failed to create kafka consumer as the consumer is not null");
  CppKafkaConsumerPtr cppKafkaConsumer =
      std::make_shared<cppkafka::Consumer>(config);
  cppKafkaConsumer->set_destroy_flags(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
  consumer_ = std::make_shared<KafkaConsumer>(
      cppKafkaConsumer, config_->getPollTimeoutMills(), batchSize_);
}

void KafkaDataSource::createCachedQueue(const uint32_t size) {
  VELOX_CHECK_GT(
      size, 0, "Kafka consume message queue size must greater than 0");
  queue_.reserve(size);
}

void KafkaDataSource::createRecordDeserializer(
    const std::string& format,
    const RowTypePtr& outputType) {
  if (format == "json") {
    deserializer_ = std::make_shared<KafkaStreamJSONRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else if (format == "csv") {
    deserializer_ = std::make_shared<KafkaCSVRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else if (format == "raw") {
    deserializer_ = std::make_shared<KafkaRawRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else {
    VELOX_FAIL_UNSUPPORTED_INPUT_UNCATCHABLE(
        "The data format {} is not supported for kafka.", format);
  }
  outRow_ = RowVector::createEmpty(outputType_, queryCtx_->memoryPool());
  outRow_->resize(1);
}

void KafkaDataSource::addSplit(ConnectorSplitPtr split) {
  const auto kafkaConnectorSplit =
      std::dynamic_pointer_cast<KafkaConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(
      kafkaConnectorSplit.get(),
      "Failed to add split, because the kafka connector split is null.");
  VELOX_CHECK_NOT_NULL(
      consumer_.get(),
      "Failed to add split, because the kafka consumer is null.");
  VELOX_CHECK_EQ(
      kafkaConnectorSplit->bootstrapServers_,
      config_->getBootstrapServers(),
      "Failed to add split, because split bootstrap servers are different from kafka config.");
  VELOX_CHECK_EQ(
      kafkaConnectorSplit->groupId_,
      config_->getGroupId(),
      "Failed to add split, because split group id is different from kafka config.");
  VELOX_CHECK_EQ(
      kafkaConnectorSplit->format_,
      config_->getFormat(),
      "Failed to add split, because split format is different from kafka config.");
  cppkafka::TopicPartitionList topicPartitions =
      getSplitTopicPartitions(*kafkaConnectorSplit);
  consumer_->setTopicPartitionsOffset(
      topicPartitions, config_->getStartupMode());
  consumer_->assign(topicPartitions);
}

cppkafka::TopicPartitionList KafkaDataSource::getSplitTopicPartitions(
    const KafkaConnectorSplit& split) {
  cppkafka::TopicPartitionList topicPartitions =
      split.getCppKafkaTopicPartitions();
  if (topicPartitions.empty()) {
    return selectPartitionsForTask(consumer_->getTopicPartitions(
        config_->getTopic(), config_->getStartupMode()));
  }
  for (const auto& topicPartition : topicPartitions) {
    VELOX_CHECK_EQ(
        topicPartition.get_topic(),
        config_->getTopic(),
        "Failed to add split, because split topic is different from kafka config.");
  }
  return topicPartitions;
}

cppkafka::TopicPartitionList KafkaDataSource::selectPartitionsForTask(
    const cppkafka::TopicPartitionList& topicPartitions) const {
  const int32_t taskIndex = getTaskIndex();
  const int32_t taskParallelism = getTaskParallelism();
  VELOX_CHECK_GE(taskIndex, 0, "Kafka task index must not be negative.");
  VELOX_CHECK_GT(
      taskParallelism, 0, "Kafka task parallelism must be positive.");
  VELOX_CHECK_LT(
      taskIndex,
      taskParallelism,
      "Kafka task index must be less than task parallelism.");

  cppkafka::TopicPartitionList selected;
  for (const auto& topicPartition : topicPartitions) {
    if (topicPartition.get_partition() % taskParallelism == taskIndex) {
      selected.emplace_back(topicPartition);
    }
  }
  return selected;
}

int32_t KafkaDataSource::getTaskIndex() const {
  return std::stoi(
      queryCtx_->sessionProperties()->get<std::string>(kTaskIndex, "0"));
}

int32_t KafkaDataSource::getTaskParallelism() const {
  return std::stoi(
      queryCtx_->sessionProperties()->get<std::string>(kTaskParallelism, "1"));
}

void KafkaDataSource::applyTaskScopedClientId() {
  if (!config_->exists(ConnectionConfig::kClientId)) {
    return;
  }
  config_ = config_->updateAndGetAllConfigs<ConnectionConfig>(
      {{ConnectionConfig::kClientId,
        fmt::format("{}-{}", config_->getClientId(), getTaskIndex())}});
}

std::optional<RowVectorPtr> KafkaDataSource::next(
    uint64_t,
    velox::ContinueFuture&) {
  std::optional<RowVectorPtr> res;
  size_t consumedMsgBytes = 0;
  if (queue_.empty()) {
    // consume the data batch from kafka, and stored the consumed data in the
    // queue.
    consumer_->consumeBatch(queue_, consumedMsgBytes);
    consumePos_ = 0;
    // If nothing consumed, return directly.
    if (consumedMsgBytes == 0) {
      return res;
    }
  }
  outRow_->prepareForReuse();
  // If batchSize > 1 and set `processDataSize = queue.size`, means to process
  // the entrie batch that consumed and stored in the `queue` at once; If
  // batchSize = 1 and set `processDataSize = 1`, means to process the consumed
  // batch data one by one.
  size_t processDataSize = batchSize_ > 1 ? queue_.size() : batchSize_;
  outRow_->resize(processDataSize);
  // Deserialize the consumed data. The `processDataSize` determines how many
  // data would be deserialized at once.
  for (size_t pos = 0; pos < processDataSize; ++pos) {
    deserializer_->deserialize(queue_[pos + consumePos_], pos, outRow_);
    completedBytes_ += queue_[pos + consumePos_].size();
    completedRows_ += 1;
  }
  res.emplace(std::dynamic_pointer_cast<RowVector>(outRow_));
  consumePos_ += processDataSize;
  if (consumePos_ >= queue_.size()) {
    queue_.clear();
    consumePos_ = 0;
  }
  return res;
}

std::unordered_map<std::string, RuntimeCounter>
KafkaDataSource::runtimeStats() {
  std::unordered_map<std::string, RuntimeCounter> stats;
  return stats;
}
} // namespace facebook::velox::connector::kafka
