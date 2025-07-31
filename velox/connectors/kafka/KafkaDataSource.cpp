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
#include "velox/connectors/kafka/KafkaTableHandle.h"
#include "velox/connectors/kafka/format/CSVRecordDeserializer.h"
#include "velox/connectors/kafka/format/JSONRecordDeserializer.h"
#include "velox/connectors/kafka/format/RawRecordDeserializer.h"
#include "velox/connectors/kafka/format/StreamJSONRecordDeserializer.h"
#include "velox/type/StringView.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::kafka {

KafkaDataSource::KafkaDataSource(
    const RowTypePtr& outputType,
    const TableHandlePtr& tableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    const ConnectionConfigPtr& config)
    : queryCtx_(connectorQueryCtx),
      config_(config),
      outputType_(outputType),
      accumulateBatchEnabled_(config_->getEnableAccumulateDataBatch()),
      consumeBatchSize_(config_->getPollMaxBatchSize()) {
  const std::shared_ptr<KafkaTableHandle> kafkaTableHandle =
      std::dynamic_pointer_cast<KafkaTableHandle>(tableHandle);
  if (kafkaTableHandle) {
    const std::unordered_map<std::string, std::string>& tableParams =
        kafkaTableHandle->tableParameters();
    config_ = config_->setConfigs<ConnectionConfig>(tableParams);
  } else {
    VELOX_FAIL(
        "The table handle {} is not supported for kafka data source.",
        tableHandle->connectorId());
  }
  if (consumerCanbeCreated()) {
    cppkafka::Configuration cppKafkaConfig =
        config_->getCppKafkaConfiguration();
    createConsumer(cppKafkaConfig);
  }
  createCachedQueue(consumeBatchSize_);
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
      cppKafkaConsumer, config_->getPollTimeoutMills(), consumeBatchSize_);
  std::string topic = config_->getTopic();
  topics_.emplace_back(topic);
  consumer_->subscribe(topics_);
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
  KafkaConnectorSplit* kafkaConnectorSplit =
      static_cast<KafkaConnectorSplit*>(split.get());
  VELOX_CHECK_NOT_NULL(
      kafkaConnectorSplit,
      "Failed to add split, because the kafka connector split is null.");
  VELOX_CHECK_NOT_NULL(
      consumer_.get(),
      "Failed to add split, because the kafka consumer is null.");
  cppkafka::TopicPartitionList topicPartitions =
      kafkaConnectorSplit->getCppKafkaTopicPartitions();
  if (topicPartitions.size() == 0) {
    const auto tps =
        consumer_->getTopicPartitions(topics_[0], config_->getStartupMode());
    consumer_->assign(tps);
  } else {
    consumer_->setTopicPartitionsOffset(topicPartitions, config_->getStartupMode());
    consumer_->assign(topicPartitions);
  }
}

std::optional<RowVectorPtr> KafkaDataSource::next(
    uint64_t,
    velox::ContinueFuture&) {
  std::optional<RowVectorPtr> res;
  size_t consumedMsgBytes = 0;
  if (accumulateBatchEnabled_) {
    consumer_->consumeBatch(queue_, consumedMsgBytes);
    if (queue_.empty()) {
      return res;
    } else {
      completedRows_ += queue_.size();
      completedBytes_ += consumedMsgBytes;
      outRow_->prepareForReuse();
      outRow_->resize(queue_.size());
      for (consumePos_ = 0; consumePos_ < queue_.size(); ++consumePos_) {
        deserializer_->deserialize(queue_[consumePos_], consumePos_, outRow_);
      }
      res.emplace(std::dynamic_pointer_cast<RowVector>(outRow_));
    }
    consumePos_  = 0;
    queue_.clear();
  } else {
    if (consumePos_ == queue_.size()) {
      queue_.clear();
      consumer_->consumeBatch(queue_, consumedMsgBytes);
      consumePos_ = 0;
    } else {
      outRow_->prepareForReuse();
      deserializer_->deserialize(queue_[consumePos_], 0, outRow_);
      completedRows_ += 1;
      completedBytes_ += queue_[consumePos_].size();
      res.emplace(std::dynamic_pointer_cast<RowVector>(outRow_));
      consumePos_++;
    }
  }
  return res;
}

std::unordered_map<std::string, RuntimeCounter> KafkaDataSource::runtimeStats() {
  std::unordered_map<std::string, RuntimeCounter> stats;
  return stats;
}
} // namespace facebook::velox::connector::kafka
