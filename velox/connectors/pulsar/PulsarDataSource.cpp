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

#include "velox/connectors/pulsar/PulsarDataSource.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/connectors/kafka/format/CSVRecordDeserializer.h"
#include "velox/connectors/kafka/format/RawRecordDeserializer.h"
#include "velox/connectors/kafka/format/StreamJSONRecordDeserializer.h"
#include "velox/connectors/pulsar/PulsarConnectorSplit.h"
#include "velox/connectors/pulsar/PulsarTableHandle.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::connector::pulsar {

PulsarDataSource::PulsarDataSource(
    const RowTypePtr& outputType,
    const TableHandlePtr& tableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    const ConnectionConfigPtr& config)
    : queryCtx_(connectorQueryCtx),
      config_(config),
      outputType_(outputType),
      batchSize_(config_->getDataBatchSize()) {
  VELOX_CHECK(batchSize_ > 0, "Batch size config value must greater than 0.");
  const std::shared_ptr<PulsarTableHandle> pulsarTableHandle =
      std::dynamic_pointer_cast<PulsarTableHandle>(tableHandle);
  if (pulsarTableHandle) {
    config_ = config_->updateAndGetAllConfigs<ConnectionConfig>(
        pulsarTableHandle->tableParameters());
  } else {
    VELOX_FAIL(
        "The table handle {} is not supported for pulsar data source.",
        tableHandle->connectorId());
  }
  if (consumerCanbeCreated()) {
    createConsumer();
  }
  createCachedQueue(batchSize_);
  createRecordDeserializer(config_->getFormat(), outputType_);
}

bool PulsarDataSource::consumerCanbeCreated() const {
  return config_->exists(ConnectionConfig::kServiceUrl) &&
      config_->exists(ConnectionConfig::kTopic) &&
      config_->exists(ConnectionConfig::kSubscriptionName) &&
      config_->exists(ConnectionConfig::kFormat) && !consumer_.get();
}

void PulsarDataSource::createConsumer() {
  VELOX_CHECK_NULL(
      consumer_.get(),
      "Failed to create pulsar consumer as the consumer is not null");
  consumer_ = std::make_shared<PulsarConsumer>(
      config_, config_->getReceiveTimeoutMills(), batchSize_);
}

void PulsarDataSource::createCachedQueue(uint32_t size) {
  VELOX_CHECK_GT(
      size, 0, "Pulsar consume message queue size must greater than 0");
  queue_.reserve(size);
}

void PulsarDataSource::createRecordDeserializer(
    const std::string& format,
    const RowTypePtr& outputType) {
  if (format == "json") {
    deserializer_ = std::make_shared<kafka::KafkaStreamJSONRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else if (format == "csv") {
    deserializer_ = std::make_shared<kafka::KafkaCSVRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else if (format == "raw") {
    deserializer_ = std::make_shared<kafka::KafkaRawRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else {
    VELOX_FAIL_UNSUPPORTED_INPUT_UNCATCHABLE(
        "The data format {} is not supported for pulsar.", format);
  }
  outRow_ = RowVector::createEmpty(outputType_, queryCtx_->memoryPool());
  outRow_->resize(1);
}

void PulsarDataSource::addSplit(ConnectorSplitPtr split) {
  PulsarConnectorSplit* pulsarConnectorSplit =
      static_cast<PulsarConnectorSplit*>(split.get());
  VELOX_CHECK_NOT_NULL(
      pulsarConnectorSplit,
      "Failed to add split, because the pulsar connector split is null.");
  VELOX_CHECK_EQ(
      pulsarConnectorSplit->serviceUrl_,
      config_->getServiceUrl(),
      "Pulsar split service url differs from data source config.");
  VELOX_CHECK_EQ(
      pulsarConnectorSplit->topic_,
      config_->getTopic(),
      "Pulsar split topic differs from data source config.");
}

std::optional<RowVectorPtr> PulsarDataSource::next(
    uint64_t,
    velox::ContinueFuture&) {
  std::optional<RowVectorPtr> res;
  size_t consumedMsgBytes = 0;
  if (queue_.empty()) {
    VELOX_CHECK_NOT_NULL(
        consumer_.get(),
        "Failed to consume pulsar messages as the consumer is null.");
    consumer_->consumeBatch(
        queue_, consumedMsgBytes, config_->getAcknowledgeMessages());
    consumePos_ = 0;
    if (consumedMsgBytes == 0) {
      return res;
    }
  }

  outRow_->prepareForReuse();
  size_t processDataSize = batchSize_ > 1 ? queue_.size() : batchSize_;
  outRow_->resize(processDataSize);
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
PulsarDataSource::runtimeStats() {
  std::unordered_map<std::string, RuntimeCounter> stats;
  return stats;
}

} // namespace facebook::velox::connector::pulsar
