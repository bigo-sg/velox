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

#include <folly/experimental/FunctionScheduler.h>
#include <atomic>
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/kafka/format/KafkaRecordDeserializer.h"
#include "velox/connectors/pulsar/PulsarConfig.h"
#include "velox/connectors/pulsar/PulsarConsumer.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::pulsar {

using TableHandlePtr = std::shared_ptr<connector::ConnectorTableHandle>;
using ConnectorSplitPtr = std::shared_ptr<ConnectorSplit>;

class PulsarDataSource : public DataSource {
 public:
  PulsarDataSource(
      const RowTypePtr& outputType,
      const TableHandlePtr& tableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      const ConnectionConfigPtr& connectionConfig);

  ~PulsarDataSource() override;

  void addSplit(ConnectorSplitPtr split) override;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  void cancel() override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {}

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::vector<std::string> checkpointState() override;

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  const PulsarConsumerPtr& getConsumer() const {
    return consumer_;
  }

  const kafka::KafkaRecordDeserializerPtr& getDeserializer() const {
    return deserializer_;
  }

 private:
  const ConnectorQueryCtx* queryCtx_;
  ConnectionConfigPtr baseConfig_;
  ConnectionConfigPtr config_;
  RowTypePtr outputType_;
  std::string connectorId_;
  PulsarConsumerPtr consumer_;
  kafka::KafkaRecordDeserializerPtr deserializer_;
  folly::FunctionScheduler scheduler_;
  std::optional<ContinuePromise> blockingPromise_;
  uint64_t blockingSequence_{0};
  std::atomic_bool canceled_{false};
  uint64_t completedRows_ = 0;
  uint64_t completedBytes_ = 0;
  VectorPtr outRow_;
  uint64_t batchSize_;
  std::vector<PulsarMessage> queue_;
  size_t consumePos_ = 0;
  uint64_t receivedMessages_ = 0;
  uint64_t receivedBytes_ = 0;
  uint64_t receiveTimeouts_ = 0;
  uint64_t acknowledgedMessages_ = 0;
  uint64_t negativelyAcknowledgedMessages_ = 0;
  uint64_t deserializeFailures_ = 0;
  uint64_t skippedMessagesAfterEnd_ = 0;
  std::string checkpointStartMessageId_;

  bool consumerCanbeCreated() const;
  void createConsumer();
  void resetSplitState();
  bool cumulativeAck() const;
  void completeBlockingFuture();
  std::optional<RowVectorPtr> blockOnReceiveTimeout(
      velox::ContinueFuture& future);
  void refreshConsumerStats();
  void createCachedQueue(uint32_t size);
  void createRecordDeserializer(
      const std::string& format,
      const RowTypePtr& outputType);
};

} // namespace facebook::velox::connector::pulsar
