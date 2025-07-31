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

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/kafka/KafkaConfig.h"
#include "velox/connectors/kafka/KafkaConnectorSplit.h"
#include "velox/connectors/kafka/KafkaConsumer.h"
#include "velox/connectors/kafka/KafkaRecordDeserializer.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"
#include <cppkafka/cppkafka.h>

namespace facebook::velox::connector::kafka {

using TableHandlePtr = std::shared_ptr<connector::ConnectorTableHandle>;
using ConnectorSplitPtr = std::shared_ptr<ConnectorSplit>;

class KafkaDataSource : public DataSource {
 public:
  KafkaDataSource(
      const RowTypePtr& outputType,
      const TableHandlePtr& tableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      const ConnectionConfigPtr& connectionConfig);

  /// Create a kafka connection to the given topics and partitions.
  void addSplit(ConnectorSplitPtr split) override;

  /// Fetch record from the consumed records.
  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {}

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  /// For test.
  const KafkaConsumerPtr & getConsumer() const {
    return consumer_;
  }

  /// For test.
  const KafkaRecordDeserializerPtr & getDeserializer() const {
    return deserializer_;
  }

 private:
  /// The context of kafka connector for query.
  const ConnectorQueryCtx* queryCtx_;
  /// The connection config for kafka.
  ConnectionConfigPtr config_;
  /// The type of output.
  RowTypePtr outputType_;
  /// The kafka topics to be consumed.
  std::vector<std::string> topics_;
  /// The kafka consumer.
  KafkaConsumerPtr consumer_;
  /// The kafka record deserializer.
  KafkaRecordDeserializerPtr deserializer_;
  /// Count how many rows consumed.
  uint64_t completedRows_ = 0;
  /// Count how many bytes consumed.
  uint64_t completedBytes_ = 0;
  /// The empty row to be returned if nothing consumed.
  RowVectorPtr emptyRow_;
  /// The output row to be returned.
  VectorPtr outRow_;
  /// Whether to accmulate batch when deserialize single row from kafka consumed.
  /// If `true`, the consumed data would be deserialized into a batch in one go, and
  /// return a row vector with batch rows. If `false`, the consumed data would
  /// be deserilized one by one, and return a row vector with a single row.
  bool accumulateBatchEnabled_;
  /// The batch size of data are consumed at once .
  uint64_t consumeBatchSize_;
  /// The cache queue for storing consumed data.
  std::vector<std::string> queue_;
  /// The consumed position of the cache queue when handle the consumed data one
  /// by one.
  size_t consumePos_ = 0;

  /// Whether consumer can be created.
  bool consumerCanbeCreated();

  /// Create kafka consumer from the configuration.
  void createConsumer(cppkafka::Configuration& config);

  /// Create message queue with given size.
  void createCachedQueue(const uint32_t size);

  /// Create deserializer to deserialize the consumed recored to the given row
  /// type.
  void createRecordDeserializer(
      const std::string& format,
      const RowTypePtr& outputType);
};

} // namespace facebook::velox::connector::kafka
