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
#include "velox/experimental/stateful/StringFormatter.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/connectors/Connector.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::from_elements {

class FromElementsSource : public DataSource {
 public:
  FromElementsSource(
    const RowTypePtr& outputType,
    const ConnectorQueryCtx* queryCtx,
    const std::vector<std::string>& s);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

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

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    std::unordered_map<std::string, RuntimeCounter> stats;
    return stats;
  }

 private:
  const RowTypePtr outputType_;
  const ConnectorQueryCtx* queryCtx_;
  const stateful::FormatterPtr formatter_;
  RowVectorPtr data_;
  mutable uint64_t completedRows_ = 0;
  mutable uint64_t completedBytes_ = 0;
};

} // namespace facebook::velox::connector::from_elements