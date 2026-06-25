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

#include <folly/dynamic.h>
#include "velox/connectors/Connector.h"
#include "velox/core/ITypedExpr.h"

namespace facebook::velox::connector::pulsar {

class PulsarTableHandle : public ConnectorTableHandle {
 public:
  PulsarTableHandle(
      std::string connectorId,
      std::string tableName,
      const RowTypePtr& dataColumns = nullptr,
      const std::unordered_map<std::string, std::string>& tableParameters = {})
      : ConnectorTableHandle(std::move(connectorId)),
        tableName_(std::move(tableName)),
        dataColumns_(dataColumns),
        tableParameters_(tableParameters) {}

  const std::string& tableName() const {
    return tableName_;
  }

  const RowTypePtr& dataColumns() const {
    return dataColumns_;
  }

  const std::unordered_map<std::string, std::string>& tableParameters() const {
    return tableParameters_;
  }

  const std::string& name() const override {
    return tableName_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  std::string tableName_;
  RowTypePtr dataColumns_;
  std::unordered_map<std::string, std::string> tableParameters_;
};

} // namespace facebook::velox::connector::pulsar
