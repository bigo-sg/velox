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
#include "velox/type/Type.h"

namespace facebook::velox::connector::print {

using ConnectorInsertTableHandlePtr =
    std::shared_ptr<const ConnectorInsertTableHandle>;

class PrintTableHandle : public ConnectorInsertTableHandle {
 public:
  PrintTableHandle(
      std::string tableName,
      const RowTypePtr& dataColumns,
      const std::string& printIdentifier,
      bool isStdErr);

  const std::string& tableName() const {
    return tableName_;
  }

  const RowTypePtr& dataColumns() const {
    return dataColumns_;
  }

  const std::string& printIdentifier() const {
    return printIdentifier_;
  }

  bool isStdErr() const {
    return isStdErr_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ConnectorInsertTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  const std::string tableName_;
  const RowTypePtr dataColumns_;
  const std::string printIdentifier_;
  const bool isStdErr_;
};

} // namespace facebook::velox::connector::print
