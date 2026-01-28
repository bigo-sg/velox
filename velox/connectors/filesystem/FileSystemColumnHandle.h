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
#include "velox/connectors/hive/TableHandle.h"
#include "velox/type/Subfield.h"

namespace facebook::velox::connector::filesystem {
class FileSystemColumnHandle : public hive::HiveColumnHandle {
 public:
  FileSystemColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType,
      std::vector<common::Subfield> requiredSubfields = {})
      : hive::HiveColumnHandle(name, columnType, dataType, dataType, {}, {}) {}

  std::string toString() const;

  folly::dynamic serialize() const override;

  static ColumnHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();
};
} // namespace facebook::velox::connector::filesystem