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

#include "velox/exec/HashTable.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::filesystem {

struct FileSystemIndexTable {
  RowTypePtr keyType;
  RowTypePtr dataType;
  std::shared_ptr<exec::BaseHashTable> table;

  FileSystemIndexTable(
      RowTypePtr _keyType,
      RowTypePtr _dataType,
      std::shared_ptr<exec::BaseHashTable> _table)
      : keyType(std::move(_keyType)),
        dataType(std::move(_dataType)),
        table(std::move(_table)) {}
};

} // namespace facebook::velox::connector::filesystem
