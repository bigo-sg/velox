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
#include "velox/common/config/Config.h"
#include "velox/connectors/filesystem/FileSystemConnector.h"
#include "velox/connectors/filesystem/FileSystemIndexSource.h"
#include "velox/connectors/filesystem/FileSystemIndexTableHandle.h"
#include "velox/connectors/filesystem/FileSystemInsertTableHandle.h"
#include "velox/connectors/filesystem/FileSystemDataSink.h"
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace facebook::velox::connector::filesystem {

std::unique_ptr<DataSource> FileSystemConnector::createDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
        columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  VELOX_NYI();
}

std::unique_ptr<DataSink> FileSystemConnector::createDataSink(
    RowTypePtr inputType,
    std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy /** commitStrategy */) {
  std::shared_ptr<FileSystemWriteConfig> writeConfig =
      std::make_shared<FileSystemWriteConfig>(config_);
  std::shared_ptr<FileSystemInsertTableHandle> insertTableHandle =
      std::dynamic_pointer_cast<FileSystemInsertTableHandle>(
          connectorInsertTableHandle);
  const std::unordered_map<std::string, std::string>& tableParams =
      insertTableHandle->tableParameters();
  std::shared_ptr<FileSystemWriteConfig> newWriteConfig =
      writeConfig->updateAndGetAllConfigs<FileSystemWriteConfig>(tableParams);
  return std::make_unique<FileSystemDataSink>(
      inputType,
      insertTableHandle,
      connectorQueryCtx,
      newWriteConfig,
      insertTableHandle->parititonIndexes(),
      insertTableHandle->partitionKeys());
}

core::TypedExprPtr toLookupJoinConditionExpr(
    const std::vector<std::shared_ptr<core::IndexLookupCondition>>&
        joinConditions,
    const std::shared_ptr<FileSystemIndexTableHandle>& tableHandle,
    const RowTypePtr& inputType) {
  if (joinConditions.empty()) {
    return nullptr;
  }
  const auto& keyType = tableHandle->keyType();
  std::vector<core::TypedExprPtr> conditionExprs;
  conditionExprs.reserve(joinConditions.size());
  for (const auto& condition : joinConditions) {
    auto indexColumnExpr = std::make_shared<core::FieldAccessTypedExpr>(
        keyType->findChild(condition->key->name()), condition->key->name());
    if (auto inCondition =
            std::dynamic_pointer_cast<core::InIndexLookupCondition>(
                condition)) {
      conditionExprs.push_back(std::make_shared<const core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              inCondition->list, std::move(indexColumnExpr)},
          "contains"));
      continue;
    }
    if (auto betweenCondition =
            std::dynamic_pointer_cast<core::BetweenIndexLookupCondition>(
                condition)) {
      conditionExprs.push_back(std::make_shared<const core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              std::move(indexColumnExpr),
              betweenCondition->lower,
              betweenCondition->upper},
          "between"));
      continue;
    }
    VELOX_FAIL("Invalid index join condition: {}", condition->toString());
  }
  return std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), conditionExprs, "and");
}

std::shared_ptr<IndexSource> FileSystemConnector::createIndexSource(
    const RowTypePtr& inputType,
    size_t numJoinKeys,
    const std::vector<std::shared_ptr<core::IndexLookupCondition>>&
        joinConditions,
    const RowTypePtr& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  const std::shared_ptr<FileSystemIndexTableHandle> fsTableHandle =
      std::dynamic_pointer_cast<FileSystemIndexTableHandle>(tableHandle);

  std::shared_ptr<folly::Executor> executor;
  if (fsTableHandle->asyncLookup()) {
    executor = std::make_shared<folly::CPUThreadPoolExecutor>(1);
  }
  return std::make_shared<FileSystemIndexSource>(
      inputType,
      outputType,
      numJoinKeys,
      toLookupJoinConditionExpr(joinConditions, fsTableHandle, inputType),
      fsTableHandle,
      connectorQueryCtx,
      executor);
}

} // namespace facebook::velox::connector::filesystem