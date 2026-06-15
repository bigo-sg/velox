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

#include "velox/connectors/pulsar/PulsarConnector.h"
#include "velox/connectors/pulsar/PulsarConnectorSplit.h"
#include "velox/connectors/pulsar/PulsarDataSource.h"
#include "velox/connectors/pulsar/PulsarTableHandle.h"
#include <mutex>

namespace facebook::velox::connector::pulsar {

namespace {

void registerPulsarSerDe() {
  static std::once_flag registerOnce;
  std::call_once(registerOnce, []() {
    PulsarConnectorSplit::registerSerDe();
    PulsarTableHandle::registerSerDe();
  });
}

} // namespace

PulsarConnectorFactory::PulsarConnectorFactory()
    : ConnectorFactory(kPulsarConnectorName) {
  registerPulsarSerDe();
}

PulsarConnectorFactory::PulsarConnectorFactory(const char* connectorName)
    : ConnectorFactory(connectorName) {
  registerPulsarSerDe();
}

std::unique_ptr<DataSource> PulsarConnector::createDataSource(
    const RowTypePtr& outputType,
    const ConnectorHandlePtr& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& /* columnHandles */,
    ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<PulsarDataSource>(
      outputType, tableHandle, connectorQueryCtx, config_);
}

std::unique_ptr<DataSink> PulsarConnector::createDataSink(
    RowTypePtr inputType,
    ConnectorInsertTableHandlePtr connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy) {
  VELOX_NYI();
}

} // namespace facebook::velox::connector::pulsar
