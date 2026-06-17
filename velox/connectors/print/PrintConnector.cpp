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

#include "velox/connectors/print/PrintConnector.h"
#include "velox/connectors/print/PrintSink.h"
#include "velox/connectors/print/PrintTableHandle.h"

namespace facebook::velox::connector::print {

std::unique_ptr<DataSink> PrintConnector::createDataSink(
    RowTypePtr inputType,
    std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy /** commitStrategy */) {
  std::shared_ptr<PrintTableHandle> printTableHandle =
      std::dynamic_pointer_cast<PrintTableHandle>(connectorInsertTableHandle);
  return std::make_unique<PrintSink>(
      inputType,
      printTableHandle->printIdentifier(),
      printTableHandle->isStdErr(),
      connectorQueryCtx);
}

} // namespace facebook::velox::connector::print
