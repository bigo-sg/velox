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

#include "velox/connectors/Connector.h"
#include "velox/connectors/utils/StringFormatter.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include <cmath>

namespace facebook::velox::connector::print {

// DataSink that writes rows to stdout or stderr.
class PrintSink : public DataSink {
 public:
  PrintSink(
      const RowTypePtr& inputType,
      const std::string& printIdentifier,
      bool isStdErr,
      const ConnectorQueryCtx* queryCtx);

  void appendData(RowVectorPtr input) override;

  bool finish() override;

  std::vector<std::string> close() override;

  void abort() override;

  connector::DataSink::Stats stats() const override;

  // Computes the output prefix from the print-identifier, parallelism and
  // task index. Mirrors Flink's PrintSinkOutputWriter.open() prefix logic.
  static std::string computePrefix(
      const std::string& printIdentifier,
      int parallelism,
      int taskIndex);

 private:

  const RowTypePtr inputType_;
  const ConnectorQueryCtx* queryCtx_;
  const std::shared_ptr<StringFormatter> formatter_;
  const std::string prefix_;
  const bool isStdErr_;
  bool finished = false;
};

} // namespace facebook::velox::connector::print
