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
#include "velox/dwio/common/Writer.h"
#include "velox/connectors/utils/StringFormatter.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include <cmath>

namespace facebook::velox::connector::print {

class PrintSink : public DataSink {
 public:
  PrintSink(
      const RowTypePtr& inputType,
      const std::string& path,
      const ConnectorQueryCtx* queryCtx);

  void appendData(RowVectorPtr input) override;

  bool finish() override;

  std::vector<std::string> close() override;

  void abort() override;

  connector::DataSink::Stats stats() const override;

  io::IoStatistics ioStats_;

 private:
  const RowTypePtr inputType_;
  const RowTypePtr outputType_;
  const ConnectorQueryCtx* queryCtx_;
  const std::unique_ptr<dwio::common::Writer> writer_;
  const std::shared_ptr<StringFormatter> formatter_;
  bool finished = true;

  std::unique_ptr<dwio::common::Writer> createWriter(const std::string& path);
  const RowTypePtr createOutputType();
  /// Format the input fields' data to a single string of flink-style. e.g. Row(1,2,3) -> +I[1, 2, 3],
  /// Row(1,Array(2,3)) -> +I[1, [2, 3]], Row(1,Map(2=3, 3=4)) -> +I[1, {2=3, 3=4}].
  const RowVectorPtr formatToSingleStringVector(const RowVectorPtr& input);
  const std::shared_ptr<StringFormatter> createStringFormatter(
      const TypePtr& type);
};

} // namespace facebook::velox::connector::print