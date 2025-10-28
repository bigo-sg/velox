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
#include "velox/connectors/print/PrintSink.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"
#include <fmt/format.h>
#include <memory>

namespace facebook::velox::connector::print {

PrintSink::PrintSink(
    const RowTypePtr& inputType,
    const std::string& path,
    const ConnectorQueryCtx* queryCtx)
    : inputType_(inputType),
      outputType_(createOutputType()),
      queryCtx_(queryCtx),
      writer_(createWriter(path)),
      formatter_(createFormatter(inputType_, tz::locateZone(queryCtx->sessionTimezone()))) {}

std::unique_ptr<dwio::common::Writer> PrintSink::createWriter(
    const std::string& path) {
  std::unordered_map<std::string, std::string> rawConfigs;
  auto fs = filesystems::getFileSystem(
      path, std::make_shared<const config::ConfigBase>(std::move(rawConfigs)));
  if (fs->exists(path)) {
    fs->remove(path);
  }
  std::unique_ptr<dwio::common::FileSink> writeFileSink =
      dwio::common::FileSink::create(
          path,
          {
              .bufferWrite = false,
              .pool = queryCtx_->memoryPool(),
              .metricLogger = dwio::common::MetricsLog::voidLog(),
              .stats = &ioStats_,
          });
  auto writerFactory =
      dwio::common::getWriterFactory(dwio::common::FileFormat::TEXT);
  std::shared_ptr<dwio::common::WriterOptions> options =
      writerFactory->createWriterOptions();
  if (options->schema == nullptr) {
    options->schema = outputType_;
  }
  if (options->memoryPool == nullptr) {
    options->memoryPool = queryCtx_->connectorMemoryPool();
  }
  return writerFactory->createWriter(std::move(writeFileSink), options);
}

const RowTypePtr PrintSink::createOutputType() {
  std::vector<std::string> fieldNames;
  std::vector<TypePtr> fieldTypes;
  fieldNames.emplace_back("result");
  fieldTypes.emplace_back(std::make_shared<const VarcharType>());
  return std::make_shared<const RowType>(
      std::move(fieldNames), std::move(fieldTypes));
}

const RowVectorPtr PrintSink::formatToSingleStringVector(
    const RowVectorPtr& input) {
  VELOX_CHECK_EQ(input->childrenSize(), inputType_->children().size());
  auto output =
      RowVector::create(outputType_, input->size(), queryCtx_->memoryPool());
  RowVectorPtr rowVector = std::dynamic_pointer_cast<RowVector>(output);
  VELOX_CHECK(rowVector != nullptr);
  VELOX_CHECK_EQ(rowVector->childrenSize(), 1);
  auto outputField =
      std::dynamic_pointer_cast<FlatVector<StringView>>(rowVector->childAt(0));
  VELOX_CHECK(outputField != nullptr);
  const std::vector<VectorPtr> inputFields = input->children();
  for (size_t i = 0; i < input->size(); ++i) {
    std::stringstream ss;
    formatter_->toString(input, inputType_, i, ss);
    const std::string sValue = ss.str();
    outputField->set(i, StringView(sValue.data(), sValue.size()));
  }
  return rowVector;
}

void PrintSink::appendData(RowVectorPtr input) {
  VELOX_CHECK(writer_ != nullptr);
  writer_->write(formatToSingleStringVector(input));
  writer_->flush();
}

std::vector<std::string> PrintSink::close() {
  std::vector<std::string> res;
  VELOX_CHECK(writer_ != nullptr);
  writer_->close();
  finished = true;
  return res;
}

bool PrintSink::finish() {
  finished = true;
  return true;
}

void PrintSink::abort() {}

connector::DataSink::Stats PrintSink::stats() const {
  connector::DataSink::Stats stats;
  return stats;
}

} // namespace facebook::velox::connector::print