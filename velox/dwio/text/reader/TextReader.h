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

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/Mutation.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::text {

class RowReaderOptions : public dwio::common::ReaderOptions,
                         public dwio::common::RowReaderOptions {
 public:
  RowReaderOptions(
      memory::MemoryPool* pool,
      const char* fieldDelimiter,
      const uint64_t maxReadRows,
      const uint64_t maxReadSize)
      : ReaderOptions(pool),
        fieldDemiliter_(fieldDelimiter),
        maxReadRows_(maxReadRows),
        maxReadSize_(maxReadSize) {}

  const size_t maxReadRows() const {
    return maxReadRows_;
  }

  const char* fieldDemiliter() const {
    return fieldDemiliter_;
  }

  const uint64_t maxReadSize() const {
    return maxReadSize_;
  }

 private:
  const char* fieldDemiliter_;
  const uint64_t maxReadRows_;
  const uint64_t maxReadSize_;
};

class TextRowReader : public dwio::common::RowReader {
 public:
  TextRowReader(
      const RowTypePtr& schema,
      const std::shared_ptr<dwio::common::ReadFileInputStream>& fileInput,
      const RowReaderOptions& options);

  uint64_t next(
      uint64_t maxRowsToRead,
      velox::VectorPtr& result,
      const dwio::common::Mutation* mutation = nullptr) override;

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override {}

  void resetFilterCaches() override {}

  std::optional<size_t> estimatedRowSize() const override;

 private:
  const RowTypePtr schema_;
  const RowReaderOptions options_;
  const std::shared_ptr<dwio::common::ReadFileInputStream> fileInput_;
  const size_t fileSize_;
  const char* lineDelimiter_ = "\n";
  mutable int64_t readRows_ = 0;
  mutable int64_t totalReadRows_ = 0;
  mutable int64_t totalReadBytes_ = 0;
  mutable bool readFinished_ = false;

  const void deserialize(
      VectorPtr& field,
      const TypePtr& type,
      const size_t index,
      const std::string& s);
};

class TextReader : public dwio::common::Reader {
 public:
  TextReader(
      std::unique_ptr<dwio::common::BufferedInput> buffer,
      const dwio::common::ReaderOptions& options)
      : buffer_(std::move(buffer)),
        options_(options),
        typeId_(dwio::common::TypeWithId::create(options_.fileSchema())) {
    VELOX_CHECK(options_.fileFormat() == dwio::common::FileFormat::TEXT);
  }

  std::optional<uint64_t> numberOfRows() const override {
    std::optional<uint64_t> rows;
    return rows;
  }

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override {
    return nullptr;
  }

  const RowTypePtr& rowType() const override {
    return options_.fileSchema();
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override {
    return typeId_;
  }

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

 private:
  const std::unique_ptr<dwio::common::BufferedInput> buffer_;
  const dwio::common::ReaderOptions options_;
  const std::shared_ptr<const dwio::common::TypeWithId> typeId_;
};

class TextReaderFactory : public dwio::common::ReaderFactory {
 public:
  TextReaderFactory() : ReaderFactory(dwio::common::FileFormat::TEXT) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> buffer,
      const dwio::common::ReaderOptions& options) override;
};
} // namespace facebook::velox::text
