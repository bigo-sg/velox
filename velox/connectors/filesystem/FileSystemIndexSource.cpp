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

#include "velox/connectors/filesystem/FileSystemIndexSource.h"
#include <connectors/Connector.h>
#include <vector/ComplexVector.h>
#include <vector/TypeAliases.h>
#include "velox/common/memory/RawVector.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/text/reader/TextReader.h"
#include "velox/exec/IndexLookupJoin.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/VectorHasher.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::connector::filesystem {

FileSystemIndexSource::FileSystemIndexSource(
    const RowTypePtr& inputType,
    const RowTypePtr& outputType,
    size_t numEqualJoinKeys,
    const core::TypedExprPtr& joinConditionExpr,
    const std::shared_ptr<FileSystemIndexTableHandle>& tableHandle,
    connector::ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<folly::Executor>& executor)
    : tableHandle_(tableHandle),
      config_(std::make_shared<FileSystemReadConfig>(
          std::make_shared<const config::ConfigBase>(
              std::move(tableHandle->tableParameters())))),
      inputType_(inputType),
      outputType_(outputType),
      keyType_(tableHandle_->keyType()),
      valueType_(tableHandle_->valueType()),
      connectorQueryCtx_(connectorQueryCtx),
      numEqualJoinKeys_(numEqualJoinKeys),
      conditionExprSet_(
          joinConditionExpr != nullptr
              ? connectorQueryCtx_->expressionEvaluator()->compile(
                    joinConditionExpr)
              : nullptr),
      pool_(connectorQueryCtx_->memoryPool()->shared_from_this()),
      executor_(executor) {
  VELOX_CHECK_LE(outputType_->size(), valueType_->size() + keyType_->size());
  VELOX_CHECK_LE(numEqualJoinKeys_, keyType_->size());
  for (int i = 0; i < numEqualJoinKeys_; ++i) {
    VELOX_CHECK(
        keyType_->childAt(i)->equivalent(*inputType_->childAt(i)),
        "{} vs {}",
        keyType_->toString(),
        inputType_->toString());
  }
  initOutputProjections();
  initConditionProjections();
  initLookupTable();
}

void FileSystemIndexSource::initOutputProjections() {
  VELOX_CHECK(lookupOutputProjections_.empty());
  lookupOutputProjections_.reserve(outputType_->size());
  for (auto outputChannel = 0; outputChannel < outputType_->size();
       ++outputChannel) {
    const auto outputName = outputType_->nameOf(outputChannel);
    if (valueType_->containsChild(outputName)) {
      const auto tableValueChannel = valueType_->getChildIdx(outputName);
      // The hash table layout is: index columns, value columns.
      lookupOutputProjections_.emplace_back(
          keyType_->size() + tableValueChannel, outputChannel);
      continue;
    }
    const auto tableKeyChannel = keyType_->getChildIdx(outputName);
    lookupOutputProjections_.emplace_back(tableKeyChannel, outputChannel);
  }
  VELOX_CHECK_EQ(lookupOutputProjections_.size(), outputType_->size());
}

void FileSystemIndexSource::initConditionProjections() {
  if (conditionExprSet_ == nullptr) {
    return;
  }
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  column_index_t outputChannel{0};
  for (const auto& field : conditionExprSet_->distinctFields()) {
    names.push_back(field->name());
    types.push_back(field->type());
    if (inputType_->getChildIdxIfExists(field->name()).has_value()) {
      conditionInputProjections_.emplace_back(
          inputType_->getChildIdx(field->name()), outputChannel++);
      continue;
    }
    conditionTableProjections_.emplace_back(
        keyType_->getChildIdx(field->name()), outputChannel++);
  }
  conditionInputType_ = ROW(std::move(names), std::move(types));
}

std::shared_ptr<connector::IndexSource::LookupResultIterator>
FileSystemIndexSource::lookup(const LookupRequest& request) {
  checkNotFailed();
  VELOX_CHECK(lookupTable_ != nullptr);
  const auto numInputRows = request.input->size();
  auto& hashTable = lookupTable_->table;
  auto lookup =
      std::make_unique<exec::HashLookup>(hashTable->hashers(), pool_.get());
  SelectivityVector activeRows(numInputRows);
  VELOX_CHECK(activeRows.isAllSelected());
  hashTable->prepareForJoinProbe(
      *lookup, request.input, activeRows, /*decodeAndRemoveNulls=*/true);
  lookup->hits.resize(numInputRows);
  std::fill(lookup->hits.data(), lookup->hits.data() + numInputRows, nullptr);
  if (!lookup->rows.empty()) {
    hashTable->joinProbe(*lookup);
  }
  // Update lookup rows to include all input rows as it might be used by left
  // join.
  auto& rows = lookup->rows;
  rows.resize(request.input->size());
  std::iota(rows.begin(), rows.end(), 0);
  return std::make_shared<ResultIterator>(
      this->shared_from_this(),
      request,
      std::move(lookup),
      tableHandle_->asyncLookup() ? executor_.get() : nullptr);
}

void FileSystemIndexSource::recordCpuTiming(const CpuWallTiming& timing) {
  VELOX_CHECK_EQ(timing.count, 1);
  std::lock_guard<std::mutex> l(mutex_);
  if (timing.wallNanos != 0) {
    exec::addOperatorRuntimeStats(
        exec::IndexLookupJoin::kConnectorLookupWallTime,
        RuntimeCounter(timing.wallNanos, RuntimeCounter::Unit::kNanos),
        runtimeStats_);
    exec::addOperatorRuntimeStats(
        exec::IndexLookupJoin::kClientLookupWaitWallTime,
        RuntimeCounter(timing.wallNanos, RuntimeCounter::Unit::kNanos),
        runtimeStats_);
  }
  if (timing.cpuNanos != 0) {
    exec::addOperatorRuntimeStats(
        exec::IndexLookupJoin::kConnectorResultPrepareTime,
        RuntimeCounter(timing.cpuNanos, RuntimeCounter::Unit::kNanos),
        runtimeStats_);
  }
}

void FileSystemIndexSource::checkNotFailed() {
  if (!error_.empty()) {
    VELOX_FAIL("TestIndexSource failed: {}", error_);
  }
}

std::unordered_map<std::string, RuntimeMetric>
FileSystemIndexSource::runtimeStats() {
  std::lock_guard<std::mutex> l(mutex_);
  return runtimeStats_;
}

const std::shared_ptr<FileSystemIndexTable>
FileSystemIndexSource::createIndexTable(
    int numEqualJoinKeys,
    const RowVectorPtr& keyData,
    const RowVectorPtr& valueData) {
  const auto keyType =
      std::dynamic_pointer_cast<const RowType>(keyData->type());
  VELOX_CHECK_GE(keyType->size(), 1);
  VELOX_CHECK_GE(keyType->size(), numEqualJoinKeys);
  auto valueType = std::dynamic_pointer_cast<const RowType>(valueData->type());
  VELOX_CHECK_GE(valueType->size(), 1);
  const auto numRows = keyData->size();
  VELOX_CHECK_EQ(numRows, valueData->size());

  std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
  hashers.reserve(numEqualJoinKeys);
  std::vector<VectorPtr> keyVectors;
  keyVectors.reserve(numEqualJoinKeys);
  for (auto i = 0; i < numEqualJoinKeys; ++i) {
    hashers.push_back(
        std::make_unique<exec::VectorHasher>(keyType->childAt(i), i));
    keyVectors.push_back(keyData->childAt(i));
  }

  std::vector<TypePtr> dependentTypes;
  std::vector<VectorPtr> dependentVectors;
  for (int i = numEqualJoinKeys; i < keyType->size(); ++i) {
    dependentTypes.push_back(keyType->childAt(i));
    dependentVectors.push_back(keyData->childAt(i));
  }
  for (int i = 0; i < valueType->size(); ++i) {
    dependentTypes.push_back(valueType->childAt(i));
    dependentVectors.push_back(valueData->childAt(i));
  }

  // Create the table.
  auto table = exec::HashTable<false>::createForJoin(
      std::move(hashers),
      /*dependentTypes=*/dependentTypes,
      /*allowDuplicates=*/true,
      /*hasProbedFlag=*/false,
      /*minTableSizeForParallelJoinBuild=*/1,
      pool_.get());

  // Insert data into the row container.
  auto* rowContainer = table->rows();
  std::vector<DecodedVector> decodedVectors;
  for (auto& vector : keyData->children()) {
    decodedVectors.emplace_back(*vector);
  }
  for (auto& vector : valueData->children()) {
    decodedVectors.emplace_back(*vector);
  }

  for (auto row = 0; row < numRows; ++row) {
    auto* newRow = rowContainer->newRow();

    for (auto col = 0; col < decodedVectors.size(); ++col) {
      rowContainer->store(decodedVectors[col], row, newRow, col);
    }
  }

  // Build the table index.
  table->prepareJoinTable(
      {}, exec::BaseHashTable::kNoSpillInputStartPartitionBit);
  return std::make_shared<FileSystemIndexTable>(
      std::move(keyType), std::move(valueType), std::move(table));
}

void FileSystemIndexSource::initLookupTable() {
  VELOX_CHECK(config_ != nullptr);
  auto fs = filesystems::getFileSystem(config_->getPath(), config_->config());
  VELOX_CHECK(fs != nullptr);
  std::shared_ptr<ReadFile> readFile = fs->openFileForRead(config_->getPath());
  VELOX_CHECK(readFile != nullptr);
  std::unique_ptr<dwio::common::BufferedInput> input =
      std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);

  /// current only support text format
  text::RowReaderOptions rowReaderOptions(
      pool_.get(),
      config_->getFieldDelimiter().data(),
      config_->getMaxReadRows(),
      config_->getMaxReadBytes());
  rowReaderOptions.setFileFormat(dwio::common::FileFormat::TEXT);
  rowReaderOptions.setFileSchema(tableHandle_->tableSchema());

  auto readerFactory = dwio::common::getReaderFactory(config_->getFormat());
  auto textReader =
      readerFactory->createReader(std::move(input), rowReaderOptions);
  VELOX_CHECK(textReader != nullptr);
  auto textRowReader = textReader->createRowReader(rowReaderOptions);
  VELOX_CHECK(textRowReader != nullptr);

  RowVectorPtr rows =
      RowVector::createEmpty(tableHandle_->tableSchema(), pool_.get());
  VectorPtr t =
      RowVector::createEmpty(tableHandle_->tableSchema(), pool_.get());
  while (textRowReader->next(config_->getMaxReadRows(), t, nullptr) != 0) {
    RowVectorPtr r = std::dynamic_pointer_cast<RowVector>(t);
    VELOX_CHECK(r->childrenSize() == rows->childrenSize());
    rows->append(r.get());
    t->prepareForReuse();
  }
  if (rows != nullptr) {
    VELOX_CHECK(keyType_ != nullptr);
    VELOX_CHECK(valueType_ != nullptr);
    std::vector<VectorPtr> keys;
    std::vector<VectorPtr> values;
    for (const auto& name : keyType_->names()) {
      keys.emplace_back(rows->childAt(name));
    }
    for (const auto& name : valueType_->names()) {
      values.emplace_back(rows->childAt(name));
    }
    lookupTable_ = createIndexTable(
        numEqualJoinKeys_,
        std::make_shared<RowVector>(
            rows->pool(), keyType_, nullptr, rows->size(), keys),
        std::make_shared<RowVector>(
            rows->pool(), valueType_, nullptr, rows->size(), values));
  }
}

FileSystemIndexSource::ResultIterator::ResultIterator(
    std::shared_ptr<FileSystemIndexSource> source,
    const LookupRequest& request,
    std::unique_ptr<exec::HashLookup> lookupResult,
    folly::Executor* executor)
    : source_(std::move(source)),
      request_(request),
      lookupResult_(std::move(lookupResult)),
      executor_(executor) {
  // Initialize the lookup result iterator.
  lookupResultIter_ = std::make_unique<exec::BaseHashTable::JoinResultIterator>(
      std::vector<vector_size_t>{}, 0, /*estimatedRowSize=*/1);
  lookupResultIter_->reset(*lookupResult_);
}

std::optional<std::unique_ptr<connector::IndexSource::LookupResult>>
FileSystemIndexSource::ResultIterator::next(
    vector_size_t size,
    ContinueFuture& future) {
  source_->checkNotFailed();

  if (hasPendingRequest_.exchange(true)) {
    VELOX_FAIL("Only one pending request is allowed at a time");
  }

  if (executor_ && !asyncResult_.has_value()) {
    asyncLookup(size, future);
    return std::nullopt;
  }

  SCOPE_EXIT {
    hasPendingRequest_ = false;
  };
  if (asyncResult_.has_value()) {
    VELOX_CHECK_NOT_NULL(executor_);
    auto result = std::move(asyncResult_.value());
    asyncResult_.reset();
    return result;
  }
  return syncLookup(size);
}

void extractColumns(
    exec::BaseHashTable* table,
    folly::Range<char* const*> rows,
    folly::Range<const exec::IdentityProjection*> projections,
    memory::MemoryPool* pool,
    const std::vector<TypePtr>& resultTypes,
    std::vector<VectorPtr>& resultVectors) {
  VELOX_CHECK_EQ(resultTypes.size(), resultVectors.size());
  for (auto projection : projections) {
    const auto resultChannel = projection.outputChannel;
    VELOX_CHECK_LT(resultChannel, resultVectors.size());
    auto& child = resultVectors[resultChannel];
    // TODO: Consider reuse of complex types.
    if (!child || !BaseVector::isVectorWritable(child) ||
        !child->isFlatEncoding()) {
      child = BaseVector::create(resultTypes[resultChannel], rows.size(), pool);
    }
    child->resize(rows.size());
    table->extractColumn(rows, projection.inputChannel, child);
  }
}

void FileSystemIndexSource::ResultIterator::extractLookupColumns(
    folly::Range<char* const*> rows,
    RowVectorPtr& result) {
  if (result == nullptr) {
    result = BaseVector::create<RowVector>(
        source_->outputType(), rows.size(), source_->pool());
  } else {
    VectorPtr output = std::move(result);
    BaseVector::prepareForReuse(output, rows.size());
    result = std::static_pointer_cast<RowVector>(output);
  }
  VELOX_CHECK_EQ(result->size(), rows.size());
  extractColumns(
      source_->indexTable()->table.get(),
      rows,
      source_->outputProjections(),
      source_->pool_.get(),
      source_->outputType_->children(),
      lookupOutput_->children());
}

void FileSystemIndexSource::ResultIterator::asyncLookup(
    vector_size_t size,
    ContinueFuture& future) {
  VELOX_CHECK_NOT_NULL(executor_);
  VELOX_CHECK(!asyncResult_.has_value());
  VELOX_CHECK(hasPendingRequest_);
  auto [lookupPromise, lookupFuture] =
      makeVeloxContinuePromiseContract("ResultIterator::asyncLookup");
  future = std::move(lookupFuture);
  auto asyncPromise =
      std::make_shared<ContinuePromise>(std::move(lookupPromise));
  executor_->add([this, size, promise = std::move(asyncPromise)]() mutable {
    VELOX_CHECK(!asyncResult_.has_value());
    VELOX_CHECK(hasPendingRequest_);
    SCOPE_EXIT {
      hasPendingRequest_ = false;
      promise->setValue();
    };
    asyncResult_ = syncLookup(size);
  });
}

std::unique_ptr<connector::IndexSource::LookupResult>
FileSystemIndexSource::ResultIterator::syncLookup(vector_size_t size) {
  VELOX_CHECK(hasPendingRequest_);
  if (lookupResultIter_->atEnd()) {
    return nullptr;
  }

  CpuWallTiming timing;
  SCOPE_EXIT {
    source_->recordCpuTiming(timing);
  };
  CpuWallTimer timer{timing};
  try {
    initBuffer<char*>(size, outputRowMapping_, rawOutputRowMapping_);
    initBuffer<vector_size_t>(size, inputRowMapping_, rawInputRowMapping_);
    auto numOut = source_->indexTable()->table->listJoinResults(
        *lookupResultIter_,
        /*includeMisses=*/true,
        folly::Range(rawInputRowMapping_, size),
        folly::Range(rawOutputRowMapping_, size),
        // TODO: support max bytes output later.
        /*maxBytes=*/std::numeric_limits<uint64_t>::max());
    outputRowMapping_->setSize(numOut * sizeof(char*));
    inputRowMapping_->setSize(numOut * sizeof(vector_size_t));

    if (numOut == 0) {
      VELOX_CHECK(lookupResultIter_->atEnd());
      return nullptr;
    }

    evalJoinConditions();

    initBuffer<vector_size_t>(numOut, inputHitIndices_, rawInputHitIndices_);
    auto numHits{0};
    for (auto i = 0; i < numOut; ++i) {
      if (rawOutputRowMapping_[i] == nullptr) {
        continue;
      }
      VELOX_CHECK_LE(numHits, i);
      rawOutputRowMapping_[numHits] = rawOutputRowMapping_[i];
      rawInputHitIndices_[numHits] = rawInputRowMapping_[i];
      if (numHits > 0) {
        // Make sure the input hit indices are in ascending order.
        VELOX_CHECK_GE(
            rawInputHitIndices_[numHits], rawInputHitIndices_[numHits - 1]);
      }
      ++numHits;
    }
    outputRowMapping_->setSize(numHits * sizeof(char*));
    inputHitIndices_->setSize(numHits * sizeof(vector_size_t));
    extractLookupColumns(
        folly::Range<char* const*>(rawOutputRowMapping_, numHits),
        lookupOutput_);
    VELOX_CHECK_EQ(lookupOutput_->size(), numHits);
    VELOX_CHECK_EQ(inputHitIndices_->size() / sizeof(vector_size_t), numHits);
    if (lookupOutput_->size() == 0) {
      return nullptr;
    } else {
      return std::make_unique<LookupResult>(inputHitIndices_, lookupOutput_);
    }
  } catch (const std::exception& e) {
    VELOX_CHECK(source_->error_.empty());
    source_->error_ = e.what();
    return nullptr;
  }
}

void FileSystemIndexSource::ResultIterator::evalJoinConditions() {
  if (source_->conditionExprSet_ == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> l(source_->mutex_);
  const auto conditionInput = createConditionInput();
  source_->connectorQueryCtx_->expressionEvaluator()->evaluate(
      source_->conditionExprSet_.get(),
      source_->conditionFilterInputRows_,
      *conditionInput,
      source_->conditionFilterResult_);
  source_->decodedConditionFilterResult_.decode(
      *source_->conditionFilterResult_, source_->conditionFilterInputRows_);

  const auto numRows = outputRowMapping_->size() / sizeof(char*);
  for (auto row = 0; row < numRows; ++row) {
    if (!joinConditionPassed(row)) {
      rawOutputRowMapping_[row] = nullptr;
    }
  }
}

RowVectorPtr FileSystemIndexSource::ResultIterator::createConditionInput() {
  VELOX_CHECK_EQ(
      inputRowMapping_->size() / sizeof(vector_size_t),
      outputRowMapping_->size() / sizeof(char*));
  const auto numRows = outputRowMapping_->size() / sizeof(char*);
  source_->conditionFilterInputRows_.resize(numRows);
  std::vector<VectorPtr> filterColumns(source_->conditionInputType_->size());
  for (const auto& projection : source_->conditionInputProjections_) {
    request_.input->childAt(projection.inputChannel)->loadedVector();
    filterColumns[projection.outputChannel] = exec::wrapChild(
        numRows,
        inputRowMapping_,
        request_.input->childAt(projection.inputChannel));
  }

  extractColumns(
      source_->indexTable()->table.get(),
      folly::Range<char* const*>(rawOutputRowMapping_, numRows),
      source_->conditionTableProjections_,
      source_->pool_.get(),
      source_->conditionInputType_->children(),
      filterColumns);

  return std::make_shared<RowVector>(
      source_->pool_.get(),
      source_->conditionInputType_,
      nullptr,
      numRows,
      std::move(filterColumns));
}

} // namespace facebook::velox::connector::filesystem
