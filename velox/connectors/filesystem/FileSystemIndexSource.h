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
#include "velox/connectors/filesystem/FileSystemConfig.h"
#include "velox/connectors/filesystem/FileSystemIndexTable.h"
#include "velox/connectors/filesystem/FileSystemIndexTableHandle.h"
#include "velox/dwio/common/Reader.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/Operator.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::filesystem {

class FileSystemIndexSource
    : public connector::IndexSource,
      public std::enable_shared_from_this<FileSystemIndexSource> {
 public:
  FileSystemIndexSource(
      const RowTypePtr& inputType,
      const RowTypePtr& outputType,
      size_t numEqualJoinKeys,
      const core::TypedExprPtr& joinConditionExpr,
      const std::shared_ptr<FileSystemIndexTableHandle>& tableHandle,
      connector::ConnectorQueryCtx* connectorQueryCtx,
      std::shared_ptr<folly::Executor>& executor);

  std::shared_ptr<LookupResultIterator> lookup(
      const LookupRequest& request) override;

  std::unordered_map<std::string, RuntimeMetric> runtimeStats() override;

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  const std::shared_ptr<FileSystemIndexTable>& indexTable() const {
    return lookupTable_;
  }

  const RowTypePtr& outputType() const {
    return outputType_;
  }

  const std::vector<exec::IdentityProjection>& outputProjections() const {
    return lookupOutputProjections_;
  }

  class ResultIterator : public LookupResultIterator {
   public:
    ResultIterator(
        std::shared_ptr<FileSystemIndexSource> source,
        const LookupRequest& request,
        std::unique_ptr<exec::HashLookup> lookupResult,
        folly::Executor* executor);

    std::optional<std::unique_ptr<LookupResult>> next(
        vector_size_t size,
        ContinueFuture& future) override;

   private:
    // Initializes the buffer used to store row pointers or indices for output
    // match result processing.
    template <typename T>
    void initBuffer(vector_size_t size, BufferPtr& buffer, T*& rawBuffer) {
      if (!buffer || !buffer->unique() ||
          buffer->capacity() < sizeof(T) * size) {
        buffer = AlignedBuffer::allocate<T>(size, source_->pool_.get(), T());
      }
      rawBuffer = buffer->asMutable<T>();
    }

    void evalJoinConditions();

    // Check if a given equality matched 'row' has passed join conditions.
    inline bool joinConditionPassed(vector_size_t row) const {
      return source_->conditionFilterInputRows_.isValid(row) &&
             !source_->decodedConditionFilterResult_.isNullAt(row) &&
             source_->decodedConditionFilterResult_.valueAt<bool>(row);
    }

    // Creates input vector for join condition evaluation.
    RowVectorPtr createConditionInput();

    // Extracts the lookup result columns from the index table and return in
    // 'result'.
    void extractLookupColumns(
        folly::Range<char* const*> rows,
        RowVectorPtr& result);

    // Inokved to trigger async lookup using background executor and return the
    // 'future'.
    void asyncLookup(vector_size_t size, ContinueFuture& future);

    // Synchronously lookup the index table and return up to 'size' number of
    // output rows in result.
    std::unique_ptr<LookupResult> syncLookup(vector_size_t size);

    const std::shared_ptr<FileSystemIndexSource> source_;
    const LookupRequest request_;
    const std::unique_ptr<exec::HashLookup> lookupResult_;
    folly::Executor* const executor_{nullptr};

    std::atomic_bool hasPendingRequest_{false};
    std::unique_ptr<exec::BaseHashTable::JoinResultIterator> lookupResultIter_;
    std::optional<std::unique_ptr<LookupResult>> asyncResult_;

    // The reusable buffers for lookup result processing.
    // The input row number in lookup request for each matched result which is
    // paired with 'outputRowMapping_' to indicate if a given input row has
    // match or not. If the corresponding output row pointer in
    // 'outputRowMapping_' is null, then there is no match for the given input
    // row pointed by 'inputRowMapping_'.
    BufferPtr inputRowMapping_;
    vector_size_t* rawInputRowMapping_{nullptr};
    // Points to the matched row pointer in 'indexTable_' for each input row. If
    // there is a miss for a given input, then this is set to null.
    BufferPtr outputRowMapping_;
    char** rawOutputRowMapping_{nullptr};
    // The input row number in request for each output row in the returned
    // lookup result. Any gap in the input row numbers means the corresponding
    // input rows that has no matches in the index table.
    BufferPtr inputHitIndices_;
    vector_size_t* rawInputHitIndices_{nullptr};

    RowVectorPtr lookupOutput_;
  };

 private:
  // Invoked to check if this source has already encountered async lookup error,
  // and throws if it has.
  void checkNotFailed();

  // Initialize the output projections for lookup result processing.
  void initOutputProjections();

  // Initialize the condition filter input type and projections if configured.
  void initConditionProjections();

  // Initialize the lookup table, load the data into table.
  void initLookupTable();

  void recordCpuTiming(const CpuWallTiming& timing);

  const std::shared_ptr<FileSystemIndexTable> createIndexTable(
      int numEqualJoinKeys,
      const RowVectorPtr& keyData,
      const RowVectorPtr& valueData);

  std::shared_ptr<FileSystemIndexTable> lookupTable_;
  const std::shared_ptr<FileSystemIndexTableHandle> tableHandle_;
  const std::shared_ptr<FileSystemReadConfig> config_;
  const RowTypePtr inputType_;
  const RowTypePtr outputType_;
  const RowTypePtr keyType_;
  const RowTypePtr valueType_;
  connector::ConnectorQueryCtx* const connectorQueryCtx_;
  const size_t numEqualJoinKeys_;
  const std::unique_ptr<exec::ExprSet> conditionExprSet_;
  const std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<folly::Executor> executor_;

  mutable std::mutex mutex_;

  // Join condition filter input type.
  RowTypePtr conditionInputType_;

  // If not empty, set to the first encountered async error.
  std::string error_;

  // Reusable memory for join condition filter evaluation.
  VectorPtr conditionFilterResult_;
  DecodedVector decodedConditionFilterResult_;
  SelectivityVector conditionFilterInputRows_;
  // Column projections for join condition input and lookup output.
  std::vector<exec::IdentityProjection> conditionInputProjections_;
  std::vector<exec::IdentityProjection> conditionTableProjections_;
  std::vector<exec::IdentityProjection> lookupOutputProjections_;
  std::unordered_map<std::string, RuntimeMetric> runtimeStats_;
};
} // namespace facebook::velox::connector::filesystem
