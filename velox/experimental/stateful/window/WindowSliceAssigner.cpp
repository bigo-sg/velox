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

#include "velox/experimental/stateful/window/WindowSliceAssigner.h"

#include <numeric>
#include <unordered_map>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

namespace {

void prepareChildrenLoaded(const RowVectorPtr& input) {
  for (auto& child : input->children()) {
    child->loadedVector();
  }
}

RowVectorPtr wrapChildrenByIndices(
    const RowVectorPtr& input,
    vector_size_t size,
    const BufferPtr& indices,
    velox::memory::MemoryPool* pool) {
  RowVectorPtr result = std::make_shared<RowVector>(
      pool,
      input->type(),
      nullptr,
      size,
      std::vector<VectorPtr>(input->childrenSize()));

  for (vector_size_t i = 0; i < input->childrenSize(); ++i) {
    auto& child = result->childAt(i);
    if (child && child->encoding() == VectorEncoding::Simple::DICTIONARY &&
        child.use_count() == 1) {
      child->BaseVector::resize(size);
      child->setWrapInfo(indices);
      child->setValueVector(input->childAt(i));
    } else {
      child = BaseVector::wrapInDictionary(
          nullptr, indices, size, input->childAt(i));
    }
  }
  return result;
}

} // namespace

std::map<int64_t, RowVectorPtr> WindowSliceAssigner::assignSliceEnd(
    const RowVectorPtr& input) {
  if (rowtimeIndex_ < 0) {
    // Processing-time windows: the whole batch shares the batch-arrival
    // wall-clock instant (Flink assigns per element, which is
    const int64_t now = WindowTimezoneUtil::getCurrentProcessingTime();
    const int64_t utcTimestamp =
        WindowTimezoneUtil::toUtcTimestampMills(now, shiftTimeZone_);
    const int64_t sliceStart = TimeWindowUtil::getWindowStartWithOffset(
        utcTimestamp, offset_, sliceSize_);
    return {{sliceStart + sliceSize_, input}};
  }

  const VectorPtr& rowtimeVector = input->childAt(rowtimeIndex_);
  prepareChildrenLoaded(input);
  const auto* tsSimple = rowtimeVector->as<SimpleVector<Timestamp>>();
  const auto* tsSimpleInt = rowtimeVector->as<SimpleVector<int64_t>>();
  VELOX_CHECK(
      tsSimple != nullptr || tsSimpleInt != nullptr,
      "rowtime column must be TIMESTAMP/Int64 simple vector");

  if (attached_) {
    // The two-stage global side: the column carries the slice end the
    VELOX_CHECK(
        tsSimpleInt != nullptr,
        "The attached slice-end column must be a BIGINT simple vector, got {}",
        rowtimeVector->typeKind());
    velox::memory::MemoryPool* pool = input->pool();
    std::map<int64_t, RowVectorPtr> sliceEndToData;
    const vector_size_t numRows = rowtimeVector->size();
    std::unordered_map<int64_t, std::vector<vector_size_t>> groups;
    for (vector_size_t i = 0; i < numRows; ++i) {
      VELOX_CHECK(
          !tsSimpleInt->isNullAt(i), "RowTime field should not be null");
      groups[tsSimpleInt->valueAt(i)].push_back(i);
    }
    for (const auto& [sliceEnd, rowIndices] : groups) {
      const vector_size_t n = rowIndices.size();
      BufferPtr indicesBuf = allocateIndices(n, pool);
      auto* raw = indicesBuf->asMutable<vector_size_t>();
      for (vector_size_t j = 0; j < n; ++j) {
        raw[j] = rowIndices[j];
      }
      sliceEndToData.emplace(
          sliceEnd, wrapChildrenByIndices(input, n, indicesBuf, pool));
    }
    return sliceEndToData;
  }

  auto isNullAtRow = [&](vector_size_t row) {
    return tsSimple ? tsSimple->isNullAt(row) : tsSimpleInt->isNullAt(row);
  };
  auto timestampMillisAt = [&](vector_size_t row) {
    return tsSimple ? tsSimple->valueAt(row).toMillis()
                    : tsSimpleInt->valueAt(row);
  };

  velox::memory::MemoryPool* pool = input->pool();
  std::map<int64_t, RowVectorPtr> sliceEndToData;
  const vector_size_t numRows = rowtimeVector->size();

  std::unordered_map<int64_t, std::vector<vector_size_t>> groups;
  for (vector_size_t i = 0; i < numRows; ++i) {
    // Flink throws on a null rowtime (SliceAssigners#assignSliceEnd);
    VELOX_CHECK(!isNullAtRow(i), "RowTime field should not be null");
    const int64_t timestampMs = timestampMillisAt(i);
    const int64_t utcTimestamp =
        WindowTimezoneUtil::toUtcTimestampMills(timestampMs, shiftTimeZone_);
    const int64_t sliceStart = TimeWindowUtil::getWindowStartWithOffset(
        utcTimestamp, offset_, sliceSize_);
    groups[sliceStart + sliceSize_].push_back(i);
  }
  for (auto& [sliceEnd, rowIndices] : groups) {
    const vector_size_t n = rowIndices.size();
    BufferPtr indicesBuf = allocateIndices(n, pool);
    auto* raw = indicesBuf->asMutable<vector_size_t>();
    for (vector_size_t j = 0; j < n; ++j) {
      raw[j] = rowIndices[j];
    }
    sliceEndToData[sliceEnd] =
        wrapChildrenByIndices(input, n, indicesBuf, pool);
  }
  return sliceEndToData;
}

} // namespace facebook::velox::stateful
