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
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include <vector/DictionaryVector.h>
#include "velox/type/Timestamp.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"
#include <cstdint>
#include <numeric>
#include <unordered_map>
#include <vector>

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

  result->updateContainsLazyNotLoaded();
  return result;
}

} // namespace

SliceAssigner::SliceAssigner(
    std::unique_ptr<KeySelector> keySelector,
    int64_t size,
    int64_t step,
    int64_t offset,
    WindowType windowType,
    int rowtimeIndex)
    : keySelector_(std::move(keySelector)),
      size_(size),
      step_(step),
      offset_(offset),
      windowType_(windowType),
      rowtimeIndex_(rowtimeIndex) {
  // TODO: calculate sliceSize_ based on windowType.
  sliceSize_ = std::gcd(size, step);
}

std::map<int64_t, RowVectorPtr> SliceAssigner::assignSliceEnd(const RowVectorPtr& input) {
  if (rowtimeIndex_ < 0) {
    int64_t timestampMs = TimeWindowUtil::getCurrentProcessingTime();
    if (windowType_ == WindowType::TUMBLE) {
      int64_t utcTimestamp = TimeWindowUtil::toEpochMillsForTimer(timestampMs, 0);
      int64_t windowStart = stateful::TimeWindowUtil::getWindowStartWithOffset(utcTimestamp, offset_, size_);
      return {{windowStart + size_, input}};
    } else {
      return {{timestampMs, input}};
    }
  } else {
    const VectorPtr& rowtimeVector = input->childAt(rowtimeIndex_);
    prepareChildrenLoaded(input);
    const auto* tsConst = rowtimeVector->as<DictionaryVector<Timestamp>>();
    const auto* tsFlat = rowtimeVector->asFlatVector<Timestamp>();
    VELOX_CHECK(
        tsConst != nullptr || tsFlat != nullptr,
        "rowtime column must be TIMESTAMP flat or constant vector");

    const vector_size_t numRows = rowtimeVector->size();
    auto isNullAtRow = [&](vector_size_t row) {
      return tsConst ? tsConst->isNullAt(row) : tsFlat->isNullAt(row);
    };
    auto timestampMillisAt = [&](vector_size_t row) {
      return tsConst ? tsConst->valueAt(row).toMillis()
                       : tsFlat->valueAt(row).toMillis();
    };

    velox::memory::MemoryPool* pool = input->pool();
    std::map<int64_t, RowVectorPtr> sliceEndToData;

    if (windowType_ == WindowType::TUMBLE) {
      std::unordered_map<int64_t, std::vector<vector_size_t>> groups;
      for (vector_size_t i = 0; i < numRows; ++i) {
        if (isNullAtRow(i)) {
          continue;
        }
        int64_t timestampMs = timestampMillisAt(i);
        int64_t utcTimestamp = TimeWindowUtil::toEpochMillsForTimer(timestampMs, 0);
        int64_t windowStart =
            stateful::TimeWindowUtil::getWindowStartWithOffset(utcTimestamp, offset_, size_);
        const int64_t sliceEnd = windowStart + size_;
        groups[sliceEnd].push_back(i);
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
    LOG(INFO) << "xxxx111";
    std::unordered_map<int64_t, std::vector<vector_size_t>> groups;
    for (vector_size_t i = 0; i < numRows; ++i) {
      if (isNullAtRow(i)) {
        continue;
      }
      int64_t timestampMs = timestampMillisAt(i);
      int64_t key = TimeWindowUtil::toEpochMillsForTimer(timestampMs, 0);
      groups[key].push_back(i);
    }
    LOG(INFO) << "xxxx222";
    for (auto& [timeKey, rowIndices] : groups) {
      const vector_size_t n = rowIndices.size();
      BufferPtr indicesBuf = allocateIndices(n, pool);
      auto* raw = indicesBuf->asMutable<vector_size_t>();
      for (vector_size_t j = 0; j < n; ++j) {
        raw[j] = rowIndices[j];
      }
      sliceEndToData[timeKey] =
          wrapChildrenByIndices(input, n, indicesBuf, pool);
    }
    LOG(INFO) << "xxxx333";
    return sliceEndToData;
  }
}

int64_t SliceAssigner::getLastWindowEnd(int64_t sliceEnd) {
  if (windowType_ == WindowType::HOP) { // Hopping window
    return sliceEnd - sliceSize_ + size_;
  }
  return sliceEnd;
}

int64_t SliceAssigner::getWindowStart(int64_t windowEnd) {
  return windowEnd - size_;
}

int64_t SliceAssigner::getSliceEndInterval() {
  return sliceSize_;
}

} // namespace facebook::velox::stateful
