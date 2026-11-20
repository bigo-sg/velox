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
#include <cstdint>
#include <numeric>
#include <unordered_map>
#include <vector>
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
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
    int rowtimeIndex,
    const tzdb::time_zone* shiftTimeZone,
    bool attached)
    : keySelector_(std::move(keySelector)),
      size_(size),
      step_(step),
      offset_(offset),
      windowType_(windowType),
      rowtimeIndex_(rowtimeIndex),
      shiftTimeZone_(shiftTimeZone),
      attached_(attached) {
  // Calculate sliceSize based on windowType (matching Flink SliceAssigners):
  // TUMBLE -> size, HOP -> gcd(size, step), CUMULATE -> step, SESSION -> N/A.
  switch (windowType_) {
    case WindowType::TUMBLE:
      sliceSize_ = size_;
      break;
    case WindowType::HOP:
      sliceSize_ = std::gcd(size_, step_);
      break;
    case WindowType::CUMULATIVE:
      sliceSize_ = step_;
      break;
    default:
      sliceSize_ = size_;
      break;
  }
}

std::map<int64_t, RowVectorPtr> SliceAssigner::assignSliceEnd(
    const RowVectorPtr& input) {
  if (rowtimeIndex_ < 0) {
    int64_t timestampMs = TimeWindowUtil::getCurrentProcessingTime();
    if (windowType_ == WindowType::TUMBLE) {
      int64_t utcTimestamp =
          TimeWindowUtil::toUtcTimestampMills(timestampMs, shiftTimeZone_);
      int64_t windowStart = stateful::TimeWindowUtil::getWindowStartWithOffset(
          utcTimestamp, offset_, size_);
      return {{windowStart + size_, input}};
    } else {
      // Non-TUMBLE processing-time windows use the same boundary
      // alignment as event time, with the window-type-specific slice
      // size (gcd for HOP, step for CUMULATE); the whole batch shares
      // the batch-arrival processing time (Flink assigns per element,
      // which is equivalent for a batch that arrives at one instant).
      int64_t utcTimestamp =
          TimeWindowUtil::toUtcTimestampMills(timestampMs, shiftTimeZone_);
      int64_t sliceStart =
          stateful::TimeWindowUtil::getWindowStartWithOffset(
              utcTimestamp, offset_, sliceSize_);
      return {{sliceStart + sliceSize_, input}};
    }
  } else {
    const VectorPtr& rowtimeVector = input->childAt(rowtimeIndex_);
    prepareChildrenLoaded(input);
    const auto* tsSimple = rowtimeVector->as<SimpleVector<Timestamp>>();
    const auto* tsSimpleInt = rowtimeVector->as<SimpleVector<int64_t>>();
    VELOX_CHECK(
        tsSimple != nullptr || tsSimpleInt != nullptr,
        "rowtime column must be TIMESTAMP/Int64 simple vector");

    velox::memory::MemoryPool* pool = input->pool();
    std::map<int64_t, RowVectorPtr> sliceEndToData;
    const vector_size_t numRows = rowtimeVector->size();

    if (attached_) {
      // The two-stage global side: the column carries the slice end
      // the local stage assigned (BIGINT windowEnd appended by the
      // local pre-aggregation; SliceAttachedWindowingStrategy).
      VELOX_CHECK(
          tsSimpleInt != nullptr,
          "The attached slice-end column must be a BIGINT simple vector, got {}",
          rowtimeVector->typeKind());
      std::unordered_map<int64_t, std::vector<vector_size_t>> groups;
      for (vector_size_t i = 0; i < numRows; ++i) {
        VELOX_CHECK(
            !(tsSimple ? tsSimple->isNullAt(i) : tsSimpleInt->isNullAt(i)),
            "RowTime field should not be null");
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
            sliceEnd,
            wrapChildrenByIndices(input, n, indicesBuf, pool));
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

    if (windowType_ == WindowType::TUMBLE) {
      std::unordered_map<int64_t, std::vector<vector_size_t>> groups;
      for (vector_size_t i = 0; i < numRows; ++i) {
        // Flink throws on a null rowtime (SliceAssigners#assignSliceEnd);
        // silently dropping rows would under-count windows.
        VELOX_CHECK(!isNullAtRow(i), "RowTime field should not be null");
        int64_t timestampMs = timestampMillisAt(i);
        int64_t utcTimestamp =
            TimeWindowUtil::toUtcTimestampMills(timestampMs, shiftTimeZone_);
        int64_t windowStart =
            stateful::TimeWindowUtil::getWindowStartWithOffset(
                utcTimestamp, offset_, size_);
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
    // Non-TUMBLE aligned windows (HOP / CUMULATE): use the same
    // getWindowStartWithOffset boundary alignment as TUMBLE, but with
    // the window-type-specific sliceSize (gcd for HOP, step for CUMULATE).
    std::unordered_map<int64_t, std::vector<vector_size_t>> groups;
    for (vector_size_t i = 0; i < numRows; ++i) {
      VELOX_CHECK(!isNullAtRow(i), "RowTime field should not be null");
      int64_t timestampMs = timestampMillisAt(i);
      int64_t utcTimestamp =
          TimeWindowUtil::toUtcTimestampMills(timestampMs, shiftTimeZone_);
      int64_t sliceStart =
          TimeWindowUtil::getWindowStartWithOffset(utcTimestamp, offset_, sliceSize_);
      const int64_t sliceEnd = sliceStart + sliceSize_;
      groups[sliceEnd].push_back(i);
    }
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
