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
#include <experimental/stateful/window/Window.h>
#include <vector/ComplexVector.h>
#include <vector/DictionaryVector.h>
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include <cstdint>
#include <numeric>
#include <vector>

namespace facebook::velox::stateful {

namespace {

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
  auto calculateWindowEnd = []
  (WindowType windowType, TypeKind rowtimeFieldType, int64_t timestampMs, int64_t offset, int64_t size, bool isProcessingTime) -> int64_t {
    if (windowType == WindowType::TUMBLE && (rowtimeFieldType == TypeKind::TIMESTAMP || isProcessingTime)) {
      int64_t utcTimestamp = TimeWindowUtil::toEpochMillsForTimer(timestampMs, 0);
      return stateful::TimeWindowUtil::getWindowStartWithOffset(utcTimestamp, offset, size) + size;
    } else {
      return timestampMs;
    }
  };
  if (rowtimeIndex_ < 0) {
    constexpr TypeKind kProcessingTimeType = TypeKind::BIGINT;
    int64_t timestampMs = TimeWindowUtil::getCurrentProcessingTime();
    int64_t windowEnd = calculateWindowEnd(windowType_, kProcessingTimeType, timestampMs, offset_, size_, true);
    return {{windowEnd, input}};
  } else {
    VELOX_CHECK_LT(
        rowtimeIndex_,
        input->childrenSize(),
        "rowtimeIndex out of bounds: {} >= {}",
        rowtimeIndex_,
        input->childrenSize());
    TypeKind rowtimeFieldType = input->childAt(rowtimeIndex_)->typeKind();
    std::map<int64_t, RowVectorPtr> res;
    std::map<int64_t, RowVectorPtr> partitionToData = keySelector_->partition(input);
    for (auto& kv : partitionToData) {
      int64_t windowEnd = calculateWindowEnd(windowType_, rowtimeFieldType, kv.first, offset_, size_, false);
      if (res.count(windowEnd) == 0) {
        res[windowEnd] = kv.second;
      } else {
        res[windowEnd] = TimeWindowUtil::mergeVectors({res[windowEnd], kv.second}, input->pool());
      }
    }
    return res;
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
