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
#include "velox/experimental/stateful/WatermarkGenerator.h"

#include "velox/common/base/Nulls.h"

namespace facebook::velox::stateful {

namespace watermark {

void validateRowtimeNoNulls(const RowVectorPtr& input, int rowtimeFieldIndex) {
  if (rowtimeFieldIndex < 0) {
    return;
  }
  VELOX_CHECK(
      rowtimeFieldIndex < input->childrenSize(),
      "Rowtime field index out of bounds");
  auto* rowtimeVector = input->childAt(rowtimeFieldIndex).get();
  const uint64_t* rawNulls = rowtimeVector->rawNulls();
  if (rawNulls != nullptr) {
    const vector_size_t size = rowtimeVector->size();
    const uint64_t nullCount = bits::countNulls(rawNulls, 0, size);
    if (nullCount > 0) {
      VELOX_FAIL(
          "RowTime field should not have nulls, but found {} nulls", nullCount);
    }
  }
}

RowVectorPtr getTimestampVector(exec::Operator* op, const RowVectorPtr& input) {
  RowVectorPtr timestampVector = op->getOutput();
  VELOX_CHECK(
      timestampVector->size() == input->size(),
      "Timestamps are not equal to input.");
  return timestampVector;
}

template <bool HasEmitFn>
int64_t extractWatermarkImpl(
    const int64_t* timestamps,
    vector_size_t timestampSize,
    int64_t currentWatermark,
    int64_t lastWatermark,
    int64_t watermarkInterval,
    const std::function<void(int64_t watermark, vector_size_t index)>& emitFn) {
  int64_t nextWatermarkThreshold = lastWatermark + watermarkInterval;

  for (vector_size_t i = 0; i < timestampSize; ++i) {
    if (timestamps[i] > currentWatermark) {
      currentWatermark = timestamps[i];
      if (currentWatermark > nextWatermarkThreshold) {
        if constexpr (HasEmitFn) {
          emitFn(currentWatermark, i);
        }
        nextWatermarkThreshold = currentWatermark + watermarkInterval;
      }
    }
  }
  return currentWatermark;
}

int64_t extractWatermark(
    const int64_t* timestamps,
    vector_size_t timestampSize,
    int64_t currentWatermark,
    int64_t lastWatermark,
    int64_t watermarkInterval,
    const std::function<void(int64_t watermark, vector_size_t index)>& emitFn) {
  if (emitFn) {
    return extractWatermarkImpl<true>(
        timestamps,
        timestampSize,
        currentWatermark,
        lastWatermark,
        watermarkInterval,
        emitFn);
  } else {
    return extractWatermarkImpl<false>(
        timestamps,
        timestampSize,
        currentWatermark,
        lastWatermark,
        watermarkInterval,
        emitFn);
  }
}

} // namespace watermark

WatermarkGenerator::WatermarkGenerator(
    std::unique_ptr<exec::Operator> op,
    const int64_t idleTimeout,
    const int rowtimeFieldIndex,
    const int64_t watermarkInterval)
    : op_(std::move(op)),
      idleTimeout_(idleTimeout),
      rowtimeFieldIndex_(rowtimeFieldIndex),
      watermarkInterval_(watermarkInterval) {}

std::optional<int64_t> WatermarkGenerator::generate(const RowVectorPtr& input) {
  watermark::validateRowtimeNoNulls(input, rowtimeFieldIndex_);
  op_->addInput(input);
  RowVectorPtr timestampVector =
      watermark::getTimestampVector(op_.get(), input);
  const int64_t* timestamps =
      timestampVector->childAt(0)->asFlatVector<int64_t>()->rawValues();
  const vector_size_t timestampSize = timestampVector->size();
  std::optional<int64_t> emittedWatermark;

  currentWatermark_ = watermark::extractWatermark(
      timestamps,
      timestampSize,
      currentWatermark_,
      lastWatermark_,
      watermarkInterval_,
      [&](int64_t watermark, vector_size_t) {
        lastWatermark_ = watermark;
        emittedWatermark = watermark;
      });
  return emittedWatermark;
}

void WatermarkGenerator::close() {
  if (!op_) {
    return;
  }
  op_->close();
  op_.reset();
}

} // namespace facebook::velox::stateful
