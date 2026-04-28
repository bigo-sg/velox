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
#include "velox/experimental/stateful/WatermarkAssigner.h"
#include <cstdint>
#include "velox/common/base/Nulls.h"

namespace facebook::velox::stateful {

WatermarkAssigner::WatermarkAssigner(
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    const int64_t idleTimeout,
    const int rowtimeFieldIndex,
    const int64_t watermarkInterval)
    : StatefulOperator(std::move(op), std::move(targets)),
      idleTimeout_(idleTimeout),
      rowtimeFieldIndex_(rowtimeFieldIndex),
      watermarkInterval_(watermarkInterval) {}

void WatermarkAssigner::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  input_ = record->record();
  op()->addInput(input_);
}

void WatermarkAssigner::advance() {
  if (!input_) {
    return;
  }
  // Check for nulls using countNulls
  auto* rowtimeVector = input_->childAt(rowtimeFieldIndex_).get();
  const uint64_t* rawNulls = rowtimeVector->rawNulls();
  if (rawNulls != nullptr) {
    const vector_size_t size = rowtimeVector->size();
    const uint64_t nullCount = bits::countNulls(rawNulls, 0, size);
    if (nullCount > 0) {
      VELOX_FAIL(
          "RowTime field should not have nulls, but found {} nulls", nullCount);
    }
  }
  RowVectorPtr timestampVector = op()->getOutput();
  VELOX_CHECK(
      timestampVector->size() == input_->size(),
      "Timestamps are not equal to input.");
  
  const int64_t* timestamps =
      timestampVector->childAt(0)->asFlatVector<int64_t>()->rawValues();
  const vector_size_t timestampSize = timestampVector->size();
  vector_size_t lastIndex = 0;

  // Pre-compute threshold to avoid repeated subtraction in hot loop
  int64_t nextWatermarkThreshold = lastWatermark + watermarkInterval_;
  for (vector_size_t i = 0; i < timestampSize; ++i) {
    const int64_t timestamp = timestamps[i];
    // Only update currentWatermark if timestamp is greater (avoid unnecessary
    // max call)
    if (timestamp > currentWatermark) {
      currentWatermark = timestamp;
      // Check if watermark threshold is crossed
      if (currentWatermark > nextWatermarkThreshold) {
        auto output = std::dynamic_pointer_cast<RowVector>(
            input_->slice(lastIndex, i - lastIndex + 1));
        lastIndex = i + 1;
        pushOutput(
            std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
        advanceWatermark();
        // Update threshold after watermark advance
        nextWatermarkThreshold = lastWatermark + watermarkInterval_;
      }
    }
  }

  // Handle remaining data
  if (lastIndex == 0) {
    pushOutput(
        std::make_shared<StreamRecord>(getPlanNodeId(), std::move(input_)));
  } else if (lastIndex < timestampSize) {
    auto output = std::dynamic_pointer_cast<RowVector>(
        input_->slice(lastIndex, timestampSize - lastIndex));
    pushOutput(
        std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
  }
  input_.reset();
}

void WatermarkAssigner::advanceWatermark() {
  if (currentWatermark > lastWatermark) {
    lastWatermark = currentWatermark;
    // emit watermark
    emitWatermark(currentWatermark);
  }
}

void WatermarkAssigner::close() {
  StatefulOperator::close();
  input_.reset();
}

} // namespace facebook::velox::stateful
