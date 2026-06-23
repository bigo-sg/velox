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

#include "velox/experimental/stateful/WatermarkGenerator.h"

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
  watermark::validateRowtimeNoNulls(input_, rowtimeFieldIndex_);
  RowVectorPtr timestampVector =
      watermark::getTimestampVector(op().get(), input_);

  const int64_t* timestamps =
      timestampVector->childAt(0)->asFlatVector<int64_t>()->rawValues();
  const vector_size_t timestampSize = timestampVector->size();
  vector_size_t lastIndex = 0;

  currentWatermark = watermark::extractWatermark(
      timestamps,
      timestampSize,
      currentWatermark,
      lastWatermark,
      watermarkInterval_,
      [&](int64_t watermark, vector_size_t i) {
        auto output = std::dynamic_pointer_cast<RowVector>(
            input_->slice(lastIndex, i - lastIndex + 1));
        lastIndex = i + 1;
        pushOutput(
            std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
        lastWatermark = watermark;
        emitWatermark(watermark);
      });

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

void WatermarkAssigner::close() {
  StatefulOperator::close();
  input_.reset();
}

} // namespace facebook::velox::stateful
