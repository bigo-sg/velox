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

#include <iostream>

namespace facebook::velox::stateful {

WatermarkAssigner::WatermarkAssigner(
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    const long idleTimeout,
    const int rowtimeFieldIndex,
    const long watermarkInterval)
    : StatefulOperator(std::move(op), std::move(targets)),
      idleTimeout_(idleTimeout),
      rowtimeFieldIndex_(rowtimeFieldIndex),
      watermarkInterval_(watermarkInterval) {
}

void WatermarkAssigner::addInput(RowVectorPtr input) {
  input_ = input;
  op()->addInput(std::move(input));
}

void WatermarkAssigner::getOutput() {
  if (!input_) {
    return;
  }

  for (int i = 0; i < input_->size(); i++) {
    VELOX_CHECK(
      !input_->childAt(rowtimeFieldIndex_)->isNullAt(i),
      "RowTime field should not be null, please convert it to a non-null long value.");
  }
  RowVectorPtr timestamps = op()->getOutput();

  VELOX_CHECK(
    timestamps->size() == input_->size(),
    "Timestamps are not equal to input.");

  // TODO: generate watermark according to timestamps;
  auto timestamp = timestamps->childAt(0)->asFlatVector<int64_t>();
  int lastIndex = 0;
  for (int i = 0; i < timestamps->size(); i++) {
    currentWatermark = std::max(currentWatermark, timestamp->valueAt(0));
    if (currentWatermark - lastWatermark > watermarkInterval_) {
      auto output = std::dynamic_pointer_cast<RowVector>(input_->slice(lastIndex, i - lastIndex + 1));
      lastIndex = i + 1;
      pushOutput(std::move(output));
      advanceWatermark();
    }
  }
  if (lastIndex == 0) {
    pushOutput(std::move(input_));
  }
  else if (lastIndex < input_->size()) {
    auto output = std::dynamic_pointer_cast<RowVector>(input_->slice(lastIndex, input_->size() - lastIndex));
    pushOutput(std::move(output));
  }
  input_.reset();
}

void WatermarkAssigner::advanceWatermark() {
  if (currentWatermark > lastWatermark) {
      lastWatermark = currentWatermark;
      // emit watermark
      pushWatermark(currentWatermark, 1);
  }
}

} // namespace facebook::velox::stateful
