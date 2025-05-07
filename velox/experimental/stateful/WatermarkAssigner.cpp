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
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const WatermarkAssignerNode>& watermarkAssigner)
    : Operator(
          driverCtx,
          watermarkAssigner->outputType(),
          operatorId,
          watermarkAssigner->id(),
          "WatermarkAssigner"),
      idleTimeout(watermarkAssigner->idleTimeout()),
      rowtimeFieldIndex(watermarkAssigner->rowtimeFieldIndex()) {
  project_ = std::make_shared<exec::FilterProject>(
      operatorId,
      driverCtx,
      nullptr,
      watermarkAssigner->project());
}

void WatermarkAssigner::initialize() {
  Operator::initialize();
  project_->initialize();
}

void WatermarkAssigner::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

bool WatermarkAssigner::isFinished() {
  // stream operator never finish
  return false;
}

RowVectorPtr WatermarkAssigner::getOutput() {
  if (!input_) {
    return nullptr;
  }

  RowVectorPtr output = input_;
  for (int i = 0; i < output->size(); i++) {
    VELOX_CHECK(
      !output->childAt(rowtimeFieldIndex)->isNullAt(i),
      "RowTime field should not be null, please convert it to a non-null long value.");
  }
  project_->addInput(input_);
  RowVectorPtr timestamps = project_->getOutput();

  VELOX_CHECK(
    timestamps->size() == output->size(),
    "Timestamps are not equal to input.");

  // TODO: generate watermark according to timestamps;
  auto timestamp = timestamps->childAt(0)->asFlatVector<int64_t>();
  for (int i = 0; i < timestamps->size(); i++) {
    currentWatermark = std::max(currentWatermark, timestamp->valueAt(0));
  }
  return output;
}

void WatermarkAssigner::close() {
  project_->close();
  Operator::close();
}

} // namespace facebook::velox::stateful
