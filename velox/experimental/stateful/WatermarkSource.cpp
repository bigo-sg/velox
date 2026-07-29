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
#include "velox/experimental/stateful/WatermarkSource.h"

#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

WatermarkSource::WatermarkSource(
    std::unique_ptr<exec::Operator> op,
    std::vector<StatefulOperatorPtr> targets,
    std::unique_ptr<WatermarkGenerator> watermarkGenerator)
    : StatefulOperator(std::move(op), std::move(targets)),
      watermarkGenerator_(std::move(watermarkGenerator)) {
  VELOX_CHECK_NOT_NULL(watermarkGenerator_);
}

WatermarkSource::~WatermarkSource() {
  shutdownScheduler();
}

void WatermarkSource::initialize() {
  StatefulOperator::initialize();
  watermarkGenerator_->initialize();
}

void WatermarkSource::advance() {
  setSourceEmpty(true);
  auto intermediateResult = op()->getOutput();
  if (!intermediateResult) {
    return;
  }

  setSourceEmpty(false);
  pushOutput(StreamRecord::create(getPlanNodeId(), intermediateResult));

  if (intermediateResult->size() == 0) {
    return;
  }

  auto watermark = watermarkGenerator_->generate(intermediateResult);
  if (watermark.has_value()) {
    emitWatermark(watermark.value());
  }

  if (watermarkGenerator_->isIdlenessEnabled()) {
    int64_t now = TimeWindowUtil::getCurrentProcessingTime();
    if (watermarkGenerator_->onRecord(now)) {
      emitWatermarkStatus(false);
    }
  }
}

void WatermarkSource::checkWatermarkStatus(int64_t now) {
  if (!watermarkGenerator_->isIdlenessEnabled()) {
    StatefulOperator::checkWatermarkStatus(now);
    return;
  }

  if (watermarkGenerator_->checkIdle(now)) {
    emitWatermarkStatus(true);
  }

  scheduleIdleTimer(now);

  StatefulOperator::checkWatermarkStatus(now);
}

void WatermarkSource::scheduleIdleTimer(int64_t now) {
  idleTimerManager_.schedule(
      now,
      watermarkGenerator_->isIdlenessEnabled(),
      watermarkGenerator_->isIdle(),
      watermarkGenerator_->idleDeadline(),
      [this](int64_t timestamp) {
        auto bridge = nativeCallbackBridge();
        if (bridge) {
          bridge->onProcessingTime(timestamp);
        }
      });
}

void WatermarkSource::shutdownScheduler() {
  idleTimerManager_.shutdown();
}

void WatermarkSource::close() {
  shutdownScheduler();
  if (watermarkGenerator_) {
    watermarkGenerator_->close();
    watermarkGenerator_.reset();
  }
  StatefulOperator::close();
}

} // namespace facebook::velox::stateful
