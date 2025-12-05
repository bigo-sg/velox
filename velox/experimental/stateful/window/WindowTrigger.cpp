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
#include "velox/experimental/stateful/window/WindowTrigger.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

long WindowTrigger::triggerTime(TimeWindow window) {
  return TimeWindowUtil::toEpochMillsForTimer(window.maxTimestamp(), ctx_->getShiftTimeZone());
}

void AfterEndOfWindow::open(std::shared_ptr<TriggerContext> ctx) {
  ctx_ = ctx;
}

bool AfterEndOfWindow::onElement(uint32_t key, RowVectorPtr element, long timestamp, TimeWindow window) {
  if (triggerTime(window) <= ctx_->getCurrentWatermark()) {
    // if the watermark is already past the window fire immediately
    return true;
  } else {
    ctx_->registerEventTimeTimer(key, window, triggerTime(window));
    return false;
  }
}

bool AfterEndOfWindow::onProcessingTime(TimeWindow window, long time) {
  return false;
}

bool AfterEndOfWindow::onEventTime(TimeWindow window, long time) {
  return time == triggerTime(window);
}

void AfterEndOfWindow::clear(uint32_t key, TimeWindow window) {
  ctx_->deleteEventTimeTimer(key, window, triggerTime(window));
}

bool AfterEndOfWindow::canMerge() {
  return true;
}

void AfterEndOfWindow::onMerge(
    uint32_t key, TimeWindow window, std::shared_ptr<TriggerContext> mergeContext) {
  ctx_->registerEventTimeTimer(key, window, triggerTime(window));
}

} // namespace facebook::velox::stateful
