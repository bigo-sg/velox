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
#include "velox/experimental/stateful/window/MergingWindowSet.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/experimental/stateful/window/WindowProcessFunction.h"

namespace facebook::velox::stateful {

MergingWindowSet::MergingWindowSet(
    std::shared_ptr<MergingWindowAssigner> windowAssigner,
    std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>> mapping)
    : windowAssigner_(std::move(windowAssigner)),
      mapping_(std::move(mapping)),
      cachedSortedWindows_(MAPPING_CACHE_SIZE) {
}

void MergingWindowSet::initializeCache(uint32_t key) {
  auto sortedWindowsItr = cachedSortedWindows_.find(key);
  if (sortedWindowsItr == cachedSortedWindows_.end()) {
    sortedWindows_ = std::set<TimeWindow>();
    std::map<TimeWindow, TimeWindow> keyValues = mapping_->entries(key, 0);
    for (auto [windowKey, value] : keyValues) {
      sortedWindows_.insert(windowKey);
    }
    cachedSortedWindows_.set(key, sortedWindows_);
  } else {
    // if the cache is not empty, we can use the cached sorted windows
    sortedWindows_ = sortedWindowsItr->second;
  }
}

TimeWindow MergingWindowSet::addWindow(
    uint32_t key,
    TimeWindow newWindow,
    std::shared_ptr<MergingFunction> mergeFunction) {
  MergeResultCollector collector;
  windowAssigner_->mergeWindows(newWindow, sortedWindows_, collector);

  TimeWindow resultWindow = newWindow;
  bool isNewWindowMerged = false;

  // perform the merge
  for (auto [mergeResult, mergedWindows] : collector.mergeResults()) {
    // if our new window is in the merged windows make the merge result the result window
    if (mergedWindows.erase(newWindow)) {
      isNewWindowMerged = true;
      resultWindow = mergeResult;
    }

    // if our new window is the same as a pre-existing window, nothing to do
    if (mergedWindows.empty()) {
      continue;
    }

    // pick any of the merged windows and choose that window's state window
    // as the state window for the merge result
    TimeWindow mergedStateNamespace = mapping_->get(key, 0, *mergedWindows.begin());

    // figure out the state windows that we are merging
    std::vector<TimeWindow> mergedStateWindows;
    for (TimeWindow mergedWindow : mergedWindows) {
      TimeWindow res = mapping_->get(key, 0, mergedWindow);
      if (res.valid()) {
        mapping_->remove(key, 0, mergedWindow);
        sortedWindows_.erase(mergedWindow);
        // don't put the target state window into the merged windows
        if (!(res == mergedStateNamespace)) {
          mergedStateWindows.push_back(res);
        }
      }
    }

    mapping_->put(key, 0, mergeResult, mergedStateNamespace);
    sortedWindows_.insert(mergeResult);

    // don't merge the new window itself, it never had any state associated with it
    // i.e. if we are only merging one pre-existing window into itself
    // without extending the pre-existing window
    if (!(mergedWindows.find(mergeResult) != mergedWindows.end() && mergedWindows.size() == 1)) {
      mergeFunction->merge(
          mergeResult, mergedWindows, mergedStateNamespace, mergedStateWindows);
    }
  }

  // the new window created a new, self-contained window without merging
  if (collector.mergeResults().empty()
      || (resultWindow == newWindow && !isNewWindowMerged)) {
    mapping_->put(key, 0, resultWindow, resultWindow);
    sortedWindows_.insert(resultWindow);
  }

  return resultWindow;
}

TimeWindow MergingWindowSet::getStateWindow(uint32_t key, TimeWindow window) {
  return mapping_->get(key, 0, window);
}

void MergingWindowSet::retireWindow(uint32_t key, TimeWindow window) {
  mapping_->remove(key, 0, window);
  VELOX_CHECK(sortedWindows_.erase(window), "Window " + window.toString() + " is not in in-flight window set.");
}

void MergingWindowSet::close() {
  cachedSortedWindows_.clear();
  sortedWindows_.clear();
  mapping_->clear();
}

MergingFunction::MergingFunction(
    std::shared_ptr<DefaultAccMergingConsumer> accMergingConsumer,
    std::shared_ptr<FunctionContext<TimeWindow>> ctx,
    int64_t allowedLateness,
    bool isEventTime)
    : accMergingConsumer_(std::move(accMergingConsumer)),
      ctx_(ctx),
      allowedLateness_(allowedLateness),
      isEventTime_(isEventTime) {
}

void MergingFunction::merge(
    TimeWindow mergeResult,
    std::set<TimeWindow>& mergedWindows,
    TimeWindow stateWindowResult,
    std::vector<TimeWindow>& stateWindowsToBeMerged) {

  int64_t mergeResultMaxTs =
      TimeWindowUtil::toEpochMillsForTimer(mergeResult.maxTimestamp(), ctx_->getShiftTimeZone());
  VELOX_CHECK(!(isEventTime_
      && mergeResultMaxTs + allowedLateness_ <= ctx_->currentWatermark()),
      std::string("The end timestamp of an "
      "event-time window cannot become earlier than the current watermark "
      "by merging. Current watermark: ")
      + std::to_string(ctx_->currentWatermark())
      + std::string(" window: ")
      + mergeResult.toString());
  VELOX_CHECK(!(!isEventTime_
      && mergeResultMaxTs <= ctx_->currentProcessingTime()),
      std::string("The end timestamp of a "
      "processing-time window cannot become earlier than the current processing time "
      "by merging. Current processing time: ")
      + std::to_string(ctx_->currentProcessingTime())
      + std::string(" window: ")
      + mergeResult.toString());

  ctx_->onMerge(mergeResult, stateWindowsToBeMerged);

  // clear registered timers
  for (TimeWindow m : mergedWindows) {
    ctx_->clearTrigger(m);
    ctx_->deleteCleanupTimer(m);
  }

  // merge the merged state windows into the newly resulting state window
  if (!stateWindowsToBeMerged.empty()) {
    accMergingConsumer_->accept(stateWindowResult, stateWindowsToBeMerged);
  }
}

} // namespace facebook::velox::stateful
