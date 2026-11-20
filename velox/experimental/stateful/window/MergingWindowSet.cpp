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
#include <cstdint>
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/experimental/stateful/window/WindowProcessFunction.h"

namespace facebook::velox::stateful {

MergingWindowSet::MergingWindowSet(
    std::shared_ptr<MergingWindowAssigner> windowAssigner,
    std::shared_ptr<SessionWindowMappingState> mapping)
    : windowAssigner_(std::move(windowAssigner)),
      mapping_(std::move(mapping)),
      cachedSortedWindows_(MAPPING_CACHE_SIZE) {}

void MergingWindowSet::initializeCache(const RowContainerStateKey& key) {
  auto sortedWindowsItr = cachedSortedWindows_.find(key);
  if (sortedWindowsItr == cachedSortedWindows_.end()) {
    sortedWindows_ = std::set<TimeWindow>();
    std::map<TimeWindow, TimeWindow> keyValues =
        mapping_->entries(key, VoidNamespace::instance());
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
    const RowContainerStateKey& key,
    TimeWindow newWindow,
    std::shared_ptr<MergingFunction> mergeFunction) {
  MergeResultCollector collector;
  windowAssigner_->mergeWindows(newWindow, sortedWindows_, collector);

  TimeWindow resultWindow = newWindow;
  bool isNewWindowMerged = false;

  // perform the merge
  for (auto [mergeResult, mergedWindows] : collector.mergeResults()) {
    // if our new window is in the merged windows make the merge result the
    // result window
    if (mergedWindows.erase(newWindow)) {
      isNewWindowMerged = true;
      resultWindow = mergeResult;
    }

    // if our new window is the same as a pre-existing window, nothing to do
    if (mergedWindows.empty()) {
      continue;
    }

    // pick any of the merged windows and choose that window's state window
    // as the state window for the merge result. Every merged window comes
    // from the sorted window set, whose windows always have a mapping entry.
    auto mergedStateNamespaceOpt =
        mapping_->get(key, VoidNamespace::instance(), *mergedWindows.begin());
    VELOX_CHECK(
        mergedStateNamespaceOpt.has_value(),
        "merge-state window lookup missed for window {}",
        mergedWindows.begin()->toString());
    TimeWindow mergedStateNamespace = *mergedStateNamespaceOpt;

    // figure out the state windows that we are merging
    std::vector<TimeWindow> mergedStateWindows;
    for (TimeWindow mergedWindow : mergedWindows) {
      if (auto res =
              mapping_->get(key, VoidNamespace::instance(), mergedWindow)) {
        mapping_->remove(key, VoidNamespace::instance(), mergedWindow);
        sortedWindows_.erase(mergedWindow);
        // don't put the target state window into the merged windows
        if (!(*res == mergedStateNamespace)) {
          mergedStateWindows.push_back(*res);
        }
      }
    }

    mapping_->put(
        key, VoidNamespace::instance(), mergeResult, mergedStateNamespace);
    sortedWindows_.insert(mergeResult);

    // don't merge the new window itself, it never had any state associated with
    // it i.e. if we are only merging one pre-existing window into itself
    // without extending the pre-existing window
    if (!(mergedWindows.find(mergeResult) != mergedWindows.end() &&
          mergedWindows.size() == 1)) {
      mergeFunction->merge(
          mergeResult, mergedWindows, mergedStateNamespace, mergedStateWindows);
    }
  }

  // The in-flight set of this key is a member copy of the cached snapshot;
  // write it back so the next initializeCache of the same batch (another
  // row of the same key) sees this row's windows. The mapping state carries
  // the authoritative (actual -> state) pairs across batches.
  cachedSortedWindows_.set(key, sortedWindows_);

  // the new window created a new, self-contained window without merging
  if (collector.mergeResults().empty() ||
      (resultWindow == newWindow && !isNewWindowMerged)) {
    mapping_->put(key, VoidNamespace::instance(), resultWindow, resultWindow);
    sortedWindows_.insert(resultWindow);
    cachedSortedWindows_.set(key, sortedWindows_);
  }

  return resultWindow;
}

TimeWindow MergingWindowSet::getStateWindow(
    const RowContainerStateKey& key,
    TimeWindow window) {
  // Callers pass windows that addWindow just mapped, so a miss here is a
  // bookkeeping bug, not an expected miss.
  auto result = mapping_->get(key, VoidNamespace::instance(), window);
  VELOX_CHECK(
      result.has_value(),
      "state window lookup missed for window {}",
      window.toString());
  return *result;
}

void MergingWindowSet::retireWindow(
    const RowContainerStateKey& key,
    TimeWindow window) {
  mapping_->remove(key, VoidNamespace::instance(), window);
  VELOX_CHECK(
      sortedWindows_.erase(window),
      "Window " + window.toString() + " is not in in-flight window set.");
  cachedSortedWindows_.set(key, sortedWindows_);
}

void MergingWindowSet::clearCache() {
  cachedSortedWindows_.clear();
  sortedWindows_.clear();
}

void MergingWindowSet::close() {
  clearCache();
  // Dead path by design - do not wire it back on. Nothing reaches this method
  // today: the operator tears windowFunction_ down via unique_ptr::reset
  // (destructor only, no close() call). And if this were ever called, the
  // clear() below would wipe the authoritative (actual -> state) window
  // mapping of EVERY key at once - the per-window retireWindow() is the only
  // sanctioned removal during the operator lifetime (CODE_REVIEW r1 item 4,
  // annotation-only fix; removing the call is deferred to a later round).
  mapping_->clear();
}

MergingFunction::MergingFunction(
    std::shared_ptr<DefaultAccMergingConsumer> accMergingConsumer,
    std::shared_ptr<FunctionContext<TimeWindow>> ctx,
    int64_t allowedLateness,
    bool isEventTime)
    : accMergingConsumer_(std::move(accMergingConsumer)),
      ctx_(std::move(ctx)),
      allowedLateness_(allowedLateness),
      isEventTime_(isEventTime) {}

void MergingFunction::merge(
    TimeWindow mergeResult,
    std::set<TimeWindow>& mergedWindows,
    TimeWindow stateWindowResult,
    std::vector<TimeWindow>& stateWindowsToBeMerged) {
  int64_t mergeResultMaxTs = TimeWindowUtil::toEpochMillsForTimer(
      mergeResult.maxTimestamp(), ctx_->getShiftTimeZone());
  // The merge guard kept as a fail-loud VELOX_CHECK (user gate 1 decision
  // #15): a merging window may not become earlier than the current
  // watermark - same observable behavior as Flink's
  // UnsupportedOperationException.
  VELOX_CHECK(
      !(isEventTime_ &&
        mergeResultMaxTs + allowedLateness_ <= ctx_->currentWatermark()),
      std::string(
          "The end timestamp of an "
          "event-time window cannot become earlier than the current watermark "
          "by merging. Current watermark: ") +
          std::to_string(ctx_->currentWatermark()) + std::string(" window: ") +
          mergeResult.toString());
  VELOX_CHECK(
      !(!isEventTime_ && mergeResultMaxTs <= ctx_->currentProcessingTime()),
      std::string(
          "The end timestamp of a "
          "processing-time window cannot become earlier than the current processing time "
          "by merging. Current processing time: ") +
          std::to_string(ctx_->currentProcessingTime()) +
          std::string(" window: ") + mergeResult.toString());

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
