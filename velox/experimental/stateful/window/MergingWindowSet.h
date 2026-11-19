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
#pragma once
#include <cstdint>

#include <folly/container/EvictingCacheMap.h>
#include <map>
#include <set>
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/experimental/stateful/window/GroupWindowAssigner.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/experimental/stateful/window/WindowProcessFunction.h"

namespace facebook::velox::stateful {

class MergingFunction;
class DefaultAccMergingConsumer;

/// Hash / equality over the key row views for the in-flight window cache:
/// value identity through the StateKey interface (the cached views stay
/// valid within the batch that produced them; clearCache runs at the batch
/// boundary).
struct RowContainerStateKeyHash {
  size_t operator()(const RowContainerStateKey& key) const {
    return std::hash<uint64_t>()(key.hash());
  }
};

struct RowContainerStateKeyEqual {
  bool operator()(
      const RowContainerStateKey& lhs,
      const RowContainerStateKey& rhs) const {
    return lhs.equals(rhs);
  }
};

// This class is relevant to Flink MergingWindowSet. Keyed by the typed
// RowContainerStateKey (row views into the backend's key table) over the
// injected session-window mapping state.
class MergingWindowSet {
 public:
  MergingWindowSet(
      std::shared_ptr<MergingWindowAssigner> windowAssigner,
      std::shared_ptr<SessionWindowMappingState> mapping);

  void initializeCache(const RowContainerStateKey& key);

  TimeWindow addWindow(
      const RowContainerStateKey& key,
      TimeWindow newWindow,
      std::shared_ptr<MergingFunction> mergeFunction);

  void retireWindow(const RowContainerStateKey& key, TimeWindow window);

  TimeWindow getStateWindow(const RowContainerStateKey& key, TimeWindow window);

  /// Drops the in-flight window cache. Called at the batch boundary: cached
  /// keys are row views whose rows may be reclaimed once the key's last
  /// state entry is gone, so views must not survive the batch that produced
  /// them (the mapping state reloads on the next initializeCache).
  void clearCache();

  void close();

 private:
  static const int MAPPING_CACHE_SIZE = 10000;
  std::shared_ptr<MergingWindowAssigner> windowAssigner_;
  std::shared_ptr<SessionWindowMappingState> mapping_;

  folly::EvictingCacheMap<
      RowContainerStateKey,
      std::set<TimeWindow>,
      /*Hash=*/RowContainerStateKeyHash,
      /*KeyEqual=*/RowContainerStateKeyEqual>
      cachedSortedWindows_;
  std::set<TimeWindow> sortedWindows_;
};

// This class is relevant to Flink MergingFunctionImpl in
// MergingWindowProcessFunction.
class MergingFunction {
 public:
  MergingFunction(
      std::shared_ptr<DefaultAccMergingConsumer> accMergingConsumer,
      std::shared_ptr<FunctionContext<TimeWindow>> ctx,
      int64_t allowedLateness,
      bool isEventTime);

  void merge(
      TimeWindow mergeResult,
      std::set<TimeWindow>& mergedWindows,
      TimeWindow stateWindowResult,
      std::vector<TimeWindow>& stateWindowsToBeMerged);

 private:
  std::shared_ptr<DefaultAccMergingConsumer> accMergingConsumer_;
  std::shared_ptr<FunctionContext<TimeWindow>> ctx_;
  int64_t allowedLateness_;
  bool isEventTime_;
};

} // namespace facebook::velox::stateful
