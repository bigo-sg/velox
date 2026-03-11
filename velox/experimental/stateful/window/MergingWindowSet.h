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
#include "velox/experimental/stateful/window/GroupWindowAssigner.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/experimental/stateful/window/WindowProcessFunction.h"

namespace facebook::velox::stateful {

class MergingFunction;
class DefaultAccMergingConsumer;

// This class is relevant to Flink MergingWindowSet.
class MergingWindowSet {
 public:
  MergingWindowSet(
      std::shared_ptr<MergingWindowAssigner> windowAssigner,
      std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>> mapping);

  void initializeCache(uint32_t key);

  TimeWindow addWindow(
      uint32_t key,
      TimeWindow newWindow,
      std::shared_ptr<MergingFunction> mergeFunction);

  void retireWindow(uint32_t key, TimeWindow window);

  TimeWindow getStateWindow(uint32_t key, TimeWindow window);

  void close();

 private:
  static const int MAPPING_CACHE_SIZE = 10000;
  std::shared_ptr<MergingWindowAssigner> windowAssigner_;
  std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>> mapping_;

  folly::EvictingCacheMap<uint32_t, std::set<TimeWindow>> cachedSortedWindows_;
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
