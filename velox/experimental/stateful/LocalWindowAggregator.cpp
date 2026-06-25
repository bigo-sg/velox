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
#include "velox/experimental/stateful/LocalWindowAggregator.h"
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include <cstdint>

namespace facebook::velox::stateful {

LocalWindowAggregator::LocalWindowAggregator(
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<KeySelector> keySelector,
    std::unique_ptr<SliceAssigner> sliceAssigner,
    const int64_t windowInterval,
    const bool useDayLightSaving,
    RowTypePtr outputType)
    : StatefulOperator(std::move(op), std::move(targets)),
      keySelector_(std::move(keySelector)),
      sliceAssigner_(std::move(sliceAssigner)),
      windowInterval_(windowInterval),
      useDayLightSaving_(useDayLightSaving),
      outputType_(std::move(outputType)) {
  windowBuffer_ = std::make_shared<RecordsWindowBuffer>();
}

void LocalWindowAggregator::addInput(StreamElementPtr input) {
  VELOX_CHECK(!input_, "Last input has not been processed");
  auto record = std::static_pointer_cast<StreamRecord>(input);
  input_ = record->record();
}

void LocalWindowAggregator::advance() {
  if (!input_) {
    return;
  }
  // partition input by key
  std::map<int64_t, RowVectorPtr> keyToData = keySelector_->partition(input_);
  for (const auto& [key, data] : keyToData) {
    // assign slice end to data
    std::map<int64_t, RowVectorPtr> sliceEndToData = sliceAssigner_->assignSliceEnd(data);
    for (auto& [sliceEnd, data] : sliceEndToData) {
      windowBuffer_->addElement(key, sliceEnd, data);
    }
  }
  input_.reset();
}

void LocalWindowAggregator::processWatermarkInternal(int64_t timestamp) {
  if (timestamp > currentWatermark_) {
    currentWatermark_ = timestamp;
    if (currentWatermark_ >= nextTriggerWatermark_) {
      // we only need to call advanceProgress() when current watermark may
      // trigger window
      auto windowKeyToData = windowBuffer_->advanceProgress(currentWatermark_);
      auto* aggregator = op().get();
      auto* aggPool = aggregator->pool();
      int64_t windowTriggered = -1;
      for (const auto& [windowKey, datas] : windowKeyToData) {
        const int64_t window = windowKey.window();
        if (datas.empty() || currentWatermark_ < window) {
          continue;
        }
        RowVectorPtr mergedInput = (datas.size() == 1) ? datas.front() : TimeWindowUtil::mergeVectors(datas, aggPool);
        if (!mergedInput) {
          continue;
        }
        aggregator->addInput(mergedInput);
        RowVectorPtr output = aggregator->getOutput();
        if (output) {
          pushOutput(
            std::make_shared<StreamRecord>(getPlanNodeId(),
            windowKey.key(),
            std::move(addWindowEndToVector(output, window))));
          if (windowTriggered < window) {
            windowTriggered = window;
          }
        }
      }
      if (windowTriggered >= 0) {
        windowBuffer_->clear(windowTriggered);
      }
      nextTriggerWatermark_ = TimeWindowUtil::getNextTriggerWatermark(
          currentWatermark_,
          windowInterval_,
          shiftTimeZone_,
          useDayLightSaving_);
    }
  }
  pushOutput(std::make_shared<Watermark>(getPlanNodeId(), timestamp));
}

void LocalWindowAggregator::close() {
  // processWatermarkInternal(INT_MAX);
  StatefulOperator::close();
  input_.reset();
  windowBuffer_->clear();
  currentWatermark_ = 0;
  nextTriggerWatermark_ = 0;
}

RowVectorPtr LocalWindowAggregator::addWindowEndToVector(
    RowVectorPtr vector,
    int64_t sliceEnd) {
  auto newColumn = BaseVector::create(BIGINT(), vector->size(), vector->pool());
  auto windowEndCol = newColumn->as<FlatVector<int64_t>>();
  for (int i = 0; i < vector->size(); ++i) {
    windowEndCol->set(i, sliceEnd);
  }

  std::vector<VectorPtr> newChildren(vector->children());
  newChildren.push_back(newColumn);

  auto newRowVector = std::make_shared<RowVector>(
      vector->pool(),
      outputType_,
      vector->nulls(), // 保留原 nulls
      vector->size(),
      std::move(newChildren),
      vector->getNullCount());
  return newRowVector;
}
} // namespace facebook::velox::stateful
