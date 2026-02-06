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
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

LocalWindowAggregator::LocalWindowAggregator(
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<KeySelector> keySelector,
    std::unique_ptr<KeySelector> sliceAssigner,
    const long windowInterval,
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

void LocalWindowAggregator::addInput(RowVectorPtr input) {
  VELOX_CHECK(!input_, "Last input has not been processed");
  input_ = input;
}

void LocalWindowAggregator::getOutput() {
  if (!input_) {
    return;
  }

  std::map<uint32_t, RowVectorPtr> keyToData = keySelector_->partition(input_);
  for (const auto& [key, data] : keyToData) {
    std::map<uint32_t, RowVectorPtr> sliceEndToData =
        sliceAssigner_->partition(data);
    for (const auto& [sliceEnd, data] : sliceEndToData) {
      // TODO: addElement may have data output.
      auto windowData = data;
      windowBuffer_->addElement(key, sliceEnd, windowData);
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
      for (const auto& [windowKey, datas] : windowKeyToData) {
        if (datas.empty()) {
          continue;
        }
        op()->addInput(TimeWindowUtil::mergeVectors(datas, op()->pool()));
        RowVectorPtr output = op()->getOutput();
        if (output) {
          pushOutput(
              addWindowEndToVector(std::move(output), windowKey.window()));
        }
      }
      nextTriggerWatermark_ = TimeWindowUtil::getNextTriggerWatermark(
          currentWatermark_,
          windowInterval_,
          shiftTimeZone_,
          useDayLightSaving_);
    }
  }
}

void LocalWindowAggregator::close() {
  processWatermarkInternal(INT_MAX);
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
