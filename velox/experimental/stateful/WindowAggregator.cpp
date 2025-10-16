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
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/experimental/stateful/WindowAggregator.h"
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include <list>
#include <memory>

namespace facebook::velox::stateful {

WindowAggregator::WindowAggregator(
    std::unique_ptr<exec::Operator> localAggerator,
    std::unique_ptr<exec::Operator> globalAggerator,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<KeySelector> keySelector,
    std::unique_ptr<SliceAssigner> sliceAssigner,
    const long windowInterval,
    const bool useDayLightSaving,
    const bool isEventTime,
    const int windowStartIndex,
    const int windowEndIndex)
    : StatefulOperator(std::move(globalAggerator), std::move(targets)),
      localAggerator_(std::move(localAggerator)),
      keySelector_(std::move(keySelector)),
      sliceAssigner_(std::move(sliceAssigner)),
      windowInterval_(windowInterval),
      useDayLightSaving_(useDayLightSaving),
      isEventTime_(isEventTime),
      windowStartIndex_(windowStartIndex),
      windowEndIndex_(windowEndIndex) {
        windowBuffer_ = std::make_shared<RecordsWindowBuffer>();
    }

void WindowAggregator::initialize() {
  StatefulOperator::initialize();
  if (localAggerator_) {
    localAggerator_->initialize();
  }

  StateDescriptor stateDesc("window-aggs");
  windowState_ = stateHandler()->getValueState(stateDesc);
  windowTimerService_ = stateHandler()->createTimerService(this);
}

void WindowAggregator::addInput(RowVectorPtr input) {
  VELOX_CHECK(!input_, "Last input has not been processed");
  input_ = input;
}

void WindowAggregator::getOutput() {
  if (!input_) {
    return;
  }

  std::map<uint64_t, RowVectorPtr> keyToData = keySelector_->partition(input_);
  for (const auto& [key, data] : keyToData) {
    std::map<uint64_t, RowVectorPtr> sliceEndToData =
        sliceAssigner_->assignSliceEnd(data);
    for (const auto& [sliceEnd, data] : sliceEndToData) {
      auto windowData = data;
      if (!isEventTime_) {
        windowTimerService_->registerProcessingTimeTimer(key, sliceEnd, sliceEnd);
      }

      if (isEventTime_ && TimeWindowUtil::isWindowFired(sliceEnd, currentProgress_, shiftTimeZone_)) {
        // the assigned slice has been triggered, which means current element is late,
        // but maybe not need to drop
        long lastWindowEnd = sliceAssigner_->getLastWindowEnd(sliceEnd);
        if (TimeWindowUtil::isWindowFired(lastWindowEnd, currentProgress_, shiftTimeZone_)) {
            // the last window has been triggered, so the element can be dropped now
            // TODO: record dropped counter.
            continue;
        } else {
            // TODO: addElement may have data output.
            windowBuffer_->addElement(key, sliceStateMergeTarget(sliceEnd), windowData);
            // we need to register a timer for the next unfired window,
            // because this may the first time we see elements under the key
            long unfiredFirstWindow = sliceEnd;
            while (TimeWindowUtil::isWindowFired(unfiredFirstWindow, currentProgress_, shiftTimeZone_)) {
                unfiredFirstWindow += windowInterval_;
            }
            windowTimerService_->registerEventTimeTimer(key, unfiredFirstWindow, unfiredFirstWindow - 1);
        }
      } else {
          // the assigned slice hasn't been triggered, accumulate into the assigned slice
          windowBuffer_->addElement(key, sliceEnd, windowData);
      }
    }
  }

  input_.reset();
}

void WindowAggregator::processWatermarkInternal(long timestamp) {
  if (isEventTime_ && timestamp > currentProgress_) {
    currentProgress_ = timestamp;
    if (currentProgress_ >= nextTriggerWatermark_) {
      // we only need to call advanceProgress() when current watermark may trigger window
      auto windowKeyToData = windowBuffer_->advanceProgress(currentProgress_);
      for (const auto&[windowKey, datas] : windowKeyToData) {
        if (datas.empty()) {
          continue;
        }
        // TODO: agg should output no matter how many rows in datas.
        localAggerator_->addInput(TimeWindowUtil::mergeVectors(datas, op()->pool()));
        RowVectorPtr localAcc = localAggerator_->getOutput(); 
        auto stateAcc = windowState_->value(windowKey.key(), windowKey.window());
        std::list<RowVectorPtr> allDatas;
        if (!localAcc && !stateAcc) {
          continue;
        } else {
          if (localAcc) {
            allDatas.push_back(localAcc);
          }
          if (stateAcc) {
            allDatas.push_back(stateAcc);
          }
          op()->addInput(TimeWindowUtil::mergeVectors(allDatas, op()->pool()));
          auto newAcc = op()->getOutput();
          if (newAcc) {
            windowState_->update(windowKey.key(), windowKey.window(), newAcc);
          }
        }
      }
      windowTimerService_->advanceWatermark(currentProgress_);
      nextTriggerWatermark_ =
          TimeWindowUtil::getNextTriggerWatermark(
              currentProgress_, windowInterval_, shiftTimeZone_, useDayLightSaving_);
    }
  }
}

/// Add window_start / window_end timestamp to output
RowVectorPtr addWindowTimestampToOutput(
  const RowVectorPtr& output,
  const std::string& fieldName,
  const TypePtr& fieldType,
  const long fieldValue,
  const int fieldIndex) {
  auto createTimestampVector = [&](
    const Timestamp& val,
    const size_t size,
    velox::memory::MemoryPool* pool) -> VectorPtr {
      const TypePtr windowStartType = std::make_shared<const TimestampType>();
      VectorPtr windowStartVec = BaseVector::create(windowStartType, output->size(), output->pool());
      FlatVector<Timestamp>* timestampVector = windowStartVec->asFlatVector<Timestamp>();
      for (size_t i = 0; i < size; ++i) {
        timestampVector->set(i, val);
      }
      return windowStartVec;
  };
  const TypePtr& outputType = output->type();
  const RowTypePtr& outputRowType = std::dynamic_pointer_cast<const RowType>(outputType);
  const std::vector<std::string>& outputFieldNames = outputRowType->names();
  const std::vector<TypePtr>& outputFieldTypes = outputRowType->children();
  const std::vector<VectorPtr>& outputFields = output->children();
  std::vector<std::string> newOutputFieldNames;
  std::vector<TypePtr> newOutputFieldTypes;
  std::vector<VectorPtr> newOutputFields;
  VectorPtr windowStartVec = createTimestampVector(Timestamp::fromMillis(fieldValue), output->size(), output->pool());
  for (int i = 0; i < fieldIndex; ++i) {
    newOutputFieldTypes.emplace_back(outputFieldTypes[i]);
    newOutputFieldNames.emplace_back(outputFieldNames[i]);
    newOutputFields.emplace_back(outputFields[i]);
  }
  newOutputFieldTypes.emplace_back(fieldType);
  newOutputFieldNames.emplace_back(fieldName);
  newOutputFields.emplace_back(windowStartVec);
  for (int i = fieldIndex + 1; i < output->childrenSize() + 1; ++i) {
    newOutputFieldTypes.emplace_back(outputFieldTypes[i-1]);
    newOutputFieldNames.emplace_back(outputFieldNames[i-1]);
    newOutputFields.emplace_back(outputFields[i-1]);
  }
  auto newOutputRowType = std::make_shared<const RowType>(std::move(newOutputFieldNames), std::move(newOutputFieldTypes));
  return std::make_shared<RowVector>(output->pool(),
    newOutputRowType,
    output->nulls(),
    output->size(),
    newOutputFields,
    output->getNullCount()
  );
}

void WindowAggregator::onTimer(std::shared_ptr<TimerHeapInternalTimer<uint32_t, long>> timer) {
  fireWindow(timer->key(), timer->timestamp(), timer->ns());
  clearWindow(timer->key(), timer->timestamp(), timer->ns());
}

template<typename K>
void WindowAggregator::fireWindow(K key, long timerTimestamp, long windowEnd) {
  RowVectorPtr output = windowState_->value(key, windowEnd);
  if (output) {
    if (windowStartIndex_ >= 0) {
      output = addWindowTimestampToOutput(
        output,
        "window_start",
        std::make_shared<const TimestampType>(),
        windowEnd - windowInterval_,
        windowStartIndex_);
    }
    if (windowEndIndex_ >= 0) {
      output = addWindowTimestampToOutput(
        output,
      "window_end",
      std::make_shared<const TimestampType>(),
      windowEnd,
      windowEndIndex_);
    }
  }
  if (output) {
    pushOutput(output);
  }
}

template<typename K>
void WindowAggregator::clearWindow(K key, long timerTimestamp, long windowEnd) {
  windowState_->remove(key, windowEnd);
}

void WindowAggregator::onEventTime(std::shared_ptr<TimerHeapInternalTimer<uint32_t, long>> timer) {
  onTimer(timer);
}

void WindowAggregator::onProcessingTime(std::shared_ptr<TimerHeapInternalTimer<uint32_t, long>> timer) {
  if (timer->timestamp() >= lastTriggeredProcessingTime_) {
    lastTriggeredProcessingTime_ = timer->timestamp();
    auto windowKeyToData = windowBuffer_->advanceProgress(timer->timestamp());
    for (const auto& [windowKey, datas] : windowKeyToData) {
      if (datas.empty()) {
        continue;
      }
      std::list<RowVectorPtr> allDatas;
      for (const auto& data: datas) {
        allDatas.push_back(data);
      }
      auto stateAcc = windowState_->value(windowKey.key(), windowKey.window());
      if (stateAcc) {
        allDatas.push_back(stateAcc);
      }
      RowVectorPtr opInput = TimeWindowUtil::mergeVectors(allDatas, op()->pool());
      op()->addInput(opInput);
      auto newAcc = op()->getOutput();
      if (newAcc) {
        windowState_->update(windowKey.key(), windowKey.window(), newAcc);
      }
    }
    windowBuffer_->clear();
    onTimer(timer);
  }
}

long WindowAggregator::sliceStateMergeTarget(long sliceToMerge) {
  // TODO: implement it
  return sliceToMerge;
}

void WindowAggregator::close() {
  processWatermarkInternal(INT_MAX);
  StatefulOperator::close();
  if (localAggerator_) {
    localAggerator_->close();
  }
  input_.reset();
  windowBuffer_->clear();
  windowState_->clear();
  currentProgress_ = 0;
  nextTriggerWatermark_ = 0;
}

} // namespace facebook::velox::stateful
