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
#include "velox/experimental/stateful/WindowAggregator.h"
#include <cstdint>
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

#include <list>

namespace facebook::velox::stateful {

WindowAggregator::WindowAggregator(
    std::unique_ptr<exec::Operator> localAggerator,
    std::unique_ptr<exec::Operator> globalAggerator,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<KeySelector> keySelector,
    std::unique_ptr<SliceAssigner> sliceAssigner,
    const int64_t windowInterval,
    const bool useDayLightSaving)
    : StatefulOperator(std::move(globalAggerator), std::move(targets)),
      localAggerator_(std::move(localAggerator)),
      keySelector_(std::move(keySelector)),
      sliceAssigner_(std::move(sliceAssigner)),
      windowInterval_(windowInterval),
      useDayLightSaving_(useDayLightSaving) {
  windowBuffer_ = std::make_shared<RecordsWindowBuffer>();
}

void WindowAggregator::initialize() {
  StatefulOperator::initialize();
  localAggerator_->initialize();
}

void WindowAggregator::initializeState() {
  StateDescriptor stateDesc("window-aggs");
  windowState_ = stateHandler()->getValueState(stateDesc);
  windowTimerService_ = stateHandler()->createTimerService(this);
}

void WindowAggregator::addInput(StreamElementPtr input) {
  VELOX_CHECK(!input_, "Last input has not been processed");
  auto record = std::static_pointer_cast<StreamRecord>(input);
  input_ = record->record();
}

void WindowAggregator::advance() {
  if (!input_) {
    return;
  }

  std::map<int64_t, RowVectorPtr> keyToData = keySelector_->partition(input_);
  for (const auto& [key, data] : keyToData) {
    std::map<int64_t, RowVectorPtr> sliceEndToData =
        sliceAssigner_->assignSliceEnd(data);
    for (const auto& [sliceEnd, data] : sliceEndToData) {
      auto windowData = data;
      if (!isEventTime) {
        windowTimerService_->registerProcessingTimeTimer(key, sliceEnd, sliceEnd);
      }

      if (isEventTime &&
          TimeWindowUtil::isWindowFired(
              sliceEnd, currentProgress_, shiftTimeZone_)) {
        // the assigned slice has been triggered, which means current element is
        // late, but maybe not need to drop
        int64_t lastWindowEnd = sliceAssigner_->getLastWindowEnd(sliceEnd);

        if (TimeWindowUtil::isWindowFired(
                lastWindowEnd, currentProgress_, shiftTimeZone_)) {
          // the last window has been triggered, so the element can be dropped
          // now
          // TODO: record dropped counter.
          continue;
        } else {
          // TODO: addElement may have data output.
          windowBuffer_->addElement(
              key, sliceStateMergeTarget(sliceEnd), windowData);
          // we need to register a timer for the next unfired window,
          // because this may the first time we see elements under the key
          int64_t unfiredFirstWindow = sliceEnd;
          while (TimeWindowUtil::isWindowFired(
              unfiredFirstWindow, currentProgress_, shiftTimeZone_)) {
            unfiredFirstWindow += windowInterval_;
          }
          windowTimerService_->registerEventTimeTimer(
              key, unfiredFirstWindow, unfiredFirstWindow - 1);
        }
      } else {
        // the assigned slice hasn't been triggered, accumulate into the
        // assigned slice
        windowBuffer_->addElement(key, sliceEnd, windowData);
      }
    }
  }

  input_.reset();
}

void WindowAggregator::processWatermarkInternal(int64_t timestamp) {
  if (isEventTime && timestamp > currentProgress_) {
    currentProgress_ = timestamp;
    if (currentProgress_ >= nextTriggerWatermark_) {
      // we only need to call advanceProgress() when current watermark may
      // trigger window
      auto windowKeyToData = windowBuffer_->advanceProgress(currentProgress_);
      for (const auto& [windowKey, datas] : windowKeyToData) {
        if (datas.empty()) {
          continue;
        }
        // TODO: agg should output no matter how many rows in data.
        localAggerator_->addInput(
            TimeWindowUtil::mergeVectors(datas, op()->pool()));
        RowVectorPtr localAcc = localAggerator_->getOutput();
        auto stateAcc =
            windowState_->value(windowKey.key(), windowKey.window());
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
      nextTriggerWatermark_ = TimeWindowUtil::getNextTriggerWatermark(
          currentProgress_,
          windowInterval_,
          shiftTimeZone_,
          useDayLightSaving_);
    }
  }
}

void WindowAggregator::onEventTime(
    std::shared_ptr<TimerHeapInternalTimer<int64_t, int64_t>> timer) {
  auto output = windowState_->value(timer->key(), timer->ns());
  windowState_->remove(timer->key(), timer->ns());
  pushOutput(
      std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
}

int64_t WindowAggregator::sliceStateMergeTarget(int64_t sliceToMerge) {
  // TODO: implement it
  return sliceToMerge;
}

/// Add window_start / window_end timestamp to output
RowVectorPtr addWindowTimestampToOutput(
  const RowVectorPtr& output,
  const std::string& fieldName,
  const int64_t fieldValue,
  const int32_t fieldIndex) {
  /// TODO: implement it
  VELOX_NYI();
}

void WindowAggregator::onTimer(std::shared_ptr<TimerHeapInternalTimer<int64_t, int64_t>> timer) {
  fireWindow(timer->key(), timer->timestamp(), timer->ns());
  clearWindow(timer->key(), timer->timestamp(), timer->ns());
}

template<typename K>
void WindowAggregator::fireWindow(const K& key, int64_t timerTimestamp, int64_t windowEnd) {
  RowVectorPtr output = windowState_->value(key, windowEnd);
  if (!output) {
    LOG(INFO) << "No output found for key: " << key << ", window end: " << windowEnd;
    return;
  } else {
    if (windowStartIndex_ >= 0) {
      output = addWindowTimestampToOutput(
        output,
        "window_start",
        windowEnd - windowInterval_,
        windowStartIndex_);
    }
    if (windowEndIndex_ >= 0) {
      output = addWindowTimestampToOutput(
        output,
      "window_end",
      windowEnd,
      windowEndIndex_);
    }
  }
  if (output) {
    pushOutput(std::make_shared<StreamRecord>(getPlanNodeId(), std::move(output)));
  }
}

template<typename K>
void WindowAggregator::clearWindow(const K& key, int64_t timerTimestamp, int64_t windowEnd) {
  windowState_->remove(key, windowEnd);
}

void WindowAggregator::onProcessingTime(std::shared_ptr<TimerHeapInternalTimer<int64_t, int64_t>> timer) {
  if (timer->timestamp() > lastTriggeredProcessingTime_) {
    lastTriggeredProcessingTime_ = timer->timestamp();
    auto windowKeyToData = windowBuffer_->advanceProgress(timer->timestamp());
    for (auto& [windowKey, datas] : windowKeyToData) {
      if (datas.empty()) {
        continue;
      }
      // std::list<RowVectorPtr> allDatas(datas.begin(), datas.end());
      auto stateAcc = windowState_->value(windowKey.key(), windowKey.window());
      if (stateAcc) {
        datas.push_back(stateAcc);
      }
      RowVectorPtr opInput = TimeWindowUtil::mergeVectors(datas, op()->pool());
      op()->addInput(opInput);
      auto newAcc = op()->getOutput();
      if (newAcc) {
        windowState_->update(windowKey.key(), windowKey.window(), newAcc);
      }
    }
    if (!windowKeyToData.empty()) {
      windowBuffer_->clear();
    }
  }
  onTimer(timer);
}

void WindowAggregator::close() {
  processWatermarkInternal(INT_MAX);
  StatefulOperator::close();
  if (localAggerator_) {
    localAggerator_->close();
  }
  input_.reset();
  windowTimerService_->close();
  windowBuffer_->clear();
  windowState_->clear();
  currentProgress_ = 0;
  nextTriggerWatermark_ = 0;
}

} // namespace facebook::velox::stateful
