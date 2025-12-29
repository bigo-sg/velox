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
#include "velox/experimental/stateful/StreamJoin.h"
#include "velox/experimental/stateful/join/JoinRecordStateViews.h"

namespace facebook::velox::stateful {

StreamJoin::StreamJoin(
    std::unique_ptr<exec::Operator> leftInput,
    std::unique_ptr<exec::Operator> rightInput,
    std::unique_ptr<KeySelector> leftKeySelector,
    std::unique_ptr<KeySelector> rightKeySelector,
    std::unique_ptr<exec::Operator> probe,
    std::vector<std::unique_ptr<StatefulOperator>> targets)
    : StatefulOperator(std::move(probe), std::move(targets)),
      leftInput_(std::move(leftInput)),
      rightInput_(std::move(rightInput)),
      leftKeySelector_(std::move(leftKeySelector)),
      rightKeySelector_(std::move(rightKeySelector)),
      probe_(static_cast<exec::NestedLoopJoinProbe*>(op().get())) {
}

void StreamJoin::initialize() {
  StatefulOperator::initialize();
  leftInput_->initialize();
  rightInput_->initialize();
  leftRecordStateView_ = JoinRecordStateViews::create(
      stateHandler().get(),
      "left-records",
      0);
  rightRecordStateView_ = JoinRecordStateViews::create(
      stateHandler().get(),
      "right-records",
      0);
}

bool StreamJoin::isFinished() {
  return leftInput_->isFinished() && rightInput_->isFinished();
}

void StreamJoin::addInput(RowVectorPtr input) {
  VELOX_NYI();
}

void StreamJoin::close() {
  StatefulOperator::close();
  leftInput_->close();
  rightInput_->close();
  leftRecordStateView_->close();
  rightRecordStateView_->close();
}

void StreamJoin::getOutput() {
  // TODO: use nested loop join logic to produce output now.
  // But it's not equal to flink's streaming join.
  auto leftResult = leftInput_->getOutput();
  if (leftResult) {
    auto keyToData = leftKeySelector_->partition(leftResult);
    for (auto & [key, data] : keyToData) {
      leftRecordStateView_->addRecord(key, data);
      auto result = join(key, data, rightRecordStateView_, true);
      pushOutput(result);
    }
  }

  auto rightResult = rightInput_->getOutput();
  if (rightResult) {
    auto keyToData = rightKeySelector_->partition(rightResult);
    for (auto & [key, data] : keyToData) {
      rightRecordStateView_->addRecord(key, data);
      auto result = join(key, data, leftRecordStateView_, false);
      pushOutput(result);
    }
  }
}

RowVectorPtr StreamJoin::join(
    uint32_t key,
    RowVectorPtr input,
    const JoinRecordStateViewPtr& otherSideStateView,
    bool inputIsLeft) {
  auto states = otherSideStateView->records(key);
  if (states.empty()) {
    return nullptr;
  }
  std::vector<RowVectorPtr> buildVector;
  for (const auto& state : states) {
    buildVector.push_back(state.first);
  }
  if (inputIsLeft) {
    probe_->addInput(std::move(input));
    probe_->setBuildData(std::move(buildVector));
  } else {
    // TODO: support another side.
    //probe_->addInput(std::move(buildVector));
    //probe_->setBuildData(std::move(input));
  }
  auto result = probe_->getOutput();
  return result;
}

} // namespace facebook::velox::stateful
