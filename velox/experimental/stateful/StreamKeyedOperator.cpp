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
#include "velox/experimental/stateful/StreamKeyedOperator.h"

namespace facebook::velox::stateful {

StreamKeyedOperator::StreamKeyedOperator(
    std::unique_ptr<exec::Operator> processor,
    std::unique_ptr<KeySelector> keySelector,
    std::vector<std::unique_ptr<StatefulOperator>> targets)
    : StatefulOperator(std::move(processor), std::move(targets)),
      keySelector_(std::move(keySelector)),
      name_(op()->operatorType()) {
  processor_ = dynamic_cast<KeyedProcessFunction*>(op().get());
}

void StreamKeyedOperator::initialize() {
  StatefulOperator::initialize();
  processor_->open(stateHandler().get());
}

bool StreamKeyedOperator::isFinished() {
  return false;
}

void StreamKeyedOperator::addInput(RowVectorPtr input) {
  VELOX_CHECK_NULL(input_);
  input_ = std::move(input);
}

void StreamKeyedOperator::close() {
  StatefulOperator::close();
}

void StreamKeyedOperator::getOutput() {
  auto keyToData = keySelector_->partition(input_);
  for (auto & [key, data] : keyToData) {
    auto result = processor_->processElements(key, data);
    if (result) {
      pushOutput(result);
    }
  }
  input_.reset();
}

} // namespace facebook::velox::stateful
