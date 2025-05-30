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
#include "velox/experimental/stateful/StreamJoinOperator.h"

#include <iostream>

namespace facebook::velox::stateful {

StreamJoinOperator::StreamJoinOperator(
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<exec::Operator> left,
    std::unique_ptr<exec::Operator> right)
    : StatefulOperator(
          std::move(op),
          std::move(targets)),
      left_(std::move(left)),
      right_(std::move(right)) {
}

void StreamJoinOperator::initialize() {
  left_->initialize();
  right_->initialize();
  StatefulOperator::initialize();
}

void StreamJoinOperator::close() {
  left_->close();
  right_->close();
  StatefulOperator::close();
}

void StreamJoinOperator::getOutput() {
  auto leftResult = left_->getOutput();
  if (leftResult) {
      // TODO: build hash side;
      op()->traceInput(leftResult);
      op()->addInput(std::move(leftResult));
  }
  auto rightResult = right_->getOutput();
  if (rightResult) {
      // TODO: build probe side;
      op()->traceInput(rightResult);
      op()->addInput(std::move(rightResult));
  }
  // TODO: implement it
  StatefulOperator::getOutput();
}

} // namespace facebook::velox::stateful
