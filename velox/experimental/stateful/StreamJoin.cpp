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

#include <iostream>

namespace facebook::velox::stateful {

StreamJoin::StreamJoin(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const StreamJoinNode>& joinNode,
    std::unique_ptr<exec::Operator> left,
    std::unique_ptr<exec::Operator> right)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "StreamJoin",
          std::nullopt),
      left_(std::move(left)),
      right_(std::move(right)) {
}

void StreamJoin::initialize() {
  left_->initialize();
  right_->initialize();
}
  
bool StreamJoin::isFinished() {
  return left_->isFinished() && right_->isFinished();
}

void StreamJoin::traceInput(const RowVectorPtr& input) {
  VELOX_NYI();
}

void StreamJoin::addInput(RowVectorPtr input) {
  VELOX_NYI();
}

void StreamJoin::close() {
  left_->close();
  right_->close();
}

RowVectorPtr StreamJoin::getOutput() {
  auto leftResult = left_->getOutput();
  if (leftResult) {
      // TODO: build hash side;
  }
  auto rightResult = right_->getOutput();
  if (rightResult) {
      // TODO: build probe side;
  }
  // TODO: implement it
  return nullptr;
}

} // namespace facebook::velox::stateful
