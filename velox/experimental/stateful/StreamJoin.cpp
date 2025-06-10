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
#include "velox/expression/Expr.h"

#include <iostream>

namespace facebook::velox::stateful {

StreamJoin::StreamJoin(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const StreamJoinNode>& joinNode,
    std::unique_ptr<exec::Operator> leftInput,
    std::unique_ptr<exec::Operator> rightInput,
    std::unique_ptr<exec::Operator> build,
    std::unique_ptr<exec::Operator> probe)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "StreamJoin",
          std::nullopt),
      leftInput_(std::move(leftInput)),
      rightInput_(std::move(rightInput)),
      build_(static_cast<exec::NestedLoopJoinBuild*>(build.release())),
      probe_(static_cast<exec::NestedLoopJoinProbe*>(probe.release())) {
}

void StreamJoin::initialize() {
  Operator::initialize();
  leftInput_->initialize();
  rightInput_->initialize();
  build_->initialize();
  probe_->initialize();
}
  
bool StreamJoin::isFinished() {
  return leftInput_->isFinished() && rightInput_->isFinished();
}

void StreamJoin::traceInput(const RowVectorPtr& input) {
  VELOX_NYI();
}

void StreamJoin::addInput(RowVectorPtr input) {
  VELOX_NYI();
}

void StreamJoin::close() {
  leftInput_->close();
  rightInput_->close();
  build_->close();
  probe_->close();
}

RowVectorPtr StreamJoin::getOutput() {
  // TODO: use nested loop join logic to produce output now.
  // But it's not equal to flink's streaming join.
  auto leftResult = leftInput_->getOutput();
  if (leftResult) {
    build_->addInput(std::move(leftResult));
  }
  auto rightResult = rightInput_->getOutput();
  if (rightResult) {
    auto buildVector = build_->mergeDataVectors();
    probe_->addInput(std::move(rightResult));
    probe_->setBuildData(std::move(buildVector));
    auto probeOutput = probe_->getOutput();
    return probeOutput;
  }
  return nullptr;
}

} // namespace facebook::velox::stateful
