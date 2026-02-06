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
#include "velox/experimental/stateful/window/GroupWindowAggsHandler.h"

namespace facebook::velox::stateful {
GroupWindowAggsHandler::GroupWindowAggsHandler(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& groupAggNode)
    : exec::Operator(
          driverCtx,
          groupAggNode->outputType(),
          operatorId,
          groupAggNode->id(),
          "GroupWindowAggsHandler") {}

void GroupWindowAggsHandler::addInput(RowVectorPtr input) {
  VELOX_NYI();
}

RowVectorPtr GroupWindowAggsHandler::getOutput() {
  VELOX_NYI();
}

bool GroupWindowAggsHandler::isFinished() {
  return false;
}

void GroupWindowAggsHandler::open() {}

void GroupWindowAggsHandler::setAccumulators(
    TimeWindow ns,
    RowVectorPtr accumulators) {}

void GroupWindowAggsHandler::accumulate(RowVectorPtr inputRow) {}

void GroupWindowAggsHandler::retract(RowVectorPtr inputRow) {}

void GroupWindowAggsHandler::merge(TimeWindow ns, RowVectorPtr otherAcc) {}

RowVectorPtr GroupWindowAggsHandler::createAccumulators() {
  return RowVector::createEmpty(outputType_, pool());
}

RowVectorPtr GroupWindowAggsHandler::getAccumulators() {
  return RowVector::createEmpty(outputType_, pool());
}

void GroupWindowAggsHandler::cleanup(TimeWindow ns) {}

void GroupWindowAggsHandler::close() {}

RowVectorPtr GroupWindowAggsHandler::getValue(TimeWindow ns) {
  return nullptr;
}

} // namespace facebook::velox::stateful
