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
#include "velox/experimental/stateful/agg/GroupAggregator.h"
#include <cstdint>

namespace facebook::velox::stateful {

GroupAggregator::GroupAggregator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& aggNode,
    std::unique_ptr<AggsHandleFunction> aggsFunction,
    int64_t stateRetentionTime,
    bool generateUpdateBefore)
    : exec::Operator(
          driverCtx,
          aggNode->outputType(),
          operatorId,
          aggNode->id(),
          "GroupAggregator"),
      aggsFunction_(std::move(aggsFunction)),
      stateRetentionTime_(stateRetentionTime),
      generateUpdateBefore_(generateUpdateBefore) {}

void GroupAggregator::open(StreamOperatorStateHandler* stateHandler) {
  StateDescriptor stateDesc("deduplicate-state");
  // TODO: support ttl
  // StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
  // if (ttlConfig.isEnabled()) {
  //    stateDesc.enableTimeToLive(ttlConfig);
  // }
  accState_ = stateHandler->getValueState(stateDesc);
}

RowVectorPtr GroupAggregator::processElements(int64_t key, RowVectorPtr input) {
  // TODO: not identically equal to Flink.
  bool firstRow;
  RowVectorPtr accumulators = accState_->value(key, State::VOID_NAMESPACE);
  if (!accumulators) {
    firstRow = true;
    accumulators = aggsFunction_->createAccumulators();
  } else {
    firstRow = false;
  }

  aggsFunction_->setAccumulators(accumulators);
  RowVectorPtr prevAggValue = aggsFunction_->getValue();

  aggsFunction_->accumulate(input);

  RowVectorPtr newAggValue = aggsFunction_->getValue();

  accumulators = aggsFunction_->getAccumulators();

  accState_->update(key, State::VOID_NAMESPACE, accumulators);

  return accumulators;
}

void GroupAggregator::close() {
  exec::Operator::close();
  accState_->clear();
  accState_.reset();
}
} // namespace facebook::velox::stateful
