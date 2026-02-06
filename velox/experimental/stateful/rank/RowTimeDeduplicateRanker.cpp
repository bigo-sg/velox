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
#include "velox/experimental/stateful/rank/RowTimeDeduplicateRanker.h"
#include <cstdint>
#include "velox/vector/DictionaryVector.h"

namespace facebook::velox::stateful {

RowTimeDeduplicateRanker::RowTimeDeduplicateRanker(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& rankNode,
    int64_t minRetentionTime,
    int rowtimeIndex,
    bool generateUpdateBefore,
    bool generateInsert,
    bool keepLastRow)
    : exec::Operator(
          driverCtx,
          rankNode->outputType(),
          operatorId,
          rankNode->id(),
          "RowTimeDeduplicateRanker"),
      minRetentionTime_(minRetentionTime),
      rowtimeIndex_(rowtimeIndex),
      generateUpdateBefore_(generateUpdateBefore),
      generateInsert_(generateInsert),
      keepLastRow_(keepLastRow) {}

void RowTimeDeduplicateRanker::open(StreamOperatorStateHandler* stateHandler) {
  StateDescriptor stateDesc("deduplicate-state");
  // TODO: support ttl
  // StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
  // if (ttlConfig.isEnabled()) {
  //    stateDesc.enableTimeToLive(ttlConfig);
  // }
  state_ = stateHandler->getValueState(stateDesc);
}

RowVectorPtr RowTimeDeduplicateRanker::processElements(
    uint32_t key,
    RowVectorPtr input) {
  // TODO: not identically equal to Flink. Flink may generate output each row,
  // but we only generate output once a batch.
  RowVectorPtr preRow = state_->value(key, State::VOID_NAMESPACE);
  RowVectorPtr output = nullptr;
  int index = 0;
  int outputIndex = -1;
  if (!preRow) {
    preRow = std::dynamic_pointer_cast<RowVector>(input->slice(0, 1));
    index = 1;
    outputIndex = 0;
  }
  auto preTimestamp = preRow->childAt(rowtimeIndex_)
                          ->as<DictionaryVector<Timestamp>>()
                          ->valueAt(0);
  auto tsVector =
      input->childAt(rowtimeIndex_)->as<DictionaryVector<Timestamp>>();
  for (auto i = index; i < input->size(); ++i) {
    auto timestamp = tsVector->valueAt(i);
    if ((keepLastRow_ && timestamp >= preTimestamp) ||
        (!keepLastRow_ && timestamp < preTimestamp)) {
      // TODO: support update before and insert.
      outputIndex = i;
    }
  }
  if (outputIndex >= 0) {
    RowVectorPtr output =
        std::dynamic_pointer_cast<RowVector>(input->slice(outputIndex, 1));
    state_->update(key, State::VOID_NAMESPACE, output);
  }
  return output;
}

void RowTimeDeduplicateRanker::close() {
  exec::Operator::close();
  state_->clear();
  state_.reset();
}
} // namespace facebook::velox::stateful
