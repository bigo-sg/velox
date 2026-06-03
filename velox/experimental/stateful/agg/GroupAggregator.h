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
#pragma once
#include <cstdint>

#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/agg/AggsHandleFunction.h"
#include "velox/experimental/stateful/functions/KeyedProcessFunction.h"

namespace facebook::velox::stateful {

/// This class is relevant to Flink GroupAggFunction.
class GroupAggregator : public exec::Operator, public KeyedProcessFunction {
 public:
  GroupAggregator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::PlanNode>& aggNode,
      std::unique_ptr<AggsHandleFunction> aggsFunction,
      int64_t stateRetentionTime,
      bool generateUpdateBefore);

  bool needsInput() const override {
    VELOX_NYI();
  }

  void addInput(RowVectorPtr input) override {
    VELOX_NYI();
  }

  RowVectorPtr getOutput() override {
    VELOX_NYI();
  }

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    VELOX_NYI();
  }

  bool isFinished() override {
    return false;
  }

  void open(StreamOperatorStateHandler* stateHandler) override;

  RowVectorPtr processElements(int64_t key, RowVectorPtr input) override;

  void close() override;

 private:
  std::unique_ptr<AggsHandleFunction> aggsFunction_;
  std::shared_ptr<ValueState<int64_t, int64_t, RowVectorPtr>> accState_;
  int64_t stateRetentionTime_;
  bool generateUpdateBefore_;
};

} // namespace facebook::velox::stateful
