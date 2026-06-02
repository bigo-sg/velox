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
#include "velox/experimental/stateful/functions/KeyedProcessFunction.h"

namespace facebook::velox::stateful {

/// This class is relevant to Flink RowTimeDeduplicateFunction.
class RowTimeDeduplicateRanker : public exec::Operator,
                                 public KeyedProcessFunction {
 public:
  RowTimeDeduplicateRanker(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::PlanNode>& rankNode,
      int64_t minRetentionTime,
      int rowtimeIndex,
      bool generateUpdateBefore,
      bool generateInsert,
      bool keepLastRow);

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

  RowVectorPtr processElements(uint32_t key, RowVectorPtr input) override;

  void close() override;

 private:
  std::shared_ptr<ValueState<int64_t, int64_t, RowVectorPtr>> state_;
  int64_t minRetentionTime_;
  int rowtimeIndex_;
  bool generateUpdateBefore_;
  bool generateInsert_;
  bool keepLastRow_;
};

} // namespace facebook::velox::stateful
