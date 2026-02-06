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

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/functions/KeyedProcessFunction.h"

namespace facebook::velox::stateful {

/// This class is relevant to Flink AppendOnlyTopNFunction.
class AppendOnlyTopNRanker : public exec::Operator,
                             public KeyedProcessFunction {
 public:
  AppendOnlyTopNRanker(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::PlanNode>& rankNode,
      std::unique_ptr<exec::Operator> veloxRanker,
      std::shared_ptr<KeySelector> sortKeySelector,
      // RankType rankType,
      // RankRange rankRange,
      bool generateUpdateBefore,
      bool outputRankNumber,
      long cacheSize);

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

  void initialize() override;

  void close() override;

  void open(StreamOperatorStateHandler* stateHandler) override;

  RowVectorPtr processElements(uint32_t key, RowVectorPtr input) override;

 private:
  std::unique_ptr<exec::Operator> veloxRanker_;
  std::shared_ptr<KeySelector> sortKeySelector_;
  std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>> dataState_;
  // RankType rankType_;
  // RankRange rankRange_;
  bool generateUpdateBefore_;
  bool outputRankNumber_;
  bool cacheSize_;
};

} // namespace facebook::velox::stateful
