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

#include "velox/experimental/stateful/window/Window.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::stateful {

/// This is relavent to flink generated NamespaceAggsHandleFunction.
/// We make it an Operator to be able to use it in a StatefulOperator.
class GroupWindowAggsHandler : public exec::Operator {
 public:
  GroupWindowAggsHandler(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::PlanNode>& groupAggNode);

  bool needsInput() const override {
    VELOX_NYI();
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    VELOX_NYI();
  }

  bool isFinished() override;

  void open();

  void setAccumulators(TimeWindow ns, RowVectorPtr accumulators);

  void accumulate(RowVectorPtr inputRow);

  void retract(RowVectorPtr inputRow);

  void merge(TimeWindow ns, RowVectorPtr otherAcc);

  RowVectorPtr createAccumulators();

  RowVectorPtr getAccumulators();

  void cleanup(TimeWindow ns);

  void close();

  RowVectorPtr getValue(TimeWindow ns);

 private:
};
} // namespace facebook::velox::stateful
