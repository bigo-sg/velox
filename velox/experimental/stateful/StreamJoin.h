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

#include "velox/exec/NestedLoopJoinBuild.h"
#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"

namespace facebook::velox::stateful {

class StreamJoin : public exec::Operator {
 public:
  StreamJoin( 
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const StreamJoinNode>& joinNode,
    std::unique_ptr<exec::Operator> leftInput,
    std::unique_ptr<exec::Operator> rightInput,
    std::unique_ptr<exec::Operator> build,
    std::unique_ptr<exec::Operator> probe);

  void initialize() override;

  bool needsInput() const override {
    VELOX_NYI();
  }

  bool isFinished() override;

  void traceInput(const RowVectorPtr& input) override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  void noMoreInput() override {
    VELOX_NYI();
  }

  exec::BlockingReason isBlocked(ContinueFuture* future) override {
    VELOX_NYI();
  }

  void close() override;

 private:
  const std::unique_ptr<exec::Operator> leftInput_;
  const std::unique_ptr<exec::Operator> rightInput_;
  const std::unique_ptr<exec::NestedLoopJoinBuild> build_;
  const std::unique_ptr<exec::NestedLoopJoinProbe> probe_;
};

} // namespace facebook::velox::stateful
