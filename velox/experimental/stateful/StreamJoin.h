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

#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/join/JoinRecordStateView.h"

namespace facebook::velox::stateful {

class StreamJoin : public StatefulOperator {
 public:
  StreamJoin(
      std::unique_ptr<exec::Operator> leftInput,
      std::unique_ptr<exec::Operator> rightInput,
      std::unique_ptr<KeySelector> leftKeySelector,
      std::unique_ptr<KeySelector> rightKeySelector,
      std::unique_ptr<exec::Operator> probe,
      std::vector<std::unique_ptr<StatefulOperator>> targets);

  void initialize() override;

  void initializeState() override;

  bool isFinished() override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  std::string name() const override {
    return "StreamJoin";
  }

 protected:
  int numInputs() const override {
    return 2;
  }

 private:
  RowVectorPtr join(
      uint32_t key,
      RowVectorPtr input,
      const JoinRecordStateViewPtr& otherSideStateView,
      bool inputIsLeft);

  const std::unique_ptr<exec::Operator> leftInput_;
  const std::unique_ptr<exec::Operator> rightInput_;
  const std::unique_ptr<KeySelector> leftKeySelector_;
  const std::unique_ptr<KeySelector> rightKeySelector_;
  exec::NestedLoopJoinProbe* probe_;
  JoinRecordStateViewPtr leftRecordStateView_;
  JoinRecordStateViewPtr rightRecordStateView_;
};

} // namespace facebook::velox::stateful
