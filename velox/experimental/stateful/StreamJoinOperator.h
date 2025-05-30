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

#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"

namespace facebook::velox::stateful {

class StreamJoinOperator : public StatefulOperator {
 public:
  StreamJoinOperator(
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<exec::Operator> left,
    std::unique_ptr<exec::Operator> right);

  void initialize() override;

  void addInput(RowVectorPtr input) override {
    VELOX_NYI();
  }

  void getOutput() override;

  void close() override;

 private:
  const std::unique_ptr<exec::Operator> left_;
  const std::unique_ptr<exec::Operator> right_;
};

} // namespace facebook::velox::stateful
