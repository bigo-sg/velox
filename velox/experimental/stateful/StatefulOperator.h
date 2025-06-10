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

#include "velox/exec/Operator.h"

namespace facebook::velox::stateful {

class StatefulOperator {
 public:
  StatefulOperator(
      std::unique_ptr<exec::Operator> op,
      std::vector<std::unique_ptr<StatefulOperator>> targets)
      : operator_(std::move(op)),
        targets_(std::move(targets)) {
    sink = operator_->operatorType() == "TableWrite";
  }

  virtual void initialize();

  virtual bool isFinished();

  virtual void addInput(RowVectorPtr input);

  virtual void getOutput();

  bool sourceEmpty();

  virtual void close();

  virtual void processWatermark(long timestamp, int index);

 protected:
  void pushOutput(RowVectorPtr output);
  void pushWatermark(long timestamp, int index);

  std::unique_ptr<exec::Operator>& op() {
    return operator_;
  }

 private:
  bool isSink() {
    return sink;
  }

  std::unique_ptr<exec::Operator> operator_;
  std::vector<std::unique_ptr<StatefulOperator>> targets_;
  bool sink;
  bool sourceEmpty_ = true;
};

using StatefulOperatorPtr = std::unique_ptr<StatefulOperator>;

} // namespace facebook::velox::stateful
