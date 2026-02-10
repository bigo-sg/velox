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

#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/CombinedWatermarkStatus.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/experimental/stateful/state/StreamOperatorStateHandler.h"

namespace facebook::velox::stateful {

class StatefulOperator {
 public:
  StatefulOperator(
      std::unique_ptr<exec::Operator> op,
      std::vector<std::unique_ptr<StatefulOperator>> targets,
      std::shared_ptr<const KeyedStateBackendParameters>
          keyedStateBackendParameters = nullptr)
      : keyedStateBackendParameters_(keyedStateBackendParameters),
        operator_(std::move(op)),
        targets_(std::move(targets)) {
    sink = operator_->operatorType() == "TableWrite";
  }

  virtual void initialize();

  virtual bool isFinished();

  virtual void addInput(StreamElementPtr input);

  virtual void advance();

  bool sourceEmpty();

  virtual void close();

  virtual void processWatermark(int64_t timestamp, int index);

  virtual void processWatermark(int64_t timestamp);

  virtual void initializeState() {}

  void initializeStateBackend(StateBackend* stateBackend);

  void snapshotState();

  std::vector<std::string> notifyCheckpointComplete(int64_t checkpointId);

  void notifyCheckpointAborted(int64_t checkpointId);

  StreamOperatorStateHandlerPtr stateHandler() const {
    return stateHandler_;
  }

  memory::MemoryPool* memoryPool() {
    return operator_->pool();
  }

  const std::string detail() const {
    std::stringstream stream;
    stream << "StatefulOperator: " << name() << "\n";
    for (size_t i = 0; i < targets_.size(); ++i) {
      stream << "\tTarget " << i << ": " << targets_[i]->detail() << "\n";
    }
    return stream.str();
  }

  virtual std::string name() const {
    return operator_->operatorType();
  }

  std::string getPlanNodeId() const {
    return operator_->planNodeId();
  }

  std::unique_ptr<exec::Operator>& op() {
    return operator_;
  }

  std::vector<std::unique_ptr<StatefulOperator>>& targets() {
    return targets_;
  }

 protected:
  void pushOutput(StreamElementPtr output);
  void emitWatermark(int64_t timestamp);

  virtual int numInputs() const {
    return 1;
  }

  std::shared_ptr<const KeyedStateBackendParameters>
      keyedStateBackendParameters_;

 private:
  bool isSink() {
    return sink;
  }

  std::unique_ptr<exec::Operator> operator_;
  std::vector<std::unique_ptr<StatefulOperator>> targets_;
  bool sink;
  bool sourceEmpty_ = true;
  std::unique_ptr<CombinedWatermarkStatus> combinedWatermarkStatus_;
  StreamOperatorStateHandlerPtr stateHandler_;
};

using StatefulOperatorPtr = std::unique_ptr<StatefulOperator>;

} // namespace facebook::velox::stateful
