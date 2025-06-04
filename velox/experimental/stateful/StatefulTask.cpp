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
#include "velox/experimental/stateful/StatefulPlanner.h"
#include "velox/experimental/stateful/StatefulTask.h"

#include <iostream>

namespace facebook::velox::stateful {

// static
std::shared_ptr<StatefulTask> StatefulTask::create(
    const std::string& taskId,
    core::PlanFragment planFragment,
    std::shared_ptr<core::QueryCtx> queryCtx) {
  VELOX_CHECK_NOT_NULL(planFragment.planNode);
  auto task = std::shared_ptr<StatefulTask>(new StatefulTask(
      taskId,
      std::move(planFragment),
      std::move(queryCtx)));
  task->initTaskPool();
  task->addToTaskList();
  return task;
}

StatefulTask::StatefulTask(
    const std::string& taskId,
    core::PlanFragment planFragment,
    std::shared_ptr<core::QueryCtx> queryCtx)
    : exec::Task(
          taskId,
          std::move(planFragment),
          0,
          std::move(queryCtx),
          exec::Task::ExecutionMode::kSerial,
          nullptr,
          0,
          nullptr) {
}

StatefulTask::~StatefulTask() {
  operators_.clear();
}

void StatefulTask::initOperators() {

  auto self = shared_from_this();
  // Create the operators.
  if (operators_.empty()) {
    auto driverCtx = std::make_unique<exec::DriverCtx>(self, 0, 0, -1, 0);
    driver = exec::Driver::testingCreate(std::move(driverCtx));
    StatefulPlanner::plan(planFragment(), driver->driverCtx(), operators_);

    for (const auto& op : operators_) {
      op->initialize();
    }

    if (pool()->reservedBytes() != 0) {
      VELOX_FAIL(
          "Unexpected memory pool allocations during stateful task[{}] initialization: {}",
          taskId(),
          pool()->treeMemoryUsage());
    }
  }
}

RowVectorPtr StatefulTask::next(int32_t& retCode) {
  retCode = 0;
  // initOperators();

  // Run operators one by one. If an operator has output, run its downstream operators.
  // If the last operator has output, return the output.
  // If source operator has no output, check whether it is finished.
  // If source is finished, return null and 1 for rerCode, else return null and 0 for retCode.
  // TODO: only support operators in a sequence mode.
  const auto numOperators = operators_.size();

  for (;;) {
    VELOX_CHECK_EQ(
        state(), exec::TaskState::kRunning, "Task has already finished processing.");

    for (auto i = 0; i < numOperators; ++i) {
      auto op = operators_[i].get();
      auto intermediateResult = op->getOutput();
      if (intermediateResult) {
        if (i == numOperators - 1) {
          return intermediateResult;
        } else {
          auto nextOp = operators_[i + 1].get();
          nextOp->traceInput(intermediateResult);
          nextOp->addInput(intermediateResult);
        }
      } else {
        // Source operator has no result
        if (i == 0) {
          // TODO: when source is finished, maybe other operators need to do something.
          if (op->isFinished()) {
            retCode = 1;
            finish();
          }
          return nullptr;
        } else {
          // break;
          return nullptr;
        }
      }

    }

    if (error()) {
      std::rethrow_exception(error());
    }
  }
}

void StatefulTask::finish() {
  for (auto& op : operators_) {
    op->close();
  }
  // TODO: update operator stats

  // remove operators to release memory.
  operators_.clear();
  driver.reset();
  testingFinish();
}

} // namespace facebook::velox::stateful
