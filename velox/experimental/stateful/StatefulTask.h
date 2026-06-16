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

#include "velox/exec/Task.h"
#include "velox/exec/TaskStats.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/StateBackend.h"

namespace facebook::velox::stateful {

/**
 * StatefulTask is used to support streaming engines such as Flink.
 * It needs to handle state operations.
 */
class StatefulTask : public exec::Task {
  // TODO: Temporally based on Task since we want to reuse context releated
  // classes
 public:
  /// Creates a stateful task to execute a plan fragment, but doesn't start
  /// execution until StatefulTask::next() method is called.
  /// @param taskId Unique task identifier.
  /// @param planFragment Plan fragment.
  /// @param queryCtx Query context containing MemoryPool and MemoryAllocator
  /// instances to use for memory allocations during execution, executor to
  /// schedule operators on, and session properties.
  /// execution fails.
  static std::shared_ptr<StatefulTask> create(
      const std::string& taskId,
      core::PlanFragment planFragment,
      std::shared_ptr<core::QueryCtx> queryCtx);

  ~StatefulTask();

  /// Single-threaded execution API. Runs the query and returns results one
  /// batch at a time. Returns nullptr and retCode 1 if query evaluation is
  /// finished and no more data will be produced, return nullptt and retCode 0
  /// is no data produced for this batch.
  ///  Throws an exception if query execution failed.
  ///
  /// This API is available for streaming plans such as Flink.
  ///
  /// The caller is required to add all the necessary splits, and signal
  /// no-more-splits before calling 'next' for the first time.
  StreamElementPtr next(int32_t& retCode);

  StreamElementPtr next(ContinueFuture* future, int32_t& retCode);

  void notifyWatermark(int64_t watermark, int index);

  void notifyWatermark(int64_t watermark);

  void initializeState(
      const std::shared_ptr<const KeyedStateBackendParameters> params);

  void snapshotState();

  std::vector<std::string> notifyCheckpointComplete(int64_t checkpointId);

  void notifyCheckpointAborted(int64_t checkpointId);

  void init();

  // The task is finished, close all operators and reset driver
  void finish();

  // get stats for stateful task.
  exec::TaskStats statefulTaskStats();

  void addOutput(StreamElementPtr element);

 private:
  StatefulTask(
      const std::string& taskId,
      core::PlanFragment planFragment,
      std::shared_ptr<core::QueryCtx> queryCtx);

  void initOperators();

  void initStateBackend(
      const std::shared_ptr<const KeyedStateBackendParameters> parameters);

  StreamElementPtr popOutput();

  std::unique_ptr<StatefulOperator> operatorChain_;

  // A task may have multi outputs once run,
  // store them and return one by one.
  std::list<StreamElementPtr> pendings_;

  // hold the driver only to avoid it be released.
  std::shared_ptr<exec::Driver> driver;

  // The state backend used by this task.
  std::unique_ptr<StateBackend> statebackend_;
};
} // namespace facebook::velox::stateful
