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

#include "velox/experimental/stateful/StatefulOperator.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/StatefulTask.h"

namespace facebook::velox::stateful::test {
namespace {

/// An exec::Operator double with no data-plane behavior that records every
/// checkpoint-plane call (snapshot, restore, commit, abort) and answers with
/// canned records, so the StatefulOperator walk can be observed without any
/// connector or split machinery.
class CheckpointSpyOperator : public exec::Operator {
 public:
  CheckpointSpyOperator(
      exec::DriverCtx* driverCtx,
      std::string planNodeId,
      std::vector<std::string> snapshotRecords)
      : Operator(
            driverCtx,
            ROW({"c"}, {BIGINT()}),
            0,
            std::move(planNodeId),
            "CheckpointSpy"),
        snapshotRecords_(std::move(snapshotRecords)) {}

  bool needsInput() const override {
    return true;
  }

  void addInput(RowVectorPtr) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture*) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }

  std::vector<std::string> snapshotState(int64_t checkpointId) override {
    snapshotCalls_.push_back(checkpointId);
    return snapshotRecords_;
  }

  void restoreState(
      const std::vector<std::string>& checkpointRecords) override {
    restoredRecords_.insert(
        restoredRecords_.end(),
        checkpointRecords.begin(),
        checkpointRecords.end());
  }

  std::vector<std::string> commit(int64_t id) override {
    commitCalls_.push_back(id);
    return {planNodeId() + "-committed"};
  }

  void abortCheckpoint(int64_t checkpointId) override {
    abortCalls_.push_back(checkpointId);
  }

  const std::vector<int64_t>& snapshotCalls() const {
    return snapshotCalls_;
  }

  const std::vector<int64_t>& commitCalls() const {
    return commitCalls_;
  }

  const std::vector<int64_t>& abortCalls() const {
    return abortCalls_;
  }

  const std::vector<std::string>& restoredRecords() const {
    return restoredRecords_;
  }

 private:
  std::vector<std::string> snapshotRecords_;
  std::vector<int64_t> snapshotCalls_;
  std::vector<int64_t> commitCalls_;
  std::vector<int64_t> abortCalls_;
  std::vector<std::string> restoredRecords_;
};

/// A three-level chain of plain StatefulOperators over spy operators:
/// root -> middle -> leaf, one spy per level, so the checkpoint walk can be
/// asserted at every depth.
struct SpyChain {
  std::unique_ptr<StatefulOperator> root;
  CheckpointSpyOperator* rootSpy;
  CheckpointSpyOperator* middleSpy;
  CheckpointSpyOperator* leafSpy;
};

class StatefulOperatorCheckpointTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"},
        std::vector<RowVectorPtr>{
            makeRowVector({makeFlatVector<int64_t>({1})})});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = StatefulTask::create(
        "StatefulOperatorCheckpointTest_task",
        std::move(planFragment),
        core::QueryCtx::create(executor_.get()));
    driver_ = exec::Driver::testingCreate();
    driverCtx_ = std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0);
    driverCtx_->driver = driver_.get();
  }

  void TearDown() override {
    driverCtx_.reset();
    driver_.reset();
    task_.reset();
    executor_.reset();
    OperatorTestBase::TearDown();
  }

  SpyChain makeChain() {
    auto leafSpy = std::make_unique<CheckpointSpyOperator>(
        driverCtx_.get(), "leaf", std::vector<std::string>{"leaf-record"});
    auto* leafSpyPtr = leafSpy.get();
    auto leaf = std::make_unique<StatefulOperator>(
        std::move(leafSpy), std::vector<std::unique_ptr<StatefulOperator>>());

    auto middleSpy = std::make_unique<CheckpointSpyOperator>(
        driverCtx_.get(), "middle", std::vector<std::string>{"middle-record"});
    auto* middleSpyPtr = middleSpy.get();
    std::vector<std::unique_ptr<StatefulOperator>> middleTargets;
    middleTargets.push_back(std::move(leaf));
    auto middle = std::make_unique<StatefulOperator>(
        std::move(middleSpy), std::move(middleTargets));

    auto rootSpy = std::make_unique<CheckpointSpyOperator>(
        driverCtx_.get(), "root", std::vector<std::string>{"root-record"});
    auto* rootSpyPtr = rootSpy.get();
    std::vector<std::unique_ptr<StatefulOperator>> rootTargets;
    rootTargets.push_back(std::move(middle));
    auto root = std::make_unique<StatefulOperator>(
        std::move(rootSpy), std::move(rootTargets));

    return SpyChain{std::move(root), rootSpyPtr, middleSpyPtr, leafSpyPtr};
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<StatefulTask> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

TEST_F(StatefulOperatorCheckpointTest, snapshotWalkCollectsEveryOperator) {
  auto chain = makeChain();

  // The walk must collect the wrapped operator's records at every depth:
  // before the legacy-handler guard was narrowed, the walk returned early
  // when no handler was set and dropped every non-root record.
  EXPECT_EQ(
      (std::vector<std::string>{"root-record", "middle-record", "leaf-record"}),
      chain.root->snapshotState(7));
  EXPECT_EQ((std::vector<int64_t>{7}), chain.rootSpy->snapshotCalls());
  EXPECT_EQ((std::vector<int64_t>{7}), chain.middleSpy->snapshotCalls());
  EXPECT_EQ((std::vector<int64_t>{7}), chain.leafSpy->snapshotCalls());
}

TEST_F(StatefulOperatorCheckpointTest, completeWalkCommitsEveryOperator) {
  auto chain = makeChain();

  // The commit notification must reach the wrapped operator at every depth
  // (sinks commit through it), with or without a legacy state handler.
  EXPECT_EQ(
      (std::vector<std::string>{
          "root-committed", "middle-committed", "leaf-committed"}),
      chain.root->notifyCheckpointComplete(9));
  EXPECT_EQ((std::vector<int64_t>{9}), chain.rootSpy->commitCalls());
  EXPECT_EQ((std::vector<int64_t>{9}), chain.middleSpy->commitCalls());
  EXPECT_EQ((std::vector<int64_t>{9}), chain.leafSpy->commitCalls());
}

TEST_F(StatefulOperatorCheckpointTest, abortWalkReachesEveryOperator) {
  auto chain = makeChain();

  chain.root->notifyCheckpointAborted(11);

  EXPECT_EQ((std::vector<int64_t>{11}), chain.rootSpy->abortCalls());
  EXPECT_EQ((std::vector<int64_t>{11}), chain.middleSpy->abortCalls());
  EXPECT_EQ((std::vector<int64_t>{11}), chain.leafSpy->abortCalls());
}

TEST_F(StatefulOperatorCheckpointTest, restoreStillDeliversToEveryOperator) {
  auto chain = makeChain();

  // Restore keeps its broadcast semantics: every operator in the chain
  // receives every checkpoint record.
  chain.root->restoreState({"record-1", "record-2"});

  EXPECT_EQ(
      (std::vector<std::string>{"record-1", "record-2"}),
      chain.rootSpy->restoredRecords());
  EXPECT_EQ(
      (std::vector<std::string>{"record-1", "record-2"}),
      chain.middleSpy->restoredRecords());
  EXPECT_EQ(
      (std::vector<std::string>{"record-1", "record-2"}),
      chain.leafSpy->restoredRecords());
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
