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

#include "velox/experimental/stateful/StatefulSinkOperator.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::stateful::test {
namespace {

// An exec::Operator that captures the last RowVector received via addInput, so
// tests can assert exactly what StatefulSinkOperator fed to the underlying
// operator (exec::TableWriter in production).
class SpyOperator : public exec::Operator {
 public:
  explicit SpyOperator(exec::DriverCtx* driverCtx)
      : Operator(driverCtx, ROW({"c"}, {BIGINT()}), 0, "spy_sink", "SpySink") {}

  bool needsInput() const override {
    return true;
  }

  void addInput(RowVectorPtr input) override {
    captured_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture*) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }

  RowVectorPtr capturedInput() const {
    return captured_;
  }

 private:
  RowVectorPtr captured_;
};

class StatefulSinkOperatorTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{plainBatch()});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "StatefulSinkOperatorTest_task",
        std::move(planFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kParallel);
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

  RowVectorPtr plainBatch() {
    return makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  }

  // User column "c" plus a trailing $row_kind TINYINT column carrying all four
  // RowKind ordinals, as it would arrive merged across the JNI boundary.
  RowVectorPtr mergedBatch() {
    auto value = makeFlatVector<int64_t>({10, 20, 30, 40});
    auto rowKind = makeFlatVector<int8_t>({0, 1, 2, 3});
    return makeRowVector({"c", "$row_kind"}, {value, rowKind});
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

// supportsRowKind=true: a changelog StreamRecord is re-merged into a RowVector
// carrying the trailing $row_kind column before being fed to the underlying
// operator.
TEST_F(
    StatefulSinkOperatorTest,
    addInputFeedsMergedRowVectorWhenSupportsRowKind) {
  auto spy = std::make_unique<SpyOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  StatefulSinkOperator sinkOp(std::move(spy), {}, /*supportsRowKind=*/true);

  sinkOp.addInput(StreamRecord::create("sink", mergedBatch()));

  auto captured = spyPtr->capturedInput();
  ASSERT_NE(captured, nullptr);
  // User column "c" plus trailing $row_kind.
  ASSERT_EQ(captured->type()->size(), 2);
  EXPECT_EQ(
      asRowType(captured->type())->nameOf(captured->type()->size() - 1),
      "$row_kind");
  auto rowKindCol = std::dynamic_pointer_cast<const SimpleVector<int8_t>>(
      captured->childAt(captured->type()->size() - 1));
  ASSERT_NE(rowKindCol, nullptr);
  EXPECT_EQ(rowKindCol->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(
      rowKindCol->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(rowKindCol->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  EXPECT_EQ(rowKindCol->valueAt(3), static_cast<int8_t>(RowKind::DELETE));
}

// supportsRowKind=false: the record value is fed as-is; the trailing $row_kind
// column is NOT appended (the non-changelog sink path).
TEST_F(
    StatefulSinkOperatorTest,
    addInputFeedsPlainRecordWhenNotSupportsRowKind) {
  auto spy = std::make_unique<SpyOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  StatefulSinkOperator sinkOp(std::move(spy), {}, /*supportsRowKind=*/false);

  // Even though the input record carries rowKind, it is dropped on this path.
  sinkOp.addInput(StreamRecord::create("sink", mergedBatch()));

  auto captured = spyPtr->capturedInput();
  ASSERT_NE(captured, nullptr);
  // Only the user column; no trailing $row_kind.
  EXPECT_EQ(captured->type()->size(), 1);
  EXPECT_EQ(asRowType(captured->type())->nameOf(0), "c");
}

// supportsRowKind=true on an appendOnly record (no rowKind): toMergedRowVector
// appends a constant INSERT trailing column so the schema stays N+1.
TEST_F(StatefulSinkOperatorTest, addInputAppendsConstantInsertForAppendOnly) {
  auto spy = std::make_unique<SpyOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  StatefulSinkOperator sinkOp(std::move(spy), {}, /*supportsRowKind=*/true);

  sinkOp.addInput(std::make_shared<StreamRecord>("sink", plainBatch()));

  auto captured = spyPtr->capturedInput();
  ASSERT_NE(captured, nullptr);
  ASSERT_EQ(captured->type()->size(), 2);
  EXPECT_EQ(
      asRowType(captured->type())->nameOf(captured->type()->size() - 1),
      "$row_kind");
  auto rowKindCol = std::dynamic_pointer_cast<const SimpleVector<int8_t>>(
      captured->childAt(captured->type()->size() - 1));
  ASSERT_NE(rowKindCol, nullptr);
  EXPECT_EQ(rowKindCol->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(rowKindCol->valueAt(1), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(rowKindCol->valueAt(2), static_cast<int8_t>(RowKind::INSERT));
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
