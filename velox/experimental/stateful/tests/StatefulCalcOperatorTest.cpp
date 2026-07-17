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

#include "velox/experimental/stateful/StatefulCalcOperator.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::stateful::test {
namespace {

class SpyOperator : public exec::Operator {
 public:
  SpyOperator(
      exec::DriverCtx* driverCtx,
      std::vector<RowVectorPtr> outputs,
      const RowTypePtr& inputType,
      std::string planNodeId = "calc_spy")
      : Operator(driverCtx, inputType, 0, std::move(planNodeId), "Spy"),
        outputs_(std::move(outputs)) {}

  bool needsInput() const override {
    return nextOutput_ < outputs_.size();
  }

  void addInput(RowVectorPtr input) override {
    lastInput_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    if (nextOutput_ >= outputs_.size()) {
      return nullptr;
    }
    return outputs_[nextOutput_++];
  }

  exec::BlockingReason isBlocked(ContinueFuture*) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return nextOutput_ >= outputs_.size();
  }

  const RowVectorPtr& lastInput() const {
    return lastInput_;
  }

 private:
  std::vector<RowVectorPtr> outputs_;
  size_t nextOutput_{0};
  RowVectorPtr lastInput_;
};

class CaptureTarget : public StatefulOperator {
 public:
  explicit CaptureTarget(exec::DriverCtx* driverCtx)
      : StatefulOperator(
            std::make_unique<SpyOperator>(
                driverCtx,
                std::vector<RowVectorPtr>{},
                ROW({"c"}, {BIGINT()}),
                "capture_spy"),
            {}) {}

  void addInput(StreamElementPtr input) override {
    lastInput_ = std::move(input);
  }

  void advance() override {}

  StreamRecord* lastRecord() const {
    if (!lastInput_ || !lastInput_->isRecord()) {
      return nullptr;
    }
    return std::static_pointer_cast<StreamRecord>(lastInput_).get();
  }

 private:
  StreamElementPtr lastInput_;
};

class StatefulCalcOperatorTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{plainBatch()});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "StatefulCalcOperatorTest_task",
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
    return makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4})});
  }

  RowVectorPtr mergedBatch() {
    auto value = makeFlatVector<int64_t>({10, 20, 30, 40});
    auto rowKind = makeFlatVector<int8_t>({0, 1, 2, 3});
    return makeRowVector({"c", "$row_kind"}, {value, rowKind});
  }

  RowVectorPtr constantInsertMergedBatch() {
    auto value = makeFlatVector<int64_t>({1, 2, 3, 4});
    auto rowKind = std::make_shared<ConstantVector<int8_t>>(
        pool(), 4, false, TINYINT(), static_cast<int8_t>(RowKind::INSERT));
    return makeRowVector({"c", "$row_kind"}, {value, rowKind});
  }

  SimpleVectorPtr<int8_t> fourKindVector() {
    return makeFlatVector<int8_t>({0, 1, 2, 3});
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

TEST_F(StatefulCalcOperatorTest, testAddInputMergesRowKindAsTrailingColumn) {
  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{},
      ROW({"c", "$row_kind"}, {BIGINT(), TINYINT()}));
  auto* spyPtr = spy.get();

  StatefulCalcOperator calcOp(std::move(spy), {});

  auto rowVector = makeRowVector({makeFlatVector<int64_t>({10, 20, 30, 40})});
  calcOp.addInput(
      std::make_shared<StreamRecord>("calc", rowVector, fourKindVector()));

  ASSERT_NE(spyPtr->lastInput(), nullptr);
  EXPECT_EQ(spyPtr->lastInput()->type()->size(), 2);
  EXPECT_EQ(asRowType(spyPtr->lastInput()->type())->nameOf(1), "$row_kind");
  auto mergedRowKind = spyPtr->lastInput()->childAt(1)->asFlatVector<int8_t>();
  EXPECT_EQ(mergedRowKind->valueAt(0), 0);
  EXPECT_EQ(mergedRowKind->valueAt(1), 1);
  EXPECT_EQ(mergedRowKind->valueAt(2), 2);
  EXPECT_EQ(mergedRowKind->valueAt(3), 3);
}

TEST_F(
    StatefulCalcOperatorTest,
    testAddInputAppendsConstantRowKindForAppendOnly) {
  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{},
      ROW({"c", "$row_kind"}, {BIGINT(), TINYINT()}));
  auto* spyPtr = spy.get();

  StatefulCalcOperator calcOp(std::move(spy), {});

  calcOp.addInput(std::make_shared<StreamRecord>(
      "calc", makeRowVector({makeFlatVector<int64_t>({1, 2, 3})})));

  ASSERT_NE(spyPtr->lastInput(), nullptr);
  EXPECT_EQ(spyPtr->lastInput()->type()->size(), 2);
  EXPECT_EQ(asRowType(spyPtr->lastInput()->type())->nameOf(1), "$row_kind");
  auto trailing = spyPtr->lastInput()->childAt(1);
  EXPECT_TRUE(trailing->isConstantEncoding());
  auto constantTrailing = trailing->as<ConstantVector<int8_t>>();
  EXPECT_EQ(constantTrailing->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
}

TEST_F(StatefulCalcOperatorTest, testAdvanceSplitsMergedRowVector) {
  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{mergedBatch()},
      ROW({"c", "$row_kind"}, {BIGINT(), TINYINT()}));

  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));

  StatefulCalcOperator calcOp(std::move(spy), std::move(targets));

  calcOp.advance();

  auto* record = capturePtr->lastRecord();
  ASSERT_NE(record, nullptr);
  EXPECT_FALSE(record->appendOnly());
  ASSERT_NE(record->rowKind(), nullptr);
  EXPECT_EQ(
      record->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(
      record->rowKind()->valueAt(1),
      static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      record->rowKind()->valueAt(2),
      static_cast<int8_t>(RowKind::UPDATE_AFTER));
  EXPECT_EQ(
      record->rowKind()->valueAt(3), static_cast<int8_t>(RowKind::DELETE));
  EXPECT_EQ(record->record()->type()->size(), 1);
}

TEST_F(
    StatefulCalcOperatorTest,
    testAdvanceNormalizesConstantInsertToAppendOnly) {
  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{constantInsertMergedBatch()},
      ROW({"c", "$row_kind"}, {BIGINT(), TINYINT()}));

  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));

  StatefulCalcOperator calcOp(std::move(spy), std::move(targets));

  calcOp.advance();

  auto* record = capturePtr->lastRecord();
  ASSERT_NE(record, nullptr);
  EXPECT_TRUE(record->appendOnly());
  EXPECT_EQ(record->rowKind(), nullptr);
  EXPECT_EQ(record->record()->type()->size(), 1);
  EXPECT_EQ(record->size(), 4);
}

TEST_F(StatefulCalcOperatorTest, testAdvanceHandlesPlainRowVector) {
  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{plainBatch()},
      ROW({"c"}, {BIGINT()}));

  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));

  StatefulCalcOperator calcOp(std::move(spy), std::move(targets));

  calcOp.advance();

  auto* record = capturePtr->lastRecord();
  ASSERT_NE(record, nullptr);
  EXPECT_TRUE(record->appendOnly());
  EXPECT_EQ(record->rowKind(), nullptr);
  EXPECT_EQ(record->record()->type()->size(), 1);
  EXPECT_EQ(record->size(), 4);
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
