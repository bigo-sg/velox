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

#include "velox/experimental/stateful/StatefulSourceOperator.h"

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

namespace facebook::velox::stateful::test {
namespace {

class PresetOutputOperator : public exec::Operator {
 public:
  PresetOutputOperator(
      exec::DriverCtx* driverCtx,
      std::vector<RowVectorPtr> outputs,
      std::string planNodeId = "preset_output")
      : Operator(
            driverCtx,
            outputs.empty() ? ROW({"c"}, {BIGINT()})
                            : asRowType(outputs[0]->type()),
            0,
            std::move(planNodeId),
            "PresetOutput"),
        outputs_(std::move(outputs)) {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr) override {}

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

 private:
  std::vector<RowVectorPtr> outputs_;
  size_t nextOutput_{0};
};

class NoOutputOperator : public exec::Operator {
 public:
  explicit NoOutputOperator(exec::DriverCtx* driverCtx)
      : Operator(
            driverCtx,
            ROW({"c"}, {BIGINT()}),
            0,
            "spy_target",
            "SpyTarget") {}

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
};

class CaptureTarget : public StatefulOperator {
 public:
  explicit CaptureTarget(exec::DriverCtx* driverCtx)
      : StatefulOperator(std::make_unique<NoOutputOperator>(driverCtx), {}) {}

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

class StatefulSourceOperatorTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{plainBatch()});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "StatefulSourceOperatorTest_task",
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

TEST_F(StatefulSourceOperatorTest, rejectsAddInput) {
  StatefulSourceOperator sourceOp(
      std::make_unique<PresetOutputOperator>(
          driverCtx_.get(), std::vector<RowVectorPtr>{plainBatch()}),
      {});

  VELOX_ASSERT_THROW(
      sourceOp.addInput(std::make_shared<StreamRecord>("source", plainBatch())),
      "StatefulSourceOperator does not support addInput");
}

TEST_F(StatefulSourceOperatorTest, advanceWrapsPlainRowVectorAsAppendOnly) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));

  StatefulSourceOperator sourceOp(
      std::make_unique<PresetOutputOperator>(
          driverCtx_.get(), std::vector<RowVectorPtr>{plainBatch()}),
      std::move(targets));

  EXPECT_TRUE(sourceOp.sourceEmpty());
  sourceOp.advance();
  EXPECT_FALSE(sourceOp.sourceEmpty());

  auto* record = capturePtr->lastRecord();
  ASSERT_NE(record, nullptr);
  EXPECT_TRUE(record->appendOnly());
  EXPECT_EQ(record->rowKind(), nullptr);
  EXPECT_EQ(record->size(), 3);
}

TEST_F(StatefulSourceOperatorTest, advanceSplitsMergedRowVector) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));

  StatefulSourceOperator sourceOp(
      std::make_unique<PresetOutputOperator>(
          driverCtx_.get(), std::vector<RowVectorPtr>{mergedBatch()}),
      std::move(targets));

  sourceOp.advance();

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

TEST_F(StatefulSourceOperatorTest, advanceTracksSourceEmpty) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get());
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));

  StatefulSourceOperator sourceOp(
      std::make_unique<PresetOutputOperator>(
          driverCtx_.get(), std::vector<RowVectorPtr>{plainBatch()}),
      std::move(targets));

  EXPECT_TRUE(sourceOp.sourceEmpty());
  sourceOp.advance();
  EXPECT_FALSE(sourceOp.sourceEmpty());
  sourceOp.advance();
  EXPECT_TRUE(sourceOp.sourceEmpty());
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
