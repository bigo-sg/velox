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

#include "velox/experimental/stateful/WatermarkAssigner.h"
#include "velox/experimental/stateful/WatermarkGenerator.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StatefulPlanner.h"
#include "velox/experimental/stateful/WatermarkSource.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::velox::stateful::test {
namespace {

class TimestampProjectionOperator : public exec::Operator {
 public:
  explicit TimestampProjectionOperator(exec::DriverCtx* driverCtx)
      : Operator(
            driverCtx,
            ROW({"timestamp"}, {BIGINT()}),
            0,
            "timestamp_projection",
            "TimestampProjection") {}

  bool needsInput() const override {
    return !input_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    VELOX_CHECK_NOT_NULL(input_);
    auto output = std::make_shared<RowVector>(
        input_->pool(),
        outputType_,
        nullptr,
        input_->size(),
        std::vector<VectorPtr>{input_->childAt(0)});
    input_.reset();
    return output;
  }

  exec::BlockingReason isBlocked(ContinueFuture*) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }

 private:
  RowVectorPtr input_;
};

class QueuedOutputOperator : public exec::Operator {
 public:
  QueuedOutputOperator(
      exec::DriverCtx* driverCtx,
      std::vector<RowVectorPtr> outputs,
      std::string planNodeId = "queued_output")
      : Operator(
            driverCtx,
            ROW({"timestamp"}, {BIGINT()}),
            0,
            std::move(planNodeId),
            "QueuedOutput"),
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
            ROW({"timestamp"}, {BIGINT()}),
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

class SpyStatefulOperator : public StatefulOperator {
 public:
  explicit SpyStatefulOperator(exec::DriverCtx* driverCtx)
      : StatefulOperator(
            std::make_unique<NoOutputOperator>(driverCtx),
            {}) {}

  void addInput(StreamElementPtr input) override {
    if (input->isRecord()) {
      ++numRecords_;
      events_.push_back("record");

      auto record = std::static_pointer_cast<StreamRecord>(input);
      recordSizes_.push_back(record->record()->size());
    }
  }

  void advance() override {}

  void processWatermark(int64_t timestamp) override {
    events_.push_back("watermark");
    watermarks_.push_back(timestamp);
  }

  size_t numRecords() const {
    return numRecords_;
  }

  const std::vector<int64_t>& watermarks() const {
    return watermarks_;
  }

  const std::vector<std::string>& events() const {
    return events_;
  }

  const std::vector<vector_size_t>& recordSizes() const {
    return recordSizes_;
  }

 private:
  size_t numRecords_{0};
  std::vector<std::string> events_;
  std::vector<vector_size_t> recordSizes_;
  std::vector<int64_t> watermarks_;
};

class WatermarkGeneratorTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{batch({0})});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "WatermarkGeneratorTest_task",
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

  RowVectorPtr batch(const std::vector<int64_t>& timestamps) {
    return makeRowVector({makeFlatVector<int64_t>(timestamps)});
  }

  RowVectorPtr nullableBatch(
      const std::vector<std::optional<int64_t>>& timestamps) {
    return makeRowVector({makeNullableFlatVector<int64_t>(timestamps)});
  }

  WatermarkGenerator makeGenerator(int64_t watermarkInterval) {
    return WatermarkGenerator(
        std::make_unique<TimestampProjectionOperator>(driverCtx_.get()),
        0,
        0,
        watermarkInterval);
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

// Verifies that WatermarkGenerator only emits after max event time crosses
// lastWatermark + watermarkInterval.
TEST_F(WatermarkGeneratorTest, emitsOnlyWhenIntervalIsCrossed) {
  auto generator = makeGenerator(1'000);

  EXPECT_EQ(std::nullopt, generator.generate(batch({100, 200})));
  EXPECT_EQ(200, generator.currentWatermark());
  EXPECT_EQ(0, generator.lastWatermark());

  EXPECT_EQ(std::nullopt, generator.generate(batch({800})));
  EXPECT_EQ(800, generator.currentWatermark());
  EXPECT_EQ(0, generator.lastWatermark());

  EXPECT_EQ(1001, generator.generate(batch({1001})));
  EXPECT_EQ(1001, generator.currentWatermark());
  EXPECT_EQ(1001, generator.lastWatermark());
}

// Verifies that a previously emitted watermark is not emitted again for
// later batches that do not cross the next interval.
TEST_F(WatermarkGeneratorTest, doesNotRepeatPreviousWatermark) {
  auto generator = makeGenerator(1'000);

  EXPECT_EQ(1'500, generator.generate(batch({1'500})));
  EXPECT_EQ(std::nullopt, generator.generate(batch({1'200})));
  EXPECT_EQ(std::nullopt, generator.generate(batch({1'900})));
  EXPECT_EQ(2'501, generator.generate(batch({2'501})));
}

// Verifies that when one batch crosses multiple intervals, generate() returns
// the last watermark emitted from that batch.
TEST_F(WatermarkGeneratorTest, returnsLastWatermarkEmittedWithinBatch) {
  auto generator = makeGenerator(1'000);

  EXPECT_EQ(4'000, generator.generate(batch({1'100, 2'500, 4'000})));
  EXPECT_EQ(4'000, generator.currentWatermark());
  EXPECT_EQ(4'000, generator.lastWatermark());
}

// Verifies that null rowtime values are rejected before watermark extraction.
TEST_F(WatermarkGeneratorTest, rejectsNullRowtime) {
  auto generator = makeGenerator(1'000);

  VELOX_ASSERT_THROW(
      generator.generate(nullableBatch({100, std::nullopt, 2'000})),
      "RowTime field should not have nulls");
}

// Verifies planner fail-fast behavior for TableScanNodeWithWatermark without
// a WatermarkPushDownSpec.
TEST_F(WatermarkGeneratorTest, plannerRejectsTableScanWithNullWatermarkSpec) {
  auto tableHandle =
      std::make_shared<connector::ConnectorTableHandle>("test_connector");
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments = {{"timestamp", std::make_shared<connector::ColumnHandle>()}};
  std::shared_ptr<WatermarkPushDownSpec> nullWatermarkSpec;
  auto tableScan = std::make_shared<TableScanNodeWithWatermark>(
      "scan",
      ROW({"timestamp"}, {BIGINT()}),
      tableHandle,
      assignments,
      nullWatermarkSpec);

  core::PlanFragment planFragment;
  planFragment.planNode =
      std::make_shared<StatefulPlanNode>(tableScan, std::vector<core::PlanNodePtr>{});

  VELOX_ASSERT_THROW(
      StatefulPlanner::plan(planFragment, driverCtx_.get(), nullptr),
      "TableScanNodeWithWatermark requires watermarkPushDownSpec");
}

// Verifies WatermarkSource forwards watermarks only when WatermarkGenerator
// reports a newly generated watermark.
TEST_F(WatermarkGeneratorTest, watermarkSourceEmitsOnlyGeneratedWatermarks) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(spy));

  WatermarkSource watermarkSource(
      std::make_unique<QueuedOutputOperator>(
          driverCtx_.get(),
          std::vector<RowVectorPtr>{
              batch({100, 200}),
              batch({1001}),
              batch({1200}),
              batch({2501})}),
      std::move(targets),
      std::make_unique<WatermarkGenerator>(
          std::make_unique<TimestampProjectionOperator>(driverCtx_.get()),
          0,
          0,
          1000));

  watermarkSource.advance();
  EXPECT_EQ(1, spyPtr->numRecords());
  EXPECT_TRUE(spyPtr->watermarks().empty());

  watermarkSource.advance();
  EXPECT_EQ(2, spyPtr->numRecords());
  EXPECT_EQ((std::vector<int64_t>{1001}), spyPtr->watermarks());

  watermarkSource.advance();
  EXPECT_EQ(3, spyPtr->numRecords());
  EXPECT_EQ((std::vector<int64_t>{1001}), spyPtr->watermarks());

  watermarkSource.advance();
  EXPECT_EQ(4, spyPtr->numRecords());
  EXPECT_EQ((std::vector<int64_t>{1001, 2501}), spyPtr->watermarks());
}

TEST_F(
    WatermarkGeneratorTest,
    watermarkAssignerEmitsWatermarksWithRecordSlices) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(spy));

  WatermarkAssigner watermarkAssigner(
      std::make_unique<TimestampProjectionOperator>(driverCtx_.get()),
      std::move(targets),
      0,
      0,
      1000);

  watermarkAssigner.addInput(
      std::make_shared<StreamRecord>("input", batch({100, 1001, 1200, 2501})));
  watermarkAssigner.advance();

  EXPECT_EQ(
      (std::vector<std::string>{
          "record", "watermark", "record", "watermark"}),
      spyPtr->events());
  EXPECT_EQ((std::vector<vector_size_t>{2, 2}), spyPtr->recordSizes());
  EXPECT_EQ((std::vector<int64_t>{1001, 2501}), spyPtr->watermarks());
}

TEST_F(WatermarkGeneratorTest, watermarkAssignerDoesNotRepeatWatermark) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(spy));

  WatermarkAssigner watermarkAssigner(
      std::make_unique<TimestampProjectionOperator>(driverCtx_.get()),
      std::move(targets),
      0,
      0,
      1000);

  watermarkAssigner.addInput(
      std::make_shared<StreamRecord>("input", batch({100, 200})));
  watermarkAssigner.advance();
  watermarkAssigner.addInput(
      std::make_shared<StreamRecord>("input", batch({1001})));
  watermarkAssigner.advance();
  watermarkAssigner.addInput(
      std::make_shared<StreamRecord>("input", batch({1200})));
  watermarkAssigner.advance();

  EXPECT_EQ(
      (std::vector<std::string>{"record", "record", "watermark", "record"}),
      spyPtr->events());
  EXPECT_EQ((std::vector<vector_size_t>{2, 1, 1}), spyPtr->recordSizes());
  EXPECT_EQ((std::vector<int64_t>{1001}), spyPtr->watermarks());
}

TEST_F(WatermarkGeneratorTest, watermarkAssignerRejectsNullRowtime) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(spy));

  WatermarkAssigner watermarkAssigner(
      std::make_unique<TimestampProjectionOperator>(driverCtx_.get()),
      std::move(targets),
      0,
      0,
      1000);

  watermarkAssigner.addInput(std::make_shared<StreamRecord>(
      "input", nullableBatch({100, std::nullopt, 2000})));
  VELOX_ASSERT_THROW(
      watermarkAssigner.advance(), "RowTime field should not have nulls");
}

TEST_F(WatermarkGeneratorTest, watermarkSourceRejectsNullGenerator) {
  EXPECT_THROW(
      WatermarkSource(
          std::make_unique<QueuedOutputOperator>(
              driverCtx_.get(), std::vector<RowVectorPtr>{}),
          {},
          nullptr),
      VeloxRuntimeError);
}

TEST_F(WatermarkGeneratorTest, watermarkSourceTracksSourceEmpty) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(spy));

  WatermarkSource watermarkSource(
      std::make_unique<QueuedOutputOperator>(
          driverCtx_.get(), std::vector<RowVectorPtr>{batch({100})}),
      std::move(targets),
      std::make_unique<WatermarkGenerator>(
          std::make_unique<TimestampProjectionOperator>(driverCtx_.get()),
          0,
          0,
          1000));

  EXPECT_TRUE(watermarkSource.sourceEmpty());

  watermarkSource.advance();
  EXPECT_FALSE(watermarkSource.sourceEmpty());

  watermarkSource.advance();
  EXPECT_TRUE(watermarkSource.sourceEmpty());
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
