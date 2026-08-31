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

#include "velox/experimental/stateful/StreamPartition.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulTask.h"
#include "velox/experimental/stateful/StreamElement.h"

namespace facebook::velox::stateful::test {
namespace {

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

// Assigns row i to partition (i % numPartitions). Used to get a deterministic
// split across partitions without depending on hash internals.
class RowModPartitionFunction : public core::PartitionFunction {
 public:
  explicit RowModPartitionFunction(int numPartitions)
      : numPartitions_(numPartitions) {}

  std::optional<uint32_t> partition(
      const RowVector& input,
      std::vector<uint32_t>& partitions) override {
    auto size = input.size();
    partitions.resize(size);
    for (uint32_t i = 0; i < size; ++i) {
      partitions[i] = i % numPartitions_;
    }
    return std::nullopt;
  }

 private:
  const int numPartitions_;
};

class RowModPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions,
      bool /*localExchange*/) const override {
    return std::make_unique<RowModPartitionFunction>(numPartitions);
  }

  std::string toString() const override {
    return "ROW_MOD";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "RowModPartitionFunctionSpec";
    return obj;
  }

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& /*obj*/,
      void* /*context*/) {
    return std::make_shared<RowModPartitionFunctionSpec>();
  }
};

class StreamPartitionTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{plainBatch()});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = StatefulTask::create(
        "StreamPartitionTest_task",
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

  RowVectorPtr plainBatch() {
    return makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<StatefulTask> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

TEST_F(StreamPartitionTest, preservesAppendOnlyOutputs) {
  RowModPartitionFunctionSpec spec;
  StreamPartition partitionOp(
      std::make_unique<NoOutputOperator>(driverCtx_.get()), spec, 2);

  auto value = makeFlatVector<int64_t>({10, 20, 30, 40});
  auto input =
      std::make_shared<StreamRecord>("source", makeRowVector({"c"}, {value}));

  partitionOp.addInput(input);
  partitionOp.advance();

  int32_t retCode;
  auto r0 = std::static_pointer_cast<StreamRecord>(task_->next(retCode));
  auto r1 = std::static_pointer_cast<StreamRecord>(task_->next(retCode));
  ASSERT_NE(r0, nullptr);
  ASSERT_NE(r1, nullptr);
  EXPECT_TRUE(r0->appendOnly());
  EXPECT_EQ(r0->rowKind(), nullptr);
  EXPECT_TRUE(r1->appendOnly());
  EXPECT_EQ(r1->rowKind(), nullptr);
}

TEST_F(StreamPartitionTest, carriesRowKindAcrossPartitions) {
  RowModPartitionFunctionSpec spec;
  StreamPartition partitionOp(
      std::make_unique<NoOutputOperator>(driverCtx_.get()), spec, 2);

  auto value = makeFlatVector<int64_t>({10, 20, 30, 40});
  auto rowKind = makeFlatVector<int8_t>({0, 1, 2, 3});
  auto input = std::make_shared<StreamRecord>(
      "source",
      makeRowVector({"c"}, {value}),
      std::dynamic_pointer_cast<SimpleVector<int8_t>>(rowKind));

  partitionOp.addInput(input);
  partitionOp.advance();

  int32_t retCode;
  auto r0 = std::static_pointer_cast<StreamRecord>(task_->next(retCode));
  auto r1 = std::static_pointer_cast<StreamRecord>(task_->next(retCode));
  ASSERT_NE(r0, nullptr);
  ASSERT_NE(r1, nullptr);
  ASSERT_NE(r0->rowKind(), nullptr);
  EXPECT_EQ(r0->size(), 2);
  EXPECT_EQ(r0->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(
      r0->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  ASSERT_NE(r1->rowKind(), nullptr);
  EXPECT_EQ(r1->size(), 2);
  EXPECT_EQ(
      r1->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(r1->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::DELETE));
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
