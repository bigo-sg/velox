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

#include <optional>
#include <vector>

#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/KeyGroupPartitionFunction.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulTask.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/type/Type.h"

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

// The routing side and the state side must derive the same key group from
// the same key values (hash same-source), and the channel must be the
// contiguous-range map of that key group: channel = keyGroup * P / maxP,
// the dual of the ownership range [ceil(i*maxP/P), floor(((i+1)*maxP-1)/P)].
// The state side's key groups come from a real backend key table probed
// through a KeySelector, never recomputed in the test.
TEST_F(StreamPartitionTest, keyGroupRoutingMatchesStateOwnership) {
  constexpr uint32_t kMaxP = 128;
  constexpr int kNumPartitions = 4;
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxP, 0, kMaxP, pool());
  KeySelector selector(backend.keyIndex(), {0});

  std::vector<std::optional<int64_t>> values;
  for (int64_t v = 0; v < 256; ++v) {
    values.push_back(v);
  }
  // Null keys form their own group on the state side; the routing chain
  // must agree with that placement too. 391/32728 are the collision pair
  // of the hash fix.
  values.push_back(std::nullopt);
  values.push_back(391);
  values.push_back(32728);
  auto input = makeRowVector({"c"}, {makeNullableFlatVector<int64_t>(values)});

  selector.probe(input);
  const auto stateKeys = std::vector<RowContainerStateKey>(
      selector.keys().begin(), selector.keys().end());
  ASSERT_EQ(values.size(), stateKeys.size());

  KeyGroupPartitionFunctionSpec spec(ROW({"c"}, {BIGINT()}), {0}, kMaxP);

  // With numPartitions == maxParallelism the channel IS the key group, so
  // this pins hash same-source: the routing hash chain lands every key in
  // the key group the state side derived for it.
  auto byKeyGroup = spec.create(kMaxP, /*localExchange=*/false);
  std::vector<uint32_t> channels;
  byKeyGroup->partition(*input, channels);
  for (auto i = 0; i < values.size(); ++i) {
    EXPECT_EQ(stateKeys[i].keyGroup(), channels[i]);
  }

  // With fewer partitions the channel is the range map of the key group,
  // and the key group lies inside the owning channel's range.
  auto routed = spec.create(kNumPartitions, /*localExchange=*/false);
  routed->partition(*input, channels);
  for (auto i = 0; i < values.size(); ++i) {
    const auto keyGroup = stateKeys[i].keyGroup();
    const auto expectedChannel =
        keyGroup * static_cast<uint64_t>(kNumPartitions) / kMaxP;
    EXPECT_EQ(expectedChannel, channels[i]);
    const auto start =
        (channels[i] * static_cast<uint64_t>(kMaxP) + kNumPartitions - 1) /
        kNumPartitions;
    const auto end =
        ((channels[i] + 1) * static_cast<uint64_t>(kMaxP) - 1) / kNumPartitions;
    EXPECT_LE(start, keyGroup);
    EXPECT_LE(keyGroup, end);
  }
}

// maxParallelism need not be a multiple of numPartitions (or vice versa):
// the ranges just come out uneven, and a serde round trip of the spec
// routes identically.
TEST_F(StreamPartitionTest, keyGroupRoutingSerdeRoundTripNonDivisor) {
  constexpr uint32_t kMaxP = 10;
  constexpr int kNumPartitions = 4;
  // The spec's input type round-trips through the type serde.
  Type::registerSerDe();
  std::vector<std::optional<int64_t>> values;
  for (int64_t v = 0; v < 100; ++v) {
    values.push_back(v);
  }
  auto input = makeRowVector({"c"}, {makeNullableFlatVector<int64_t>(values)});

  KeyGroupPartitionFunctionSpec spec(ROW({"c"}, {BIGINT()}), {0}, kMaxP);
  auto roundTripped =
      KeyGroupPartitionFunctionSpec::deserialize(spec.serialize(), pool());

  std::vector<uint32_t> channels;
  spec.create(kNumPartitions, /*localExchange=*/false)
      ->partition(*input, channels);
  std::vector<uint32_t> roundTrippedChannels;
  roundTripped->create(kNumPartitions, /*localExchange=*/false)
      ->partition(*input, roundTrippedChannels);
  ASSERT_EQ(channels.size(), roundTrippedChannels.size());
  for (auto i = 0; i < channels.size(); ++i) {
    EXPECT_EQ(channels[i], roundTrippedChannels[i]);
    EXPECT_LT(channels[i], kNumPartitions);
  }
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
