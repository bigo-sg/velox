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
#include "velox/experimental/stateful/rank/DeduplicateRanker.h"
#include "velox/experimental/stateful/rank/StreamDeduplicateOperator.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/Driver.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StatefulTask.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/HashMapStateBackend.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::stateful::test {
namespace {

/// A minimal concrete exec::Operator used only as the wrapped operator of a
/// CaptureTarget; it has no data-plane behavior.
class DummyOperator : public exec::Operator {
 public:
  DummyOperator(exec::DriverCtx* driverCtx, std::string planNodeId)
      : Operator(
            driverCtx,
            ROW({"c"}, {BIGINT()}),
            0,
            std::move(planNodeId),
            "Dummy") {}

  bool needsInput() const override {
    return false;
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

/// A StatefulOperator that records the last StreamRecord it receives, used to
/// capture the deduplicate output for assertions.
class CaptureTarget : public StatefulOperator {
 public:
  CaptureTarget(exec::DriverCtx* driverCtx, std::string planNodeId)
      : StatefulOperator(
            std::make_unique<DummyOperator>(driverCtx, std::move(planNodeId)),
            {}) {}

  void addInput(StreamElementPtr input) override {
    lastInput_ = std::move(input);
    receiveCount_++;
  }

  void advance() override {}

  StreamRecord* lastRecord() const {
    if (!lastInput_ || !lastInput_->isRecord()) {
      return nullptr;
    }
    return std::static_pointer_cast<StreamRecord>(lastInput_).get();
  }

  int receiveCount() const {
    return receiveCount_;
  }

 private:
  StreamElementPtr lastInput_;
  int receiveCount_{0};
};

class DeduplicateTest : public testing::Test,
                        public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"},
        std::vector<RowVectorPtr>{
            makeRowVector({makeFlatVector<int64_t>(std::vector<int64_t>{1})})});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = StatefulTask::create(
        "DeduplicateTest_task",
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
  }

  /// Builds a StreamDeduplicateOperator over 'rowType' with the given key
  /// channels and dedup parameters, wrapping a CaptureTarget.
  std::unique_ptr<StreamDeduplicateOperator> makeOperator(
      const RowTypePtr& rowType,
      std::vector<column_index_t> keyChannels,
      int rowtimeIndex,
      bool generateUpdateBefore,
      bool generateInsert,
      bool keepLastRow,
      CaptureTarget** capture,
      HashMapStateBackend* backend = nullptr,
      bool initializeState = true) {
    std::vector<TypePtr> keyTypes;
    keyTypes.reserve(keyChannels.size());
    for (auto channel : keyChannels) {
      keyTypes.push_back(rowType->childAt(channel));
    }
    // Unique plan-node id and operator id per call: the memory pool name is
    // derived from both, so two operators built by this helper must not
    // collide (used by the snapshot/restore round-trip test).
    auto planNodeId = "dedup" + std::to_string(nextOperatorId_);
    auto operatorId = static_cast<int32_t>(nextOperatorId_++);
    auto dedupNode = std::make_shared<DeduplicateNode>(
        planNodeId,
        rowType,
        /*minRetentionTime=*/0,
        rowtimeIndex,
        generateUpdateBefore,
        generateInsert,
        keepLastRow);
    auto ranker = std::make_unique<DeduplicateRanker>(
        operatorId,
        driverCtx_.get(),
        dedupNode,
        rowType,
        /*minRetentionTime=*/0,
        rowtimeIndex,
        generateUpdateBefore,
        generateInsert,
        keepLastRow);
    auto captureOp = std::make_unique<CaptureTarget>(
        driverCtx_.get(), planNodeId + "-capture");
    *capture = captureOp.get();
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(captureOp));
    auto op = std::make_unique<StreamDeduplicateOperator>(
        std::move(ranker),
        std::move(keyChannels),
        std::move(keyTypes),
        std::move(targets));
    op->initialize();
    op->initializeStateBackend(backend != nullptr ? backend : &stateBackend_);
    if (initializeState) {
      op->initializeState();
    }
    return op;
  }

  /// Builds an operator whose declared dedup output/input type may differ from
  /// the runtime batch type (proc-time: declared TIMESTAMP vs runtime BIGINT).
  /// The ranker opens with the declared type; normalizeInput() converts the
  /// runtime batch before KeySelector/state/output.
  std::unique_ptr<StreamDeduplicateOperator> makeOperatorWithTypes(
      const RowTypePtr& declaredType,
      const RowTypePtr& inputType,
      std::vector<column_index_t> keyChannels,
      int rowtimeIndex,
      bool generateUpdateBefore,
      bool generateInsert,
      bool keepLastRow,
      CaptureTarget** capture,
      HashMapStateBackend* backend = nullptr,
      bool initializeState = true) {
    std::vector<TypePtr> keyTypes;
    keyTypes.reserve(keyChannels.size());
    for (auto channel : keyChannels) {
      keyTypes.push_back(declaredType->childAt(channel));
    }
    auto planNodeId = "dedup" + std::to_string(nextOperatorId_);
    auto operatorId = static_cast<int32_t>(nextOperatorId_++);
    auto dedupNode = std::make_shared<DeduplicateNode>(
        planNodeId,
        declaredType,
        /*minRetentionTime=*/0,
        rowtimeIndex,
        generateUpdateBefore,
        generateInsert,
        keepLastRow);
    auto ranker = std::make_unique<DeduplicateRanker>(
        operatorId,
        driverCtx_.get(),
        dedupNode,
        declaredType,
        /*minRetentionTime=*/0,
        rowtimeIndex,
        generateUpdateBefore,
        generateInsert,
        keepLastRow);
    auto captureOp = std::make_unique<CaptureTarget>(
        driverCtx_.get(), planNodeId + "-capture");
    *capture = captureOp.get();
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(captureOp));
    auto op = std::make_unique<StreamDeduplicateOperator>(
        std::move(ranker),
        std::move(keyChannels),
        std::move(keyTypes),
        std::move(targets));
    op->initialize();
    op->initializeStateBackend(backend != nullptr ? backend : &stateBackend_);
    if (initializeState) {
      op->initializeState();
    }
    return op;
  }

  /// Feeds one batch of 'input' and drains the operator. Returns the captured
  /// record when the operator emitted something, or nullptr when it emitted
  /// nothing (the previous capture is NOT returned, so "no output" is
  /// distinguishable from "output identical to the previous emission").
  StreamRecord* feed(
      StreamDeduplicateOperator& op,
      RowVectorPtr input,
      CaptureTarget* capture) {
    const int before = capture->receiveCount();
    op.addInput(std::make_shared<StreamRecord>("src", std::move(input)));
    op.advance();
    if (capture->receiveCount() == before) {
      return nullptr;
    }
    return capture->lastRecord();
  }

  HashMapStateBackend stateBackend_{
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP,
          "job",
          "op",
          /*maxParallelism=*/128,
          /*startKeyGroup=*/0,
          /*numKeyGroups=*/128)};

  int64_t nextOperatorId_{0};
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<StatefulTask> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

TEST_F(DeduplicateTest, rowtimeKeepLast) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, true, true, &capture);

  // First row -> +I.
  auto out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_FALSE(out->appendOnly());
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // Later rowtime -> -U old +U new.
  out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(200)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));

  // Earlier rowtime -> suppressed (no emission).
  out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(50)})}),
      capture);
  EXPECT_EQ(out, nullptr);
}

TEST_F(DeduplicateTest, rowtimeKeepLastGenerateUpdateBeforeOnly) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  // generateUpdateBefore=true, generateInsert=false: the first row must still
  // be +I (Flink emits +I whenever either flag is on), not +U.
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, false, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // Later rowtime -> -U +U.
  out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(200)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
}

TEST_F(DeduplicateTest, rowtimeKeepLastNoGenerateFlags) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  // Both flags off -> constant +U (no INSERT, no UPDATE_BEFORE).
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, false, false, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_AFTER));

  out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(200)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_AFTER));
}

TEST_F(DeduplicateTest, rowtimeKeepFirst) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, true, false, &capture);

  // First row -> +I.
  auto out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // Earlier rowtime -> -U old +U new.
  out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(99)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  // The UPDATE_AFTER carries the earlier rowtime.
  auto ts = out->record()->childAt(1)->asFlatVector<Timestamp>();
  EXPECT_EQ(ts->valueAt(1), Timestamp::fromMicros(99));
}

TEST_F(DeduplicateTest, rowtimeEqualTimestampKeepLast) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, true, true, &capture);

  feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  // Equal rowtime: keep-last replaces (later arrival wins).
  auto out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
}

TEST_F(DeduplicateTest, rowtimeEqualTimestampKeepFirst) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, true, false, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // Equal rowtime: keep-first does not replace (no emission).
  out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  EXPECT_EQ(out, nullptr);
}

TEST_F(DeduplicateTest, procTimeKeepFirst) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, false, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(out->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 10);

  // Second row for the same key is ignored (no emission).
  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{20})}),
      capture);
  EXPECT_EQ(out, nullptr);
}

TEST_F(DeduplicateTest, procTimeKeepLast) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // Different value -> -U +U.
  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{20})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));

  // Same value again -> suppressed (same-value suppression, TTL off).
  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{20})}),
      capture);
  EXPECT_EQ(out, nullptr);
}

TEST_F(DeduplicateTest, procTimeKeepLastArraySameValueSuppression) {
  auto rowType = ROW({"a", "arr"}, {BIGINT(), ARRAY(BIGINT())});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  const auto makeArrRow = [&](const std::vector<int64_t>& vals) {
    return makeRowVector(
        {"a", "arr"},
        {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
         makeArrayVector<int64_t>({vals})});
  };

  // First row -> +I.
  auto out = feed(*op, makeArrRow({1, 2}), capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // Same array content -> suppressed (complex-type equality via equalValueAt).
  out = feed(*op, makeArrRow({1, 2}), capture);
  EXPECT_EQ(out, nullptr);

  // Different array content -> -U +U.
  out = feed(*op, makeArrRow({1, 3}), capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
}

TEST_F(DeduplicateTest, multiKeyIsolation) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, true, true, &capture);

  // Key 1 -> +I.
  auto out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // Key 2 -> +I, independent of key 1.
  out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{2}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 2);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // Updating key 1 does not touch key 2's state.
  out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(200)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(1), 1);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
}

TEST_F(DeduplicateTest, thousandKeysIsolation) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, true, true, &capture);

  // First arrival for keys 0..999 -> +I each, with per-key state independent.
  for (int64_t k = 0; k < 1000; ++k) {
    auto out = feed(
        *op,
        makeRowVector(
            {"a", "ts"},
            {makeFlatVector<int64_t>(std::vector<int64_t>{k}),
             makeFlatVector<Timestamp>({Timestamp::fromMicros(k)})}),
        capture);
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
    EXPECT_EQ(
        out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), k);
  }

  // Update key 500 with a later rowtime -> -U +U. The -U must retract key
  // 500's previously stored row (ts=500), proving per-key state survived the
  // 1000-key population.
  auto out = feed(
      *op,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{500}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(500000)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  auto tsCol = out->record()->childAt(1)->asFlatVector<Timestamp>();
  EXPECT_EQ(tsCol->valueAt(0), Timestamp::fromMicros(500));
  EXPECT_EQ(tsCol->valueAt(1), Timestamp::fromMicros(500000));
}

TEST_F(DeduplicateTest, nonInsertInputFails) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, false, &capture);

  auto input = makeRowVector(
      {"a", "v"},
      {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
       makeFlatVector<int64_t>(std::vector<int64_t>{10})});
  auto rowKind = makeFlatVector<int8_t>(
      std::vector<int8_t>{static_cast<int8_t>(RowKind::UPDATE_AFTER)});
  EXPECT_THROW(
      op->addInput(std::make_shared<StreamRecord>("src", input, rowKind)),
      VeloxUserError);
}

TEST_F(DeduplicateTest, globalDeduplicate) {
  auto rowType = ROW({"v", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* capture = nullptr;
  // Empty key channels -> global (single synthetic key), rowtime keep-last.
  auto op =
      makeOperator(rowType, {}, /*rowtimeIndex=*/1, true, true, true, &capture);

  // First row (any key) -> +I.
  auto out = feed(
      *op,
      makeRowVector(
          {"v", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{10}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(100)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 10);

  // A different "v" with a later rowtime replaces the global representative.
  out = feed(
      *op,
      makeRowVector(
          {"v", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{20}),
           makeFlatVector<Timestamp>({Timestamp::fromMicros(200)})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  // The -U retracts the previous global row (v=10).
  EXPECT_EQ(out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 10);
  EXPECT_EQ(out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(1), 20);
}

TEST_F(DeduplicateTest, procTimeKeepLastNoGenerateFlags) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  // Both generateUpdateBefore and generateInsert off -> stateless, always +U.
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, false, false, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_AFTER));

  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{20})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_AFTER));
}

TEST_F(DeduplicateTest, snapshotRestoreRoundTrip) {
  auto rowType = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  CaptureTarget* captureA = nullptr;
  CaptureTarget* captureB = nullptr;
  auto opA = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, true, true, &captureA);
  auto opB = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/1, true, true, true, &captureB);

  // Feed the first row for key 1 to A, then snapshot the backend.
  feed(
      *opA,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMillis(100)})}),
      captureA);
  auto bytes = opA->keyedStateBackend()->snapshot();

  // Restore the snapshot into a fresh operator B.
  opB->keyedStateBackend()->restore(bytes);

  // Feed the same update to both A and B; they must emit identically.
  auto outA = feed(
      *opA,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMillis(200)})}),
      captureA);
  auto outB = feed(
      *opB,
      makeRowVector(
          {"a", "ts"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<Timestamp>({Timestamp::fromMillis(200)})}),
      captureB);

  ASSERT_NE(outA, nullptr);
  ASSERT_NE(outB, nullptr);
  EXPECT_EQ(outA->record()->size(), outB->record()->size());
  EXPECT_EQ(outA->rowKind()->valueAt(0), outB->rowKind()->valueAt(0));
  EXPECT_EQ(outA->rowKind()->valueAt(1), outB->rowKind()->valueAt(1));

  // The -U must retract the round-tripped old value (ts=100), not roll back to
  // a fresh state.
  auto tsA = outA->record()->childAt(1)->asFlatVector<Timestamp>();
  auto tsB = outB->record()->childAt(1)->asFlatVector<Timestamp>();
  EXPECT_EQ(tsA->valueAt(0), Timestamp::fromMillis(100));
  EXPECT_EQ(tsA->valueAt(0), tsB->valueAt(0));
  EXPECT_EQ(tsA->valueAt(1), tsB->valueAt(1));
  EXPECT_EQ(tsA->valueAt(1), Timestamp::fromMillis(200));
}

TEST_F(DeduplicateTest, operatorSnapshotRestoreBeforeInitialize) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* captureA = nullptr;
  CaptureTarget* captureB = nullptr;
  auto opA = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &captureA);
  auto opB = makeOperator(
      rowType,
      {0},
      /*rowtimeIndex=*/-1,
      true,
      true,
      true,
      &captureB,
      nullptr,
      /*initializeState=*/false);

  auto makeRow = [&](int64_t v) {
    return makeRowVector(
        {"a", "v"},
        {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
         makeFlatVector<int64_t>(std::vector<int64_t>{v})});
  };

  feed(*opA, makeRow(10), captureA);
  const auto records = opA->snapshotState(1);
  ASSERT_FALSE(records.empty());

  // StatefulTask restores before initializeState; the state id must already be
  // registered by initializeStateBackend for this to work.
  opB->restoreState(records);
  opB->initializeState();

  auto outA = feed(*opA, makeRow(20), captureA);
  auto outB = feed(*opB, makeRow(20), captureB);
  ASSERT_NE(outA, nullptr);
  ASSERT_NE(outB, nullptr);
  EXPECT_EQ(outA->record()->size(), outB->record()->size());
  EXPECT_EQ(outA->rowKind()->valueAt(0), outB->rowKind()->valueAt(0));
  EXPECT_EQ(outA->rowKind()->valueAt(1), outB->rowKind()->valueAt(1));
  EXPECT_EQ(
      outA->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(0),
      outB->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(0));
}

TEST_F(DeduplicateTest, keyGroupSubrangeSnapshotRestore) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  constexpr uint32_t kMaxParallelism = 256;
  constexpr uint32_t kStartKeyGroup = 10;
  constexpr uint32_t kNumKeyGroups = 20;

  HashMapStateBackend backendA{
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP,
          "job",
          "op-a",
          kMaxParallelism,
          kStartKeyGroup,
          kNumKeyGroups)};
  CaptureTarget* captureA = nullptr;
  auto opA = makeOperator(
      rowType,
      {0},
      /*rowtimeIndex=*/-1,
      true,
      true,
      true,
      &captureA,
      &backendA);

  auto& keyIndex = opA->keyedStateBackend()->keyIndex();
  int64_t key = 0;
  for (; key < 100000; ++key) {
    const auto candidate =
        keyIndex.probeKey({makeFlatVector<int64_t>(std::vector<int64_t>{key})});
    if (candidate.keyGroup() >= kStartKeyGroup &&
        candidate.keyGroup() < kStartKeyGroup + kNumKeyGroups) {
      break;
    }
  }
  ASSERT_LT(key, 100000) << "No key found inside the key-group subrange";

  auto makeRow = [&](int64_t v) {
    return makeRowVector(
        {"a", "v"},
        {makeFlatVector<int64_t>(std::vector<int64_t>{key}),
         makeFlatVector<int64_t>(std::vector<int64_t>{v})});
  };

  feed(*opA, makeRow(10), captureA);
  auto bytes = opA->keyedStateBackend()->snapshot();

  HashMapStateBackend backendB{
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP,
          "job",
          "op-b",
          kMaxParallelism,
          kStartKeyGroup,
          kNumKeyGroups)};
  CaptureTarget* captureB = nullptr;
  auto opB = makeOperator(
      rowType,
      {0},
      /*rowtimeIndex=*/-1,
      true,
      true,
      true,
      &captureB,
      &backendB);
  opB->keyedStateBackend()->restore(bytes);

  auto outA = feed(*opA, makeRow(20), captureA);
  auto outB = feed(*opB, makeRow(20), captureB);
  ASSERT_NE(outA, nullptr);
  ASSERT_NE(outB, nullptr);
  EXPECT_EQ(outA->record()->size(), outB->record()->size());
  EXPECT_EQ(outA->rowKind()->valueAt(0), outB->rowKind()->valueAt(0));
  EXPECT_EQ(outA->rowKind()->valueAt(1), outB->rowKind()->valueAt(1));
  EXPECT_EQ(
      outA->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(0),
      outB->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(0));
}

TEST_F(DeduplicateTest, procTimeRuntimeTypeNormalization) {
  auto declared = ROW({"a", "v", "$2"}, {BIGINT(), BIGINT(), TIMESTAMP()});
  auto runtime = ROW({"a", "v", "$2"}, {BIGINT(), BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperatorWithTypes(
      declared, runtime, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);
  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v", "$2"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10}),
           makeFlatVector<int64_t>(std::vector<int64_t>{111})}),
      capture);

  ASSERT_NE(out, nullptr);
  // The operator output data row (row kind is carried separately) must match
  // the declared output type, not the runtime BIGINT batch type.
  EXPECT_TRUE(out->record()->type()->equivalent(*declared));
  EXPECT_EQ(out->record()->childAt(2)->type()->kind(), TypeKind::TIMESTAMP);
  EXPECT_EQ(
      out->record()->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(111 * 1000));
}

TEST_F(DeduplicateTest, procTimeSnapshotRestoreRuntimeType) {
  auto declared = ROW({"a", "v", "$2"}, {BIGINT(), BIGINT(), TIMESTAMP()});
  auto runtime = ROW({"a", "v", "$2"}, {BIGINT(), BIGINT(), BIGINT()});
  CaptureTarget* captureA = nullptr;
  CaptureTarget* captureB = nullptr;
  auto opA = makeOperatorWithTypes(
      declared, runtime, {0}, /*rowtimeIndex=*/-1, true, true, true, &captureA);

  HashMapStateBackend backendB{
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP,
          "job",
          "op",
          /*maxParallelism=*/128,
          /*startKeyGroup=*/0,
          /*numKeyGroups=*/128)};
  auto opB = makeOperatorWithTypes(
      declared,
      runtime,
      {0},
      /*rowtimeIndex=*/-1,
      true,
      true,
      true,
      &captureB,
      &backendB);

  auto makeRow = [&](int64_t v, int64_t p) {
    return makeRowVector(
        {"a", "v", "$2"},
        {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
         makeFlatVector<int64_t>(std::vector<int64_t>{v}),
         makeFlatVector<int64_t>(std::vector<int64_t>{p})});
  };

  feed(*opA, makeRow(10, 111), captureA);
  auto bytes = opA->keyedStateBackend()->snapshot();

  // Must not fail: the store/checkpoint schema is the declared TIMESTAMP
  // type, independent of the runtime BIGINT batch.
  ASSERT_NO_THROW(opB->keyedStateBackend()->restore(bytes));

  auto outB = feed(*opB, makeRow(20, 222), captureB);
  ASSERT_NE(outB, nullptr);
  EXPECT_EQ(outB->record()->size(), 2);
  auto* timestamps = outB->record()->childAt(2)->asFlatVector<Timestamp>();
  EXPECT_EQ(timestamps->valueAt(0), Timestamp::fromMillis(111 * 1000));
  EXPECT_EQ(timestamps->valueAt(1), Timestamp::fromMillis(222 * 1000));
  EXPECT_TRUE(outB->record()->type()->equivalent(*declared));
}

TEST_F(DeduplicateTest, procTimeVarcharChangelogPayload) {
  auto rowType = ROW({"a", "s"}, {BIGINT(), VARCHAR()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "s"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<std::string>({"alpha"})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(
      out->record()->childAt(1)->asFlatVector<StringView>()->valueAt(0),
      "alpha");

  out = feed(
      *op,
      makeRowVector(
          {"a", "s"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<std::string>({"alpha"})}),
      capture);
  EXPECT_EQ(out, nullptr);

  out = feed(
      *op,
      makeRowVector(
          {"a", "s"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<std::string>({"beta"})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->record()->size(), 2);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  auto strings = out->record()->childAt(1)->asFlatVector<StringView>();
  EXPECT_EQ(strings->valueAt(0), "alpha");
  EXPECT_EQ(strings->valueAt(1), "beta");
}

TEST_F(DeduplicateTest, procTimeArrayVarcharSnapshotRestore) {
  auto rowType = ROW({"a", "arr"}, {BIGINT(), ARRAY(VARCHAR())});
  CaptureTarget* captureA = nullptr;
  CaptureTarget* captureB = nullptr;
  auto opA = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &captureA);

  auto makeArrRow = [&](const std::vector<std::string>& values) {
    return makeRowVector(
        {"a", "arr"},
        {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
         makeArrayVector<std::string>({values})});
  };

  auto out = feed(*opA, makeArrRow({"old", "x"}), captureA);
  ASSERT_NE(out, nullptr);
  auto bytes = opA->keyedStateBackend()->snapshot();

  HashMapStateBackend backendB{
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP,
          "job",
          "op",
          /*maxParallelism=*/128,
          /*startKeyGroup=*/0,
          /*numKeyGroups=*/128)};
  auto opB = makeOperatorWithTypes(
      rowType,
      rowType,
      {0},
      /*rowtimeIndex=*/-1,
      true,
      true,
      true,
      &captureB,
      &backendB);
  ASSERT_NO_THROW(opB->keyedStateBackend()->restore(bytes));

  out = feed(*opB, makeArrRow({"new", "y"}), captureB);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->record()->size(), 2);
  EXPECT_EQ(
      out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  velox::test::assertEqualVectors(
      makeArrayVector<std::string>({{"old", "x"}, {"new", "y"}}),
      out->record()->childAt(1));
}

TEST_F(DeduplicateTest, procTimeKeepFirstSkipsExistingArrayPayload) {
  auto rowType = ROW({"a", "arr"}, {BIGINT(), ARRAY(VARCHAR())});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, false, &capture);

  auto makeRow = [&](const std::vector<std::string>& values) {
    return makeRowVector(
        {"a", "arr"},
        {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
         makeArrayVector<std::string>({values})});
  };

  auto out = feed(*op, makeRow({"old"}), capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  // keep-first only needs to know that the key exists; the existing
  // variable-width payload must not be materialized or re-emitted.
  out = feed(*op, makeRow({"new", "value"}), capture);
  EXPECT_EQ(out, nullptr);
}

TEST_F(DeduplicateTest, procTimeNullKeyFormsOneGroup) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeNullableFlatVector<int64_t>({std::nullopt}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_TRUE(out->record()->childAt(0)->isNullAt(0));

  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeNullableFlatVector<int64_t>({std::nullopt}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10})}),
      capture);
  EXPECT_EQ(out, nullptr);

  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeNullableFlatVector<int64_t>({std::nullopt}),
           makeFlatVector<int64_t>(std::vector<int64_t>{20})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->record()->size(), 2);
  EXPECT_TRUE(out->record()->childAt(0)->isNullAt(0));
  EXPECT_TRUE(out->record()->childAt(0)->isNullAt(1));

  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeNullableFlatVector<int64_t>({1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{30})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
}

TEST_F(DeduplicateTest, procTimeEmptyBatchKeepsState) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  auto empty = makeRowVector(
      {"a", "v"},
      {makeFlatVector<int64_t>(std::vector<int64_t>{}),
       makeFlatVector<int64_t>(std::vector<int64_t>{})});
  EXPECT_EQ(feed(*op, empty, capture), nullptr);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));

  EXPECT_EQ(feed(*op, empty, capture), nullptr);

  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{20})}),
      capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->record()->size(), 2);
  EXPECT_EQ(out->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 10);
  EXPECT_EQ(out->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(1), 20);
}

TEST_F(DeduplicateTest, procTimeDuplicateRowsInOneBatchKeepLast) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1, 1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10, 20})}),
      capture);
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->record()->size(), 3);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  auto values = out->record()->childAt(1)->asFlatVector<int64_t>();
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(1), 10);
  EXPECT_EQ(values->valueAt(2), 20);

  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{20})}),
      capture);
  EXPECT_EQ(out, nullptr);
}

TEST_F(DeduplicateTest, procTimeDuplicateRowsInOneBatchKeepFirst) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, false, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1, 1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10, 20})}),
      capture);
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->record()->size(), 1);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(out->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 10);
}

TEST_F(DeduplicateTest, insertOnlyRowKindColumnAccepted) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, false, &capture);

  auto input = makeRowVector(
      {"a", "v"},
      {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
       makeFlatVector<int64_t>(std::vector<int64_t>{10})});
  auto rowKind = makeFlatVector<int8_t>(
      std::vector<int8_t>{static_cast<int8_t>(RowKind::INSERT)});
  const int before = capture->receiveCount();
  op->addInput(std::make_shared<StreamRecord>("src", input, rowKind));
  op->advance();
  ASSERT_EQ(capture->receiveCount(), before + 1);
  auto out = capture->lastRecord();
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
}

TEST_F(DeduplicateTest, normalizeUnsupportedTypeFails) {
  auto declared = ROW({"a", "v"}, {BIGINT(), VARCHAR()});
  auto runtime = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperatorWithTypes(
      declared, runtime, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  EXPECT_THROW(
      feed(
          *op,
          makeRowVector(
              {"a", "v"},
              {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
               makeFlatVector<int64_t>(std::vector<int64_t>{10})}),
          capture),
      VeloxException);
}

TEST_F(DeduplicateTest, rowtimeBigIntMillisNormalization) {
  auto declared = ROW({"a", "ts"}, {BIGINT(), TIMESTAMP()});
  auto runtime = ROW({"a", "ts"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperatorWithTypes(
      declared, runtime, {0}, /*rowtimeIndex=*/1, true, true, true, &capture);

  auto makeRow = [&](int64_t millis) {
    return makeRowVector(
        {"a", "ts"},
        {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
         makeFlatVector<int64_t>(std::vector<int64_t>{millis})});
  };

  auto out = feed(*op, makeRow(1000), capture);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(
      out->record()->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(1000));

  out = feed(*op, makeRow(2000), capture);
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->record()->size(), 2);
  auto timestamps = out->record()->childAt(1)->asFlatVector<Timestamp>();
  EXPECT_EQ(timestamps->valueAt(0), Timestamp::fromMillis(1000));
  EXPECT_EQ(timestamps->valueAt(1), Timestamp::fromMillis(2000));
}

TEST_F(DeduplicateTest, procTimeMixedBatchNewAndUpdateOrder) {
  auto rowType = ROW({"a", "v"}, {BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperator(
      rowType, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1, 2, 1}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10, 30, 20})}),
      capture);
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->record()->size(), 4);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(
      out->rowKind()->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(3), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  auto keys = out->record()->childAt(0)->asFlatVector<int64_t>();
  auto values = out->record()->childAt(1)->asFlatVector<int64_t>();
  EXPECT_EQ(keys->valueAt(0), 1);
  EXPECT_EQ(keys->valueAt(1), 2);
  EXPECT_EQ(keys->valueAt(2), 1);
  EXPECT_EQ(keys->valueAt(3), 1);
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(1), 30);
  EXPECT_EQ(values->valueAt(2), 10);
  EXPECT_EQ(values->valueAt(3), 20);

  out = feed(
      *op,
      makeRowVector(
          {"a", "v"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{2}),
           makeFlatVector<int64_t>(std::vector<int64_t>{40})}),
      capture);
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->record()->size(), 2);
  EXPECT_EQ(out->record()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 2);
  EXPECT_EQ(out->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 30);
  EXPECT_EQ(out->record()->childAt(1)->asFlatVector<int64_t>()->valueAt(1), 40);
}

TEST_F(DeduplicateTest, procTimeDeclaredLayoutChangelog) {
  auto declared = ROW({"a", "v", "$2"}, {BIGINT(), BIGINT(), TIMESTAMP()});
  auto runtime = ROW({"a", "v", "$2"}, {BIGINT(), BIGINT(), BIGINT()});
  CaptureTarget* capture = nullptr;
  auto op = makeOperatorWithTypes(
      declared, runtime, {0}, /*rowtimeIndex=*/-1, true, true, true, &capture);

  auto out = feed(
      *op,
      makeRowVector(
          {"a", "v", "$2"},
          {makeFlatVector<int64_t>(std::vector<int64_t>{1, 1, 2}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10, 20, 30}),
           makeFlatVector<int64_t>(std::vector<int64_t>{111, 222, 333})}),
      capture);
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->record()->size(), 4);
  ASSERT_EQ(out->record()->childrenSize(), 3);
  EXPECT_TRUE(out->record()->type()->equivalent(*declared));
  EXPECT_EQ(out->record()->childAt(0)->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(out->record()->childAt(1)->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(out->record()->childAt(2)->type()->kind(), TypeKind::TIMESTAMP);
  EXPECT_EQ(out->rowKind()->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(
      out->rowKind()->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(
      out->rowKind()->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  EXPECT_EQ(out->rowKind()->valueAt(3), static_cast<int8_t>(RowKind::INSERT));
  auto timestamps = out->record()->childAt(2)->asFlatVector<Timestamp>();
  EXPECT_EQ(timestamps->valueAt(0), Timestamp::fromMillis(111 * 1000));
  EXPECT_EQ(timestamps->valueAt(1), Timestamp::fromMillis(111 * 1000));
  EXPECT_EQ(timestamps->valueAt(2), Timestamp::fromMillis(222 * 1000));
  EXPECT_EQ(timestamps->valueAt(3), Timestamp::fromMillis(333 * 1000));
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
