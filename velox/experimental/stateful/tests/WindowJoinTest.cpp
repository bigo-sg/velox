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

#include <limits>

#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/WindowJoin.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/experimental/stateful/state/HashMapStateBackend.h"
#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful::test {

using namespace facebook::velox::test;

namespace {

constexpr int64_t kWindowSize = 10000;

// A terminal scan stub with its own plan node id: the join routes pushed
// records by the record's node id, so each side's scan needs a distinct one.
class ScanStub : public exec::Operator {
 public:
  ScanStub(
      exec::DriverCtx* driverCtx,
      const RowTypePtr& inputType,
      std::string planNodeId)
      : Operator(driverCtx, inputType, 0, std::move(planNodeId), "ScanStub") {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr /*input*/) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture*) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return true;
  }
};

// Downstream collector: records every pushed record and watermark, so
// tests can assert that fired records reach downstream ahead of the
// watermark.
class CaptureTarget : public StatefulOperator {
 public:
  CaptureTarget(
      exec::DriverCtx* driverCtx,
      const RowTypePtr& rowType,
      std::string spyNodeId)
      : StatefulOperator(
            std::make_unique<ScanStub>(
                driverCtx,
                rowType,
                std::move(spyNodeId)),
            {}) {}

  void addInput(StreamElementPtr input) override {
    records_.push_back(std::move(input));
    events_.push_back("record");
  }

  void advance() override {}

  void processWatermark(int64_t timestamp) override {
    events_.push_back("watermark:" + std::to_string(timestamp));
  }

  const std::vector<StreamElementPtr>& records() const {
    return records_;
  }

  const std::vector<std::string>& events() const {
    return events_;
  }

 private:
  std::vector<StreamElementPtr> records_;
  std::vector<std::string> events_;
};

class WindowJoinTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"},
        std::vector<RowVectorPtr>{
            makeRowVector({"x"}, {makeFlatVector<int64_t>({1})})});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "WindowJoinTest_task",
        std::move(planFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kParallel);
    driver_ = exec::Driver::testingCreate();
    driverCtx_ = std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0);
    driverCtx_->driver = driver_.get();

    taskBackend_ = std::make_unique<HashMapStateBackend>(
        std::make_shared<const KeyedStateBackendParameters>(
            StateBackendType::HEAP, "job", "window-join"));
  }

  void TearDown() override {
    driverCtx_.reset();
    driver_.reset();
    task_.reset();
    executor_.reset();
    OperatorTestBase::TearDown();
  }

  RowTypePtr leftType() const {
    return ROW(
        {"l_k", "l_ws", "l_we", "l_v"},
        {BIGINT(), BIGINT(), BIGINT(), VARCHAR()});
  }

  RowTypePtr rightType() const {
    return ROW(
        {"r_k", "r_ws", "r_we", "r_v"},
        {BIGINT(), BIGINT(), BIGINT(), VARCHAR()});
  }

  // The join emits the concatenated inputs (both sides in full); the
  // projection above the join selects columns, as in Flink.
  RowTypePtr joinedType() const {
    return ROW(
        {"l_k", "l_ws", "l_we", "l_v", "r_k", "r_ws", "r_we", "r_v"},
        {BIGINT(),
         BIGINT(),
         BIGINT(),
         VARCHAR(),
         BIGINT(),
         BIGINT(),
         BIGINT(),
         VARCHAR()});
  }

  core::TypedExprPtr equiCondition() const {
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "l_k"),
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "r_k")},
        "equalto");
  }

  // The composite key: (join key, window end) on both sides, channels 0 and 2.
  std::shared_ptr<const StreamWindowJoinNode> makeNode(
      core::JoinType joinType,
      core::TypedExprPtr joinCondition,
      const RowTypePtr& outputType) {
    auto leftInput = leftType();
    auto rightInput = rightType();
    // The probe always carries kInner (NestedLoopJoinNode rejects
    // semi/anti in isSupported); the actual join type rides the node.
    auto probeNode = std::make_shared<core::NestedLoopJoinNode>(
        core::PlanNodeId{nextNodeId()},
        core::JoinType::kInner,
        joinCondition,
        std::make_shared<EmptyNode>(leftInput),
        std::make_shared<EmptyNode>(rightInput),
        outputType);
    return std::make_shared<const StreamWindowJoinNode>(
        core::PlanNodeId{nextNodeId()},
        std::vector<core::PlanNodePtr>{
            std::make_shared<EmptyNode>(leftInput),
            std::make_shared<EmptyNode>(rightInput)},
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        probeNode,
        outputType,
        /*numPartitions=*/1,
        /*leftWindowEndIndex=*/2,
        /*rightWindowEndIndex=*/2,
        /*leftKeyChannels=*/std::vector<column_index_t>{0, 2},
        /*rightKeyChannels=*/std::vector<column_index_t>{0, 2},
        /*shiftTimeZone=*/0,
        /*joinType=*/static_cast<int>(joinType));
  }

  std::unique_ptr<WindowJoin> makeJoin(
      const std::shared_ptr<const StreamWindowJoinNode>& node,
      std::unique_ptr<StatefulOperator> capture) {
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(capture));
    leftScanId_ = "left-scan-" + nextNodeId();
    rightScanId_ = "right-scan-" + nextNodeId();
    auto leftOp =
        std::make_unique<ScanStub>(driverCtx_.get(), leftType(), leftScanId_);
    auto rightOp =
        std::make_unique<ScanStub>(driverCtx_.get(), rightType(), rightScanId_);
    auto probeOp = std::make_unique<exec::NestedLoopJoinProbe>(
        2, driverCtx_.get(), node->probe());
    auto join = std::make_unique<WindowJoin>(
        std::move(leftOp),
        std::move(rightOp),
        std::move(probeOp),
        std::move(targets),
        node);
    join->initialize();
    join->initializeStateBackend(taskBackend_.get());
    return join;
  }

  StreamElementPtr record(
      const std::string& nodeId,
      const RowTypePtr& type,
      const std::vector<std::optional<int64_t>>& k,
      const std::vector<int64_t>& windowEnd,
      const std::vector<std::string>& v) {
    std::vector<int64_t> ws;
    ws.reserve(windowEnd.size());
    for (auto we : windowEnd) {
      ws.push_back(we - kWindowSize);
    }
    auto vector = makeRowVector(
        {type->names()[0],
         type->names()[1],
         type->names()[2],
         type->names()[3]},
        {makeNullableFlatVector<int64_t>(k),
         makeFlatVector<int64_t>(ws),
         makeFlatVector<int64_t>(windowEnd),
         makeFlatVector<std::string>(v)});
    return std::make_shared<StreamRecord>(nodeId, std::move(vector));
  }

  StreamElementPtr leftRecord(
      const std::vector<std::optional<int64_t>>& k,
      const std::vector<int64_t>& windowEnd,
      const std::vector<std::string>& v) {
    return record(leftScanId_, leftType(), k, windowEnd, v);
  }

  StreamElementPtr rightRecord(
      const std::vector<std::optional<int64_t>>& k,
      const std::vector<int64_t>& windowEnd,
      const std::vector<std::string>& v) {
    return record(rightScanId_, rightType(), k, windowEnd, v);
  }

  static std::string nextNodeId() {
    static int64_t id = 0;
    return "node-" + std::to_string(id++);
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
  std::unique_ptr<HashMapStateBackend> taskBackend_;
  std::string leftScanId_;
  std::string rightScanId_;
};

// Two left rows and two right rows in the same (key, window) pair to an
// INNER join: the window fires with the full 2x2 cross product.
TEST_F(WindowJoinTest, SameWindowPairsAcrossSides) {
  functions::sparksql::registerFunctions();
  auto node = makeNode(core::JoinType::kInner, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1, 1}, {20000, 20000}, {"a", "b"}));
  join->addInput(rightRecord({1, 1}, {20000, 20000}, {"x", "y"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  // One fired record carries the whole window's cross product (2x2 rows).
  ASSERT_EQ(1, spy->records().size());
  auto* out = static_cast<StreamRecord*>(spy->records()[0].get());
  ASSERT_EQ(4, out->record()->size());
  ASSERT_EQ(8, out->record()->childrenSize());
}

// Same key, different windows: the two windows fire independently and the
// rows never pair.
TEST_F(WindowJoinTest, DifferentWindowsDoNotPair) {
  auto node = makeNode(core::JoinType::kInner, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({5}, {20000}, {"a"}));
  join->addInput(rightRecord({5}, {30000}, {"x"}));
  join->processWatermark(29999, 0);
  join->processWatermark(29999, 1);

  ASSERT_EQ(0, spy->records().size());
}

// Rows whose window already fired under the combined watermark are late:
// dropped and counted per side, never stored.
TEST_F(WindowJoinTest, LateRowsAreDroppedAndCounted) {
  auto node = makeNode(core::JoinType::kInner, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->processWatermark(25000, 0);
  join->processWatermark(25000, 1);
  join->addInput(leftRecord({7, 7}, {20000, 20000}, {"a", "b"}));
  join->addInput(rightRecord({7}, {20000}, {"x"}));

  ASSERT_EQ(0, spy->records().size());
  ASSERT_EQ(2, join->leftLateRecordsDropped());
  ASSERT_EQ(1, join->rightLateRecordsDropped());
}

// Fired records reach the capture target ahead of the watermark event.
TEST_F(WindowJoinTest, RecordsPrecedeWatermarkDownstream) {
  auto node = makeNode(core::JoinType::kInner, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1}, {20000}, {"a"}));
  join->addInput(rightRecord({1}, {20000}, {"x"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  ASSERT_FALSE(spy->events().empty());
  ASSERT_EQ("record", spy->events().front());
  ASSERT_EQ(2, spy->events().size());
  ASSERT_EQ("watermark:19999", spy->events()[1]);
}

// NULL keys never satisfy an INNER equality: the fast path drops them
// instead of emitting a null-null pair.
TEST_F(WindowJoinTest, NullKeysNeverPairOnFastPath) {
  auto node = makeNode(core::JoinType::kInner, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({std::optional<int64_t>{}}, {20000}, {"a"}));
  join->addInput(rightRecord({std::optional<int64_t>{}}, {20000}, {"x"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  ASSERT_EQ(0, spy->records().size());
}

// Rows of one (key, window) accumulate across batches before the window
// fires.
TEST_F(WindowJoinTest, BatchesAccumulateIntoOneWindow) {
  auto node = makeNode(core::JoinType::kInner, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1}, {20000}, {"a"}));
  join->addInput(leftRecord({1}, {20000}, {"b"}));
  join->addInput(rightRecord({1}, {20000}, {"x"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  // Both left batches landed in the same (key, window) entry: one fired
  // record with the 2x1 cross product.
  ASSERT_EQ(1, spy->records().size());
  auto* out = static_cast<StreamRecord*>(spy->records()[0].get());
  ASSERT_EQ(2, out->record()->size());
}

// A window with rows on one side only fires nothing and clears both
// sides' state.
TEST_F(WindowJoinTest, EmptySideFiresNothing) {
  auto node = makeNode(core::JoinType::kInner, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1, 2}, {20000, 20000}, {"a", "b"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);
  ASSERT_EQ(0, spy->records().size());

  // The fired window is gone: a late right row is dropped, not paired.
  join->addInput(rightRecord({1}, {20000}, {"x"}));
  ASSERT_EQ(1, join->rightLateRecordsDropped());
  ASSERT_EQ(0, spy->records().size());
}

// -----------------------------------------------------------------------
// Six-semantics tests (LEFT, RIGHT, FULL, SEMI, ANTI).
// -----------------------------------------------------------------------

TEST_F(WindowJoinTest, LeftOuterJoin) {
  auto node = makeNode(core::JoinType::kLeft, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1, 2}, {20000, 20000}, {"a", "b"}));
  join->addInput(rightRecord({1}, {20000}, {"x"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  int totalRows = 0;
  for (const auto& element : spy->records()) {
    totalRows += static_cast<StreamRecord*>(element.get())->record()->size();
  }
  ASSERT_EQ(2, totalRows);
}

TEST_F(WindowJoinTest, LeftOuterJoinEmptyRight) {
  auto node = makeNode(core::JoinType::kLeft, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1, 2}, {20000, 20000}, {"a", "b"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  int totalRows = 0;
  for (const auto& element : spy->records()) {
    totalRows += static_cast<StreamRecord*>(element.get())->record()->size();
  }
  ASSERT_EQ(2, totalRows);
}

TEST_F(WindowJoinTest, RightOuterJoin) {
  auto node = makeNode(core::JoinType::kRight, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1}, {20000}, {"a"}));
  join->addInput(rightRecord({1, 2}, {20000, 20000}, {"x", "y"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  int totalRows = 0;
  for (const auto& element : spy->records()) {
    totalRows += static_cast<StreamRecord*>(element.get())->record()->size();
  }
  ASSERT_EQ(2, totalRows);
}

TEST_F(WindowJoinTest, FullOuterJoin) {
  auto node = makeNode(core::JoinType::kFull, nullptr, joinedType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), joinedType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1, 2}, {20000, 20000}, {"a", "b"}));
  join->addInput(rightRecord({3, 4}, {20000, 20000}, {"x", "y"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  int totalRows = 0;
  for (const auto& element : spy->records()) {
    totalRows += static_cast<StreamRecord*>(element.get())->record()->size();
  }
  ASSERT_EQ(4, totalRows);
}

TEST_F(WindowJoinTest, SemiJoin) {
  auto node = makeNode(core::JoinType::kLeftSemiFilter, nullptr, leftType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), leftType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1, 2}, {20000, 20000}, {"a", "b"}));
  join->addInput(rightRecord({1}, {20000}, {"x"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  int totalRows = 0;
  for (const auto& element : spy->records()) {
    totalRows += static_cast<StreamRecord*>(element.get())->record()->size();
  }
  ASSERT_EQ(1, totalRows);
}

TEST_F(WindowJoinTest, AntiJoin) {
  auto node = makeNode(core::JoinType::kAnti, nullptr, leftType());
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), leftType(), "spy");
  auto* spy = capture.get();
  auto join = makeJoin(node, std::move(capture));

  join->addInput(leftRecord({1, 2}, {20000, 20000}, {"a", "b"}));
  join->addInput(rightRecord({1}, {20000}, {"x"}));
  join->processWatermark(19999, 0);
  join->processWatermark(19999, 1);

  int totalRows = 0;
  for (const auto& element : spy->records()) {
    totalRows += static_cast<StreamRecord*>(element.get())->record()->size();
  }
  ASSERT_EQ(1, totalRows);
}

// The Java side serializes joinType; the C++ create() must round-trip it or
// every non-INNER join silently degrades to INNER in end-to-end plans.
TEST_F(WindowJoinTest, NodeSerdeRoundTripsJoinType) {
  facebook::velox::Type::registerSerDe();
  facebook::velox::core::PlanNode::registerSerDe();
  StatefulPlanNode::registerSerDe();
  facebook::velox::exec::registerPartitionFunctionSerDe();
  // Use the serde-registered hash spec, mirroring the Java-side node.
  auto leftInput = leftType();
  auto rightInput = rightType();
  auto outputType = joinedType();
  auto probeNode = std::make_shared<core::NestedLoopJoinNode>(
      core::PlanNodeId{nextNodeId()},
      core::JoinType::kInner,
      nullptr,
      std::make_shared<EmptyNode>(leftInput),
      std::make_shared<EmptyNode>(rightInput),
      outputType);
  auto hashSpec = std::make_shared<exec::HashPartitionFunctionSpec>(
      leftInput, std::vector<column_index_t>{0, 2});
  auto node = std::make_shared<const StreamWindowJoinNode>(
      core::PlanNodeId{nextNodeId()},
      std::vector<core::PlanNodePtr>{
          std::make_shared<EmptyNode>(leftInput),
          std::make_shared<EmptyNode>(rightInput)},
      hashSpec,
      hashSpec,
      probeNode,
      outputType,
      /*numPartitions=*/1,
      /*leftWindowEndIndex=*/2,
      /*rightWindowEndIndex=*/2,
      std::vector<column_index_t>{0, 2},
      std::vector<column_index_t>{0, 2},
      /*shiftTimeZone=*/0,
      /*joinType=*/static_cast<int>(core::JoinType::kLeft));
  auto obj = node->serialize();
  auto back = std::dynamic_pointer_cast<const StreamWindowJoinNode>(
      StreamWindowJoinNode::create(obj, nullptr));
  ASSERT_NE(nullptr, back);
  ASSERT_EQ(static_cast<int>(core::JoinType::kLeft), back->joinType());
  ASSERT_EQ(std::vector<column_index_t>({0, 2}), back->leftKeyChannels());
}

} // namespace
} // namespace facebook::velox::stateful::test
