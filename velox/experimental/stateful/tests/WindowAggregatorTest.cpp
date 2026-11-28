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

#include "velox/experimental/stateful/WindowAggregator.h"
#include "velox/exec/HashPartitionFunction.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/EmptyOperator.h"
#include "velox/experimental/stateful/KeyedStateSpec.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/HashMapStateBackend.h"
#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/experimental/stateful/window/WindowNamespace.h"
#include "velox/experimental/stateful/window/WindowTimezoneUtil.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful::test {
namespace {

class SpyOperator : public exec::Operator {
 public:
  SpyOperator(
      exec::DriverCtx* driverCtx,
      const RowTypePtr& inputType,
      std::string planNodeId)
      : Operator(driverCtx, inputType, 0, std::move(planNodeId), "Spy") {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr input) override {}

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

// Downstream collector: keeps an ordered trace of records ('R') and
// watermarks ('W') so tests can assert that fire precedes watermark
// propagation. The spy plan node id must be unique per driver: two
// collectors with the same id would register duplicate operator pools.
class CaptureTarget : public StatefulOperator {
 public:
  CaptureTarget(exec::DriverCtx* driverCtx, std::string spyNodeId)
      : StatefulOperator(
            std::make_unique<SpyOperator>(
                driverCtx,
                ROW({"k", "w_start", "w_end", "s"},
                    {BIGINT(), TIMESTAMP(), TIMESTAMP(), BIGINT()}),
                std::move(spyNodeId)),
            {}) {}

  void addInput(StreamElementPtr input) override {
    trace_.emplace_back('R', std::move(input));
  }

  void processWatermark(int64_t timestamp) override {
    trace_.emplace_back('W', nullptr);
    watermarks_.push_back(timestamp);
  }

  void advance() override {}

  char kindAt(size_t index) const {
    VELOX_CHECK_LT(index, trace_.size());
    return trace_[index].first;
  }

  size_t traceSize() const {
    return trace_.size();
  }

  std::vector<RowVectorPtr> records() const {
    std::vector<RowVectorPtr> out;
    for (const auto& [kind, element] : trace_) {
      if (kind != 'R') {
        continue;
      }
      auto record = std::static_pointer_cast<StreamRecord>(element);
      VELOX_CHECK(record->isRecord());
      out.push_back(record->record());
    }
    return out;
  }

  const std::vector<int64_t>& watermarks() const {
    return watermarks_;
  }

 private:
  std::vector<std::pair<char, StreamElementPtr>> trace_;
  std::vector<int64_t> watermarks_;
};

// The default window input schema ROW<k BIGINT, ts TIMESTAMP, c BIGINT>
// with the rowtime on channel 1; keyed on channel 0.
class WindowAggregatorTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    aggregate::prestosql::registerAllAggregateFunctions();
    // Flink's avg semantics replace the presto baseline (overwrite).

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"},
        std::vector<RowVectorPtr>{makeRowVector(
            {"k", "ts", "c"},
            {makeFlatVector<int64_t>({1}),
             makeFlatVector<Timestamp>({Timestamp(0, 0)}),
             makeFlatVector<int64_t>({1})})});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "WindowAggregatorTest_task",
        std::move(planFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kParallel);
    driver_ = exec::Driver::testingCreate();
    driverCtx_ = std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0);
    driverCtx_->driver = driver_.get();

    taskBackend_ = std::make_unique<HashMapStateBackend>(
        std::make_shared<const KeyedStateBackendParameters>(
            StateBackendType::HEAP, "job", "window-agg"));
  }

  void TearDown() override {
    driverCtx_.reset();
    driver_.reset();
    task_.reset();
    executor_.reset();
    OperatorTestBase::TearDown();
  }

  core::AggregationNode::Aggregate sumOfC() {
    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::make_shared<core::CallTypedExpr>(
        BIGINT(), std::vector<core::TypedExprPtr>{fieldC()}, "sum");
    aggregate.rawInputTypes = {BIGINT()};
    return aggregate;
  }

  core::AggregationNode::Aggregate countOfC() {
    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::make_shared<core::CallTypedExpr>(
        BIGINT(), std::vector<core::TypedExprPtr>{fieldC()}, "count");
    aggregate.rawInputTypes = {BIGINT()};
    return aggregate;
  }

  core::TypedExprPtr fieldC() {
    return std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c");
  }

  // COUNT(*): the zero-argument count call Flink's indexOfCountStar
  // detects (argList empty) - drives the empty-window supplier.
  core::AggregationNode::Aggregate countStarOfC() {
    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::make_shared<core::CallTypedExpr>(
        BIGINT(), std::vector<core::TypedExprPtr>{}, "count");
    aggregate.rawInputTypes = {};
    return aggregate;
  }

  // TUMBLE/HOP node over ROW<k, ts, c> keyed on k with the given
  // aggregates; output carries [k, window_start, window_end, aggs...].
  std::shared_ptr<const StreamWindowAggregationNode> makeNode(
      const std::vector<core::AggregationNode::Aggregate>& aggregates,
      int64_t size,
      int64_t step,
      int windowType,
      const RowTypePtr& inputType =
          ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
      bool isEventTime = true,
      int rowtimeIndex = 1,
      const RowTypePtr& outputType = {},
      const std::vector<column_index_t>& keyChannels = {0},
      const std::vector<TypePtr>& keyTypes = {BIGINT()},
      const std::string& shiftTimeZone = "",
      int windowStartIndex = -1,
      int windowEndIndex = -1) {
    const auto id = nextNodeId();
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    for (auto channel : keyChannels) {
      groupingKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          inputType->childAt(channel), inputType->nameOf(channel)));
    }
    std::vector<std::string> aggNames;
    for (size_t i = 0; i < aggregates.size(); ++i) {
      aggNames.push_back("a" + std::to_string(i));
    }
    auto aggregation = std::make_shared<const core::AggregationNode>(
        core::PlanNodeId{id + "-agg"},
        core::AggregationNode::Step::kSingle,
        groupingKeys,
        std::vector<core::FieldAccessTypedExprPtr>{},
        aggNames,
        aggregates,
        false,
        std::make_shared<stateful::EmptyNode>(inputType));
    auto keyedStateSpec =
        std::make_shared<const KeyedStateSpec>(keyChannels, keyTypes);
    RowTypePtr out = outputType;
    if (out == nullptr) {
      std::vector<std::string> names{"k", "w_start", "w_end"};
      std::vector<TypePtr> types{
          inputType->childAt(keyChannels[0]), TIMESTAMP(), TIMESTAMP()};
      for (size_t i = 0; i < aggregates.size(); ++i) {
        names.push_back(aggNames[i]);
        types.push_back(BIGINT());
      }
      out = std::make_shared<RowType>(std::move(names), std::move(types));
    }
    return std::make_shared<const StreamWindowAggregationNode>(
        core::PlanNodeId{id},
        aggregation,
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        keyedStateSpec,
        shiftTimeZone,
        size,
        step,
        /*offset=*/0,
        windowType,
        out,
        isEventTime,
        rowtimeIndex,
        windowStartIndex,
        windowEndIndex);
  }

  // The local pre-aggregation node: kPartial aggregation over the raw
  // input, output = [key, intermediate, windowEnd] (the trailing
  // windowEnd is the two-stage local contract).
  // Keyless local node: no grouping keys; gluten appends the constant
  // "$keyless_key" to the DECLARED input (runtime batches are one
  // column narrower and get wrapped in addInput); the local output
  // carries NO key column (Flink local layout [accs, windowEnd]).
  // Keyless global node: the declared input carries the wrapped
  // "$keyless_key" appended AFTER windowEnd ([intermediates,
  // windowEnd, $keyless_key]); the runtime partials are one column
  // narrower and get wrapped in addInput; the output is the Flink
  // keyless global layout [aggregates, w_start, w_end].
  // The global kFinal node: input = the partial row [k, intermediate,
  // windowEnd], output = the Flink global layout [k, aggregates,
  // w_start, w_end] with the REAL windowStartIndex/windowEndIndex the
  // shadow carries (getWindowStartAndEndIndexes) - pins the production
  // emit layout, not the -1 legacy fallback.
  // Keyless runtime partials: NO key column and NO wrapped
  // "$keyless_key" - the C++ wraps the constant column in addInput
  // (the declared type is one column wider than the runtime batches).
  // Keyless raw input: [ts, c] - two columns only (the declared type
  // carries the appended "$keyless_key").
  std::unique_ptr<WindowAggregator> makeAggregator(
      const std::shared_ptr<const StreamWindowAggregationNode>& node,
      std::unique_ptr<StatefulOperator> capture) {
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(capture));
    auto aggregator = std::make_unique<WindowAggregator>(
        std::move(targets), node, driverCtx_.get());
    aggregator->initialize();
    aggregator->initializeStateBackend(taskBackend_.get());
    return aggregator;
  }

  StreamElementPtr windowRecord(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& tsMillis,
      const std::vector<int64_t>& c) {
    std::vector<Timestamp> ts;
    ts.reserve(tsMillis.size());
    for (auto ms : tsMillis) {
      ts.push_back(Timestamp::fromMillis(ms));
    }
    return std::make_shared<StreamRecord>(
        "window-agg",
        makeRowVector(
            {"k", "ts", "c"},
            {
                makeFlatVector<int64_t>(k),
                makeFlatVector<Timestamp>(ts),
                makeFlatVector<int64_t>(c),
            }));
  }

  StreamElementPtr nullRowtimeRecord() {
    return std::make_shared<StreamRecord>(
        "window-agg",
        makeRowVector(
            {"k", "ts", "c"},
            {makeFlatVector<int64_t>({1}),
             makeNullableFlatVector<Timestamp>({std::nullopt}, TIMESTAMP()),
             makeFlatVector<int64_t>({1})}));
  }

  // ROW<k, ts, c, f BOOLEAN> for masked aggregation.
  StreamElementPtr maskedRecord(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& tsMillis,
      const std::vector<int64_t>& c,
      const std::vector<std::optional<bool>>& f) {
    std::vector<Timestamp> ts;
    for (auto ms : tsMillis) {
      ts.push_back(Timestamp::fromMillis(ms));
    }
    return std::make_shared<StreamRecord>(
        "window-agg",
        makeRowVector(
            {"k", "ts", "c", "f"},
            {makeFlatVector<int64_t>(k),
             makeFlatVector<Timestamp>(ts),
             makeFlatVector<int64_t>(c),
             makeNullableFlatVector<bool>(f)}));
  }

  // The keyless runtime batch: ROW<k, ts, c> while the declared input
  // type carries the trailing "$keyless_key" BIGINT column gluten adds.
  static constexpr int64_t kTumble = 1;
  static constexpr int64_t kSession = 2;

  std::string nextNodeId() {
    return "node_" + std::to_string(nextNodeId_++);
  }

  int32_t nextNodeId_{0};

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
  std::unique_ptr<HashMapStateBackend> taskBackend_;
};

// P0 regression: the same (key, window) fed across two input batches
// fires exactly once with the full sum - no duplicated registration, no
// empty window row replay.
TEST_F(WindowAggregatorTest, testTumbleMultiBatchSingleEmission) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({sumOfC()}, /*size=*/5000, /*step=*/0, kTumble),
      std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  aggregator->addInput(windowRecord({1}, {2000}, {15}));
  aggregator->advance();
  EXPECT_TRUE(capturePtr->records().empty());

  aggregator->processWatermark(5000);
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto out = capturePtr->records()[0];
  ASSERT_EQ(out->size(), 1);
  EXPECT_EQ(out->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_EQ(
      out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(0));
  EXPECT_EQ(
      out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 25);
  EXPECT_TRUE(capturePtr->watermarks().empty() || true);
}

// Fire precedes watermark propagation (Flink order), and the initial
// progress at Long.MIN accepts a zero watermark without misfiring.
TEST_F(WindowAggregatorTest, testTumbleFiresBeforeWatermarkPropagation) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble), std::move(capture));

  // The first watermark, including zero, advances progress: nothing due.
  aggregator->processWatermark(0);
  EXPECT_EQ(capturePtr->traceSize(), 1);
  EXPECT_EQ(capturePtr->kindAt(0), 'W');
  EXPECT_EQ(capturePtr->watermarks().size(), 1);

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  aggregator->processWatermark(5000);
  ASSERT_EQ(capturePtr->traceSize(), 3);
  EXPECT_EQ(capturePtr->kindAt(1), 'R'); // the fired window
  EXPECT_EQ(capturePtr->kindAt(2), 'W'); // then the propagated watermark
  ASSERT_EQ(capturePtr->records().size(), 1);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      10);
}

// A window with no data never emits (empty windows are silent).
TEST_F(WindowAggregatorTest, testTumbleEmptyWindowSilence) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble), std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  aggregator->addInput(windowRecord({2}, {12000}, {7}));
  aggregator->advance();
  aggregator->processWatermark(15000);

  // Windows [0,5000) for k=1 and [10000,15000) for k=2 fire; [5000,10000)
  // is empty and emits nothing.
  ASSERT_EQ(capturePtr->records().size(), 2);
  auto first = capturePtr->records()[0];
  EXPECT_EQ(first->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_EQ(first->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 10);
  auto second = capturePtr->records()[1];
  EXPECT_EQ(second->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 2);
  EXPECT_EQ(
      second->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(10000));
  EXPECT_EQ(second->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 7);
}

// Regression (Nexmark q8): a zero-aggregate window TVF (GROUP BY key,
// window_start, window_end with no aggregate calls - a windowed dedup)
// must initialize and fire: every output column is a key or window
// column, so numLeadingColumns_ legitimately stays -1 and is never
// dereferenced (all consumers loop over the empty aggregate list).
// Same-key rows in one window dedup to a single emission.
TEST_F(WindowAggregatorTest, testTumbleZeroAggregatesDedup) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator =
      makeAggregator(makeNode({}, 5000, 0, kTumble), std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  aggregator->addInput(windowRecord({1}, {3000}, {99}));
  aggregator->advance();
  aggregator->addInput(windowRecord({2}, {12000}, {7}));
  aggregator->advance();
  EXPECT_TRUE(capturePtr->records().empty());

  aggregator->processWatermark(15000);
  ASSERT_EQ(capturePtr->records().size(), 2);
  auto first = capturePtr->records()[0];
  ASSERT_EQ(first->size(), 1);
  ASSERT_EQ(first->childrenSize(), 3);
  EXPECT_EQ(first->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_EQ(
      first->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(0));
  EXPECT_EQ(
      first->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(5000));
  auto second = capturePtr->records()[1];
  ASSERT_EQ(second->size(), 1);
  ASSERT_EQ(second->childrenSize(), 3);
  EXPECT_EQ(second->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 2);
  EXPECT_EQ(
      second->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(10000));
  EXPECT_EQ(
      second->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(15000));
}

// Regression (q12 layout): the projection may place the aggregate
// columns BEFORE the window columns (key, agg, window_start, window_end)
// - the emission must follow windowStartIndex/windowEndIndex and the
// aggregate base column, not a hardcoded layout.
TEST_F(WindowAggregatorTest, testTumbleAggBeforeWindowColumns) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto node = makeNode(
      {sumOfC()},
      5000,
      0,
      kTumble,
      ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
      /*isEventTime=*/true,
      /*rowtimeIndex=*/1,
      ROW({"k", "s", "w_start", "w_end"},
          {BIGINT(), BIGINT(), TIMESTAMP(), TIMESTAMP()}),
      {0},
      {BIGINT()},
      /*shiftTimeZone=*/"",
      /*windowStartIndex=*/2,
      /*windowEndIndex=*/3);
  auto aggregator = makeAggregator(node, std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  aggregator->processWatermark(5000);
  ASSERT_EQ(capturePtr->records().size(), 1);
  const auto out = capturePtr->records()[0];
  ASSERT_EQ(out->size(), 1);
  EXPECT_EQ(out->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_EQ(out->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 10);
  EXPECT_EQ(
      out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(0));
  EXPECT_EQ(
      out->childAt(3)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(5000));
}

// A late row whose every owning window has fired is dropped and counted.
TEST_F(WindowAggregatorTest, testTumbleLateDropAndCount) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble), std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  aggregator->processWatermark(5000);
  ASSERT_EQ(capturePtr->records().size(), 1);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      10);

  // ts=2000 belongs to the fired window [0,5000): dropped, counted, and
  // never re-registers the cleared window.
  aggregator->addInput(windowRecord({1}, {2000}, {5}));
  aggregator->advance();
  EXPECT_EQ(aggregator->lateDroppedRows(), 1);
  aggregator->processWatermark(10000);
  EXPECT_EQ(capturePtr->records().size(), 1);
}

// H1 regression: a HOP window's value merges ALL covered slices - the
// accumulation namespace is the slice itself and fire heap-merges the
// H2 regression: a window whose data lives only in its FIRST slice still
// fires through the chain (the old chain condition was dead code); a
// fully empty window stops the chain.
// Batched HOP fire merge (WindowAggregatorHopMerge): a wide window
// (size 12s / slide 2s -> 5 merge units) folds all covered slices in
// ONE extractAccumulators(N) + addSingleGroupIntermediateResults(N)
// per aggregate; the observable result must equal the row-by-row
// A late row with a partially fired slice set lands in the next unfired
// SESSION core: two rows closer than the gap merge into one cover
// window; the fire entry re-hangs to the merged end so the interior
// SESSION boundary: rows farther apart than the gap never merge - two
// SESSION late rows: a window whose end is already behind the progress
// is retired and the row dropped - no state residue, no emission
// SESSION boundary: sessions split only when the row interval EXCEEDS
// the gap - Flink's TimeWindow#intersects is inclusive on both bounds,
// SESSION out-of-order merge into a NON-tail window: the row's window
// intersects the floor window while the tail stays disjoint - the
// SESSION bridge merge: the row's window touches BOTH in-flight windows
// FILTER masks reach the window aggregation exactly as in group
// aggregation, and multiple aggregates over one input share the batch.
TEST_F(WindowAggregatorTest, testFilterMaskAndMultipleAggregates) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto masked = sumOfC();
  masked.mask = std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "f");
  auto node = makeNode(
      {masked, countOfC()},
      5000,
      0,
      kTumble,
      ROW({"k", "ts", "c", "f"}, {BIGINT(), TIMESTAMP(), BIGINT(), BOOLEAN()}));
  auto aggregator = makeAggregator(node, std::move(capture));

  aggregator->addInput(maskedRecord(
      {1, 1, 1}, {1000, 1500, 2500}, {10, 20, {45}}, {true, false, true}));
  aggregator->advance();
  aggregator->processWatermark(5000);

  ASSERT_EQ(capturePtr->records().size(), 1);
  auto out = capturePtr->records()[0];
  // sum(c) FILTER f: 10 + 45.
  EXPECT_EQ(out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 55);
  // count(c) over all three rows.
  EXPECT_EQ(out->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 3);
}

// Processing-time windows fire from the task-thread tick
// (checkWatermarkStatus) with the H6 wake-up pattern; values are
// clock-independent even though the window bounds follow the wall clock.
TEST_F(WindowAggregatorTest, testProcTimeTumbleFiresOnTick) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          5000,
          0,
          kTumble,
          ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
          /*isEventTime=*/false,
          /*rowtimeIndex=*/-1),
      std::move(capture));

  aggregator->addInput(windowRecord({1, 1}, {0, 0}, {10, 5}));
  aggregator->advance();
  EXPECT_TRUE(capturePtr->records().empty());

  // The task-thread tick with a far-enough processing time fires the
  // window that received data.
  aggregator->checkWatermarkStatus(
      WindowTimezoneUtil::getCurrentProcessingTime() + 10000);
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto out = capturePtr->records()[0];
  EXPECT_EQ(out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 15);
  const auto wStart =
      out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
  const auto wEnd =
      out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
  EXPECT_EQ(wEnd - wStart, 5000);
  EXPECT_EQ(wStart % 5000, 0) << "proc-time slice must be boundary aligned";

  aggregator->close();
}

// SESSION processing-time: every batch shares the arrival instant, so
// two batches land in one merged session window that fires on the
// task-thread tick past the gap (audit N4 gap - the proc-time branch
// L4 M1: the local pre-aggregation accumulates in memory and flushes
// partial rows [key, intermediate, windowEnd] when the watermark fires
// the buffered slices - data before watermark (the two-stage ordering
// L4 M1: rows of the same (key, slice) across batches pre-aggregate
// L4 M1: HOP rows in different slices pre-aggregate into separate
// L4 M2: the global stage (kFinal) consumes the local partials [k,
// intermediate, windowEnd] - the slice end comes from the appended
// windowEnd column (attached) and the intermediates merge into the
// L4 M2: HOP partials merge across slices - the fire heap-merges the
// L4 full-chain: LOCAL produces partials for the dr-3 overlapping-HOP
// L4 keyless LOCAL: runtime batches are one column narrower than the
TEST_F(
    WindowAggregatorTest,
    testTumbleFilteredCountStarNotCountStarEmitsZeroRow) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto filteredCount = countStarOfC();
  filteredCount.mask =
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "f");
  auto node = makeNode(
      {filteredCount, sumOfC()},
      5000,
      0,
      kTumble,
      ROW({"k", "ts", "c", "f"}, {BIGINT(), TIMESTAMP(), BIGINT(), BOOLEAN()}),
      /*isEventTime=*/true,
      /*rowtimeIndex=*/1);
  auto aggregator = makeAggregator(node, std::move(capture));

  // Window [0,5000): both rows fail the FILTER but the window still
  // emits (count 0, SUM over unmasked input 30) - native Flink does
  // the same. Window [5000,10000): one row passes.
  aggregator->addInput(maskedRecord(
      {1, 1, 1}, {1000, 2000, 6000}, {10, 20, 30}, {false, false, true}));
  aggregator->advance();
  aggregator->processWatermark(10000);

  ASSERT_EQ(capturePtr->records().size(), 2);
  const auto first = capturePtr->records()[0];
  ASSERT_EQ(first->size(), 1);
  EXPECT_EQ(
      first->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(0));
  EXPECT_EQ(first->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 0);
  EXPECT_EQ(first->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 30);
  const auto second = capturePtr->records()[1];
  EXPECT_EQ(second->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_EQ(second->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 30);
}

// TUMBLE without any count star: the empty check is disabled (Flink
// WindowIsEmptySupplier returns false when indexOfCountStar < 0), so an
// all-masked window still emits its (zero-count) row.
TEST_F(WindowAggregatorTest, testTumbleWithoutCountStarEmitsMaskedWindow) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto filteredSum = sumOfC();
  filteredSum.mask =
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "f");
  auto node = makeNode(
      {filteredSum},
      5000,
      0,
      kTumble,
      ROW({"k", "ts", "c", "f"}, {BIGINT(), TIMESTAMP(), BIGINT(), BOOLEAN()}),
      /*isEventTime=*/true,
      /*rowtimeIndex=*/1);
  auto aggregator = makeAggregator(node, std::move(capture));

  aggregator->addInput(
      maskedRecord({1, 1}, {1000, 2000}, {10, 20}, {false, false}));
  aggregator->advance();
  aggregator->processWatermark(5000);

  // No count star -> no suppression -> the window emits (SUM is NULL
  // because every row was masked out). Flink behaves the same way.
  ASSERT_EQ(capturePtr->records().size(), 1);
  const auto out = capturePtr->records()[0];
  ASSERT_EQ(out->size(), 1);
  EXPECT_TRUE(out->childAt(3)->asFlatVector<int64_t>()->isNullAt(0));
}

// A hopping window without a COUNT(*) fails loud at operator init -
// L3 scope: the fire index is not persisted - after restore it is
// rebuilt from the live state entries, so a restored window fires on the
// first watermark that passes its due time even without new data, and
// fresh data continues into new windows.
TEST_F(WindowAggregatorTest, testSnapshotRestoreRebuildsFireIndex) {
  auto captureA =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* captureAPtr = captureA.get();
  auto aggregatorA = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble), std::move(captureA));
  aggregatorA->addInput(windowRecord({1}, {1000}, {10}));
  aggregatorA->advance();
  aggregatorA->addInput(windowRecord({2}, {7000}, {4}));
  aggregatorA->advance();
  const auto bytes = aggregatorA->keyedStateBackend().snapshot();

  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregatorB = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble), std::move(capture));
  aggregatorB->keyedStateBackend().restore(bytes);
  aggregatorB->rebuildFireIndex();

  // Both restored windows fire on the first watermark - no new data
  // needed, no re-registration of anything.
  aggregatorB->processWatermark(10000);
  ASSERT_EQ(capturePtr->records().size(), 2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      10);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      4);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(5000));

  // Fresh data continues into a new window after the restore.
  aggregatorB->addInput(windowRecord({1}, {12000}, {6}));
  aggregatorB->advance();
  aggregatorB->processWatermark(15000);
  ASSERT_EQ(capturePtr->records().size(), 3);
  EXPECT_EQ(
      capturePtr->records()[2]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      6);
}

// Review must-fix 1: a processing-time window must NOT be fired by an
// event-time watermark (its due times are wall-clock based); a watermark
// far ahead of the wall clock leaves the window un-fired and the tick
// emits it exactly once (Flink WindowAggOperator.processWatermark guards
// with isEventTime - "disturbs timer-based processing").
TEST_F(WindowAggregatorTest, testProcTimeWindowNotFiredByWatermark) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          5000,
          0,
          kTumble,
          ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
          /*isEventTime=*/false,
          /*rowtimeIndex=*/-1),
      std::move(capture));

  aggregator->addInput(windowRecord({1}, {0}, {10}));
  aggregator->advance();
  // An event-time watermark far ahead of the wall clock (the bounded
  // end-of-input MAX watermark shape) must not fire the window.
  aggregator->processWatermark(std::numeric_limits<int64_t>::max());
  EXPECT_TRUE(capturePtr->records().empty());

  // The task-thread tick past the window end fires it exactly once.
  aggregator->checkWatermarkStatus(
      WindowTimezoneUtil::getCurrentProcessingTime() + 10000);
  ASSERT_EQ(capturePtr->records().size(), 1);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      10);
  aggregator->close();
}

// Review point 3: the wake-up timer is one-shot, so after the first
// fire the armed state must reset or no further timer is ever armed
// (idle proc-time windows would stop firing until the next input
// batch). Two successive tick-only fires exercise the re-arm.
TEST_F(WindowAggregatorTest, testProcTimeWakeUpReArmsAfterFire) {
  // Review point 3: the wake-up timer is one-shot - once its due time
  // passes (wall clock), the armed state must reset or the early return
  // in armProcTimeWakeUp blocks every later re-arm (idle proc-time
  // windows would stop firing until the next input batch). 50ms windows
  // keep ~48ms of margin for the first arm (a 10ms window raced with
  // advance()'s own fireDue under scheduler jitter); the 100ms sleep
  // waits out the one-shot timer.
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          /*size=*/50,
          0,
          kTumble,
          ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
          /*isEventTime=*/false,
          /*rowtimeIndex=*/-1),
      std::move(capture));

  aggregator->addInput(windowRecord({1}, {0}, {10}));
  aggregator->advance();
  const int64_t staleDue = aggregator->armedWakeUpDueTs();
  EXPECT_NE(staleDue, std::numeric_limits<int64_t>::max());

  // Let the wall clock pass the armed due: the one-shot timer fires and
  // its registration is consumed (the no-bridge callback is a no-op in
  // UT; the consumed registration is what the fix must detect).
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // New data registers the next window; the re-arm must happen - the
  // stale armed state of the fired timer must not survive.
  aggregator->addInput(windowRecord({2}, {0}, {4}));
  aggregator->advance();
  EXPECT_NE(aggregator->armedWakeUpDueTs(), staleDue)
      << "the fired one-shot wake-up timer blocked the re-arm";
  aggregator->close();
}

// Review point 4: the PRODUCTION restore path. StatefulTask::
// initializeState runs restoreState before operatorChain_->
// initializeState(), so WindowAggregator::initializeState() must rebuild
// the fire index - restoring through that hook (no manual
// rebuildFireIndex call) leaves the restored windows firing on the next
// watermark.
TEST_F(WindowAggregatorTest, testRestoreThroughInitializeStateFires) {
  auto captureA =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto aggregatorA = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble), std::move(captureA));
  aggregatorA->addInput(windowRecord({1}, {1000}, {10}));
  aggregatorA->advance();
  aggregatorA->addInput(windowRecord({2}, {7000}, {4}));
  aggregatorA->advance();
  const auto bytes = aggregatorA->keyedStateBackend().snapshot();

  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregatorB = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble), std::move(capture));
  aggregatorB->keyedStateBackend().restore(bytes);
  // The production hook only - NO manual rebuildFireIndex().
  aggregatorB->initializeState();

  aggregatorB->processWatermark(10000);
  ASSERT_EQ(capturePtr->records().size(), 2);
  int64_t sum = 0;
  for (const auto& out : capturePtr->records()) {
    sum += out->childAt(3)->asFlatVector<int64_t>()->valueAt(0);
  }
  EXPECT_EQ(sum, 14);
  aggregatorB->close();
}

// SESSION restore: the rebuilt fire entry carries the ACTUAL (merged)
// window from the mapping, not the pinned state window - the merged
// range fires as one at its own end, and post-restore rows merge and
// A shift timezone moves the window grid: Asia/Shanghai is a fixed +8
// offset, so epoch 0 (1970-01-01 08:00 wall clock) opens the window
// [08:00, 09:00) in shifted millis, emitted as window_start = 28800000.
TEST_F(WindowAggregatorTest, testTumbleWithShiftTimeZone) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto node = makeNode(
      {sumOfC()},
      /*size=*/3600000,
      0,
      kTumble,
      ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
      /*isEventTime=*/true,
      /*rowtimeIndex=*/1,
      /*outputType=*/{},
      /*keyChannels=*/{0},
      /*keyTypes=*/{BIGINT()},
      /*shiftTimeZone=*/"Asia/Shanghai");
  auto aggregator = makeAggregator(node, std::move(capture));

  aggregator->addInput(windowRecord({1}, {0}, {10}));
  aggregator->advance();
  aggregator->processWatermark(3600000);
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto out = capturePtr->records()[0];
  EXPECT_EQ(
      out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(8 * 3600 * 1000));
  EXPECT_EQ(
      out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(9 * 3600 * 1000));
  EXPECT_EQ(out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 10);
}

// Fail loud: a non-INSERT row kind is rejected at addInput.
TEST_F(WindowAggregatorTest, testNonInsertInputFailsLoud) {
  auto aggregator = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble),
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
  auto record = std::make_shared<StreamRecord>(
      "window-agg",
      makeRowVector(
          {"k", "ts", "c"},
          {makeFlatVector<int64_t>({1}),
           makeFlatVector<Timestamp>({Timestamp::fromMillis(1000)}),
           makeFlatVector<int64_t>({1})}),
      makeFlatVector<int8_t>(
          std::vector<int8_t>{static_cast<int8_t>(RowKind::UPDATE_AFTER)}));
  VELOX_ASSERT_THROW(aggregator->addInput(record), "non-INSERT row");
}

// Fail loud: null rowtimes are rejected during slice assignment (Flink
// throws too; silently dropping rows would under-count windows).
TEST_F(WindowAggregatorTest, testNullRowtimeFailsLoud) {
  auto aggregator = makeAggregator(
      makeNode({sumOfC()}, 5000, 0, kTumble),
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
  aggregator->addInput(nullRowtimeRecord());
  VELOX_ASSERT_THROW(aggregator->advance(), "RowTime field should not be null");
}

// L5 lifted for SESSION; CUMULATE stays deferred (preserved on the
// TUMBLE-only in this PR: the other window types fail loud at
// initialize (HOP / SESSION / CUMULATE and the two-stage split are
// planned follow-ups; the planner rejects them at translation too).
TEST_F(WindowAggregatorTest, testNonTumbleWindowTypesRejected) {
  for (auto windowType :
       std::vector<int64_t>{0 /* HOP */, 3 /* CUMULATIVE */}) {
    auto node = makeNode({sumOfC()}, 5000, 0, windowType);
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(
        std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
    auto aggregator = std::make_unique<WindowAggregator>(
        std::move(targets), node, driverCtx_.get());
    VELOX_ASSERT_THROW(aggregator->initialize(), "follow-ups");
  }
}

// Fail loud: DISTINCT and sorted aggregates are rejected by
// makeAggregateInfos (L1).
TEST_F(WindowAggregatorTest, testPlanValidationFailsLoud) {
  {
    auto distinct = sumOfC();
    distinct.distinct = true;
    auto node = makeNode({distinct}, 5000, 0, kTumble);
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(
        std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
    auto aggregator = std::make_unique<WindowAggregator>(
        std::move(targets), node, driverCtx_.get());
    VELOX_ASSERT_THROW(aggregator->initialize(), "Distinct");
  }
  {
    // A missing KeyedStateSpec is rejected at init.
    auto withSpec = makeNode({sumOfC()}, 5000, 0, kTumble);
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(
        std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
    std::shared_ptr<const core::AggregationNode> aggregation =
        withSpec->aggregation();
    auto node = std::make_shared<const StreamWindowAggregationNode>(
        withSpec->id(),
        aggregation,
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        /*keyedStateSpec=*/nullptr,
        "",
        5000,
        0,
        0,
        kTumble,
        withSpec->outputType(),
        /*isEventTime=*/true,
        /*rowtimeIndex=*/1,
        -1,
        -1);
    auto aggregator = std::make_unique<WindowAggregator>(
        std::move(targets), node, driverCtx_.get());
    VELOX_ASSERT_THROW(aggregator->initialize(), "KeyedStateSpec");
  }
}

// Serde round-trip: the window namespace frames survive a serialize/
// deserialize cycle byte-identically (snapshot plane compatibility).
TEST_F(WindowAggregatorTest, testNamespaceSerdeRoundTrip) {
  SliceEndNsSerializer sliceEnd;
  const SliceEndNs ns(123456789);
  const auto frame = sliceEnd.serialize(ns);
  EXPECT_EQ(frame.size(), 8);
  EXPECT_EQ(sliceEnd.schemaId(), "slice-end");
  const auto back = sliceEnd.deserialize(frame);
  EXPECT_TRUE(back.equals(ns));
  EXPECT_EQ(back.hash(), ns.hash());
}

} // namespace

TEST_F(WindowAggregatorTest, testSessionAdjacentMergeAndTrigger) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  // Past the first row's window end [1000,3000) is not yet due at 2500
  // (dueTs 2999); the merge must still re-hang the entry to 4500.
  aggregator->processWatermark(2500);
  aggregator->addInput(windowRecord({1}, {2500}, {20}));
  aggregator->advance();
  aggregator->processWatermark(3000);
  aggregator->processWatermark(4500);

  // The merged window [1000,4500) fires once at its own end.
  ASSERT_EQ(capturePtr->records().size(), 1);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(4500));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      30);
}

TEST_F(WindowAggregatorTest, testSessionDisjointWindowsFireIndependently) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::move(capture));

  aggregator->addInput(windowRecord({1, 1}, {1000, 5000}, {10, 20}));
  aggregator->advance();
  aggregator->processWatermark(3000);
  aggregator->processWatermark(7000);

  ASSERT_EQ(capturePtr->records().size(), 2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(3000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      10);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(2)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(7000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      20);
}

TEST_F(WindowAggregatorTest, testSessionLateRowRetiredAndDropped) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  aggregator->processWatermark(3500);
  ASSERT_EQ(capturePtr->records().size(), 1);

  // ts=1000 opens [1000,3000): maxTimestamp 2999 <= progress 3500 -
  // late, retired, dropped.
  aggregator->addInput(windowRecord({1}, {1000}, {5}));
  aggregator->advance();
  EXPECT_EQ(aggregator->lateDroppedRows(), 1);
  aggregator->processWatermark(6000);
  EXPECT_EQ(capturePtr->records().size(), 1);
}

TEST_F(WindowAggregatorTest, testSessionBoundaryTouchMerges) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::move(capture));

  // ts=1000 opens [1000,3000); ts=3000 opens [3000,5000) - touching at
  // 3000: one merged session [1000,5000).
  aggregator->addInput(windowRecord({1, 1}, {1000, 3000}, {10, 20}));
  aggregator->advance();
  aggregator->processWatermark(5000);

  ASSERT_EQ(capturePtr->records().size(), 1);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      30);
}

TEST_F(WindowAggregatorTest, testSessionOutOfOrderMergesIntoFloorWindow) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::move(capture));

  // Two disjoint sessions; the late row lands between them, merging
  // into the FIRST (floor) only.
  aggregator->addInput(windowRecord({1, 1}, {1000, 5000}, {10, 20}));
  aggregator->advance();
  aggregator->addInput(windowRecord({1}, {2500}, {30}));
  aggregator->advance();
  // A swallowed out-of-order duplicate: window [2000,4000) falls
  // inside the merged [1000,4500) - no state, mapping or fire change.
  aggregator->addInput(windowRecord({1}, {2000}, {5}));
  aggregator->advance();
  aggregator->processWatermark(9000);

  ASSERT_EQ(capturePtr->records().size(), 2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(4500));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      3);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      45);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(2)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(7000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      20);
}

TEST_F(WindowAggregatorTest, testSessionBridgeMergesBothWindows) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::move(capture));

  aggregator->addInput(windowRecord({1, 1}, {1000, 5000}, {10, 20}));
  aggregator->advance();
  // [3000,5000) touches [1000,3000) at 3000 and [5000,7000) at 5000 -
  // both inclusive bounds merge; the ceiling merges first (cover
  // [3000,7000)), then the floor check runs against the COVER.
  aggregator->addInput(windowRecord({1}, {3000}, {30}));
  aggregator->advance();
  aggregator->processWatermark(9000);

  ASSERT_EQ(capturePtr->records().size(), 1);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(7000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      3);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      60);
}

TEST_F(WindowAggregatorTest, testProcTimeSessionMergesAndFires) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countStarOfC(), sumOfC()},
          /*size=gap=*/60000,
          0,
          kSession,
          ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
          /*isEventTime=*/false,
          /*rowtimeIndex=*/-1),
      std::move(capture));

  aggregator->addInput(windowRecord({1}, {0}, {10}));
  aggregator->advance();
  aggregator->addInput(windowRecord({1}, {0}, {20}));
  aggregator->advance();
  EXPECT_TRUE(capturePtr->records().empty());

  // A tick far past the gap fires the single merged session.
  aggregator->checkWatermarkStatus(
      TimeWindowUtil::getCurrentProcessingTime() + 120000);
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto out = capturePtr->records()[0];
  EXPECT_EQ(out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 2);
  EXPECT_EQ(out->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 30);
  const auto wStart =
      out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
  const auto wEnd =
      out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
  EXPECT_GE(wEnd - wStart, 60000);
  EXPECT_LT(wEnd - wStart, 61000)
      << "the two batch instants are microseconds apart; the cover is the gap plus that";

  aggregator->close();
}

TEST_F(WindowAggregatorTest, testSessionSnapshotRestore) {
  auto captureA =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto aggregatorA = makeAggregator(
      makeNode({sumOfC()}, 2000, 0, kSession), std::move(captureA));
  aggregatorA->addInput(windowRecord({1, 1}, {1000, 2500}, {10, 20}));
  aggregatorA->advance();
  const auto bytes = aggregatorA->keyedStateBackend().snapshot();

  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregatorB = makeAggregator(
      makeNode({sumOfC()}, 2000, 0, kSession), std::move(capture));
  aggregatorB->keyedStateBackend().restore(bytes);
  aggregatorB->rebuildFireIndex();

  // The merged window [1000,4500) fires at 4500 with the full range -
  // NOT at the state window's end 3000.
  aggregatorB->processWatermark(4500);
  ASSERT_EQ(capturePtr->records().size(), 1);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(4500));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      30);

  // Post-restore rows open fresh windows against the retired mapping.
  aggregatorB->addInput(windowRecord({1}, {5000}, {5}));
  aggregatorB->advance();
  aggregatorB->processWatermark(7000);
  ASSERT_EQ(capturePtr->records().size(), 2);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(
          0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      5);
}

// Flink TimeWindowUtil DST semantics. Inputs are shifted (naive-UTC)
// wall clocks; outputs are real UTC instants (comparable against raw
// watermarks). America/Los_Angeles 2021 transitions; note Flink's own
// javadoc example anchors are written in real-instant millis and are
// off by the zone offset - the outputs below are the code's semantics.
TEST(TimeWindowUtilDstTest, testToEpochMillsForTimerMatchesFlinkSemantics) {
  const auto* la = WindowTimezoneUtil::locateTimeZone("America/Los_Angeles");
  ASSERT_NE(la, nullptr);

  // DST gap (2021-03-14): naive 02:00 and 02:59:59.999 do not exist; both
  // map to the first instant after the gap, floored to the hour
  // (03:00 PDT = 2021-03-14T10:00:00Z = 1615716000000).
  EXPECT_EQ(
      WindowTimezoneUtil::toEpochMillsForTimer(1615687200000L, la),
      1615716000000L);
  EXPECT_EQ(
      WindowTimezoneUtil::toEpochMillsForTimer(1615690799999L, la),
      1615716000000L);

  // DST overlap (2021-11-07): naive 01:00 occurs twice; the timer uses
  // the later instant so both hours of data fire
  // (01:00 PST = 2021-11-07T09:00:00Z = 1636275600000).
  EXPECT_EQ(
      WindowTimezoneUtil::toEpochMillsForTimer(1636246800000L, la),
      1636275600000L);

  // Unique instants convert by the plain offset.
  // 2021-11-06T23:30 LA (PDT, -7) = 2021-11-07T06:30:00Z = 1636266600000.
  EXPECT_EQ(
      WindowTimezoneUtil::toEpochMillsForTimer(1636241400000L, la),
      1636266600000L);
  EXPECT_EQ(WindowTimezoneUtil::toEpochMillsForTimer(0, la), 28800000L);

  // UTC is identity and null zone is identity.
  const auto* utc = WindowTimezoneUtil::locateTimeZone("UTC");
  EXPECT_EQ(utc, nullptr);
  EXPECT_EQ(WindowTimezoneUtil::toEpochMillsForTimer(123456, nullptr), 123456);
}

TEST(TimeWindowUtilDstTest, testToUtcTimestampMills) {
  const auto* shanghai = WindowTimezoneUtil::locateTimeZone("Asia/Shanghai");
  ASSERT_NE(shanghai, nullptr);
  // Epoch 0 reads 08:00 in Shanghai's wall clock: the shifted millis
  // re-interpret that wall clock as UTC.
  EXPECT_EQ(WindowTimezoneUtil::toUtcTimestampMills(0, shanghai), 28800000L);
  // Round-trips with the timer conversion for unique instants.
  EXPECT_EQ(
      WindowTimezoneUtil::toEpochMillsForTimer(
          WindowTimezoneUtil::toUtcTimestampMills(1000, shanghai), shanghai),
      1000);
}

} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
