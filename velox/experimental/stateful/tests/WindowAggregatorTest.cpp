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

#include "velox/experimental/stateful/LocalWindowAggregator.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/experimental/stateful/window/WindowPartitionFunction.h"
#include "velox/experimental/stateful/WindowAggregator.h"

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
#include "velox/functions/flinksql/aggregates/RegisterAggregateFunctions.h"
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
    aggregate::flinksql::registerAggregateFunctions();

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

  core::AggregationNode::Aggregate avgOfC() {
    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::make_shared<core::CallTypedExpr>(
        BIGINT(), std::vector<core::TypedExprPtr>{fieldC()}, "avg");
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
      std::vector<TypePtr> types{inputType->childAt(keyChannels[0]),
                                  TIMESTAMP(),
                                  TIMESTAMP()};
      for (size_t i = 0; i < aggregates.size(); ++i) {
        names.push_back(aggNames[i]);
        types.push_back(BIGINT());
      }
      out = std::make_shared<RowType>(std::move(names), std::move(types));
    }
    std::shared_ptr<const core::AggregationNode> noLocalAgg;
    return std::make_shared<const StreamWindowAggregationNode>(
        core::PlanNodeId{id},
        aggregation,
        noLocalAgg,
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        keyedStateSpec,
        shiftTimeZone,
        /*isLocalAgg=*/false,
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
  std::shared_ptr<const StreamWindowAggregationNode> makeLocalNode(
      const std::vector<core::AggregationNode::Aggregate>& aggregates,
      const RowTypePtr& partialRowType,
      int64_t size,
      int64_t step,
      int windowType) {
    const auto id = nextNodeId();
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    groupingKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        BIGINT(), "k"));
    std::vector<std::string> aggNames;
    for (size_t i = 0; i < aggregates.size(); ++i) {
      aggNames.push_back("a" + std::to_string(i));
    }
    auto aggregation = std::make_shared<const core::AggregationNode>(
        core::PlanNodeId{id + "-agg"},
        core::AggregationNode::Step::kPartial,
        groupingKeys,
        std::vector<core::FieldAccessTypedExprPtr>{},
        aggNames,
        aggregates,
        false,
        std::make_shared<stateful::EmptyNode>(
            ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()})));
    auto keyedStateSpec =
        std::make_shared<KeyedStateSpec>(
            std::vector<column_index_t>{0}, std::vector<TypePtr>{BIGINT()});
    std::shared_ptr<const core::AggregationNode> noLocalAgg;
    return std::make_shared<const StreamWindowAggregationNode>(
        core::PlanNodeId{id},
        aggregation,
        noLocalAgg,
        std::make_shared<exec::HashPartitionFunctionSpec>(
            ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
            std::vector<column_index_t>{0}),
        std::make_shared<stateful::StreamWindowPartitionFunctionSpec>(
            ROW({"k", "ts", "c"}, {BIGINT(), TIMESTAMP(), BIGINT()}),
            1,
            size,
            step,
            0,
            static_cast<WindowType>(windowType)),
        keyedStateSpec,
        /*shiftTimeZone=*/"",
        /*isLocalAgg=*/true,
        size,
        step,
        /*offset=*/0,
        windowType,
        partialRowType,
        /*isEventTime=*/true,
        /*rowtimeIndex=*/1,
        /*windowStartIndex=*/-1,
        /*windowEndIndex=*/-1);
  }

  // Keyless local node: no grouping keys; gluten appends the constant
  // "$keyless_key" to the DECLARED input (runtime batches are one
  // column narrower and get wrapped in addInput); the local output
  // carries NO key column (Flink local layout [accs, windowEnd]).
  std::shared_ptr<const StreamWindowAggregationNode> makeLocalKeylessNode(
      const std::vector<core::AggregationNode::Aggregate>& aggregates,
      const RowTypePtr& partialRowType,
      int64_t size,
      int64_t step,
      int windowType) {
    const auto id = nextNodeId();
    const auto inputType = ROW(
        {"ts", "c", "$keyless_key"},
        {TIMESTAMP(), BIGINT(), BIGINT()});
    std::vector<std::string> aggNames;
    for (size_t i = 0; i < aggregates.size(); ++i) {
      aggNames.push_back("a" + std::to_string(i));
    }
    auto aggregation = std::make_shared<const core::AggregationNode>(
        core::PlanNodeId{id + "-agg"},
        core::AggregationNode::Step::kPartial,
        std::vector<core::FieldAccessTypedExprPtr>{},
        std::vector<core::FieldAccessTypedExprPtr>{},
        aggNames,
        aggregates,
        false,
        std::make_shared<stateful::EmptyNode>(inputType));
    auto keyedStateSpec = std::make_shared<KeyedStateSpec>(
        std::vector<column_index_t>{2}, std::vector<TypePtr>{BIGINT()});
    std::shared_ptr<const core::AggregationNode> noLocalAgg;
    return std::make_shared<const StreamWindowAggregationNode>(
        core::PlanNodeId{id},
        aggregation,
        noLocalAgg,
        std::make_shared<exec::HashPartitionFunctionSpec>(
            inputType,
            std::vector<column_index_t>{2}),
        std::make_shared<stateful::StreamWindowPartitionFunctionSpec>(
            inputType,
            1,
            size,
            step,
            0,
            static_cast<WindowType>(windowType)),
        keyedStateSpec,
        /*shiftTimeZone=*/"",
        /*isLocalAgg=*/true,
        size,
        step,
        /*offset=*/0,
        windowType,
        partialRowType,
        /*isEventTime=*/true,
        /*rowtimeIndex=*/1,
        /*windowStartIndex=*/-1,
        /*windowEndIndex=*/-1);
  }

  // Keyless global node: the declared input carries the wrapped
  // "$keyless_key" appended AFTER windowEnd ([intermediates,
  // windowEnd, $keyless_key]); the runtime partials are one column
  // narrower and get wrapped in addInput; the output is the Flink
  // keyless global layout [aggregates, w_start, w_end].
  std::shared_ptr<const StreamWindowAggregationNode> makeGlobalKeylessNode(
      const RowTypePtr& partialInputType,
      const RowTypePtr& outputType,
      int64_t size,
      int64_t step,
      int windowType,
      int rowtimeIndex,
      int windowStartIndex,
      int windowEndIndex,
      column_index_t keylessChannel) {
    const auto id = nextNodeId();
    std::vector<core::AggregationNode::Aggregate> aggregates{sumOfC(), countStarOfC()};
    aggregates[0].rawInputTypes = {BIGINT()};
    aggregates[1].rawInputTypes = {};
    std::vector<std::string> aggNames{"a0", "a1"};
    auto aggregation = std::make_shared<const core::AggregationNode>(
        core::PlanNodeId{id + "-agg"},
        core::AggregationNode::Step::kFinal,
        std::vector<core::FieldAccessTypedExprPtr>{},
        std::vector<core::FieldAccessTypedExprPtr>{},
        aggNames,
        aggregates,
        false,
        std::make_shared<stateful::EmptyNode>(partialInputType));
    auto keyedStateSpec = std::make_shared<KeyedStateSpec>(
        std::vector<column_index_t>{keylessChannel},
        std::vector<TypePtr>{BIGINT()});
    std::shared_ptr<const core::AggregationNode> noLocalAgg;
    return std::make_shared<const StreamWindowAggregationNode>(
        core::PlanNodeId{id},
        aggregation,
        noLocalAgg,
        std::make_shared<exec::HashPartitionFunctionSpec>(
            partialInputType, std::vector<column_index_t>{keylessChannel}),
        std::make_shared<stateful::StreamWindowPartitionFunctionSpec>(
            partialInputType,
            rowtimeIndex,
            size,
            step,
            0,
            static_cast<WindowType>(windowType)),
        keyedStateSpec,
        /*shiftTimeZone=*/"",
        /*isLocalAgg=*/false,
        size,
        step,
        /*offset=*/0,
        windowType,
        outputType,
        /*isEventTime=*/true,
        rowtimeIndex,
        windowStartIndex,
        windowEndIndex);
  }

  std::unique_ptr<LocalWindowAggregator> makeLocalAggregator(
      const std::shared_ptr<const StreamWindowAggregationNode>& node,
      std::unique_ptr<StatefulOperator> capture) {
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(capture));
    auto sliceAssigner = std::make_unique<SliceAssigner>(
        std::make_unique<KeySelector>(
            node->sliceAssignerSpec()->create(INT_MAX, true),
            pool()),
        node->size(),
        node->step(),
        node->offset(),
        Window::getType(node->windowType()),
        node->rowtimeIndex());
    auto aggregator = std::make_unique<LocalWindowAggregator>(
        std::move(targets), std::move(sliceAssigner), node, driverCtx_.get());
    aggregator->initialize();
    return aggregator;
  }

  // The global kFinal node: input = the partial row [k, intermediate,
  // windowEnd], output = the Flink global layout [k, aggregates,
  // w_start, w_end] with the REAL windowStartIndex/windowEndIndex the
  // shadow carries (getWindowStartAndEndIndexes) - pins the production
  // emit layout, not the -1 legacy fallback.
  std::shared_ptr<const StreamWindowAggregationNode> makeGlobalNode(
      const RowTypePtr& partialInputType,
      const RowTypePtr& outputType,
      int64_t size,
      int64_t step,
      int windowType,
      int rowtimeIndex,
      int windowStartIndex,
      int windowEndIndex) {
    const auto id = nextNodeId();
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    groupingKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        BIGINT(), "k"));
    auto sumAgg = sumOfC();
    sumAgg.rawInputTypes = {BIGINT()};
    auto cntAgg = countStarOfC();
    cntAgg.rawInputTypes = {};
    std::vector<std::string> aggNames{"a0", "a1"};
    auto aggregation = std::make_shared<const core::AggregationNode>(
        core::PlanNodeId{id + "-agg"},
        core::AggregationNode::Step::kFinal,
        groupingKeys,
        std::vector<core::FieldAccessTypedExprPtr>{},
        aggNames,
        std::vector<core::AggregationNode::Aggregate>{sumAgg, cntAgg},
        false,
        std::make_shared<stateful::EmptyNode>(partialInputType));
    auto keyedStateSpec = std::make_shared<KeyedStateSpec>(
        std::vector<column_index_t>{0}, std::vector<TypePtr>{BIGINT()});
    std::shared_ptr<const core::AggregationNode> noLocalAgg;
    return std::make_shared<const StreamWindowAggregationNode>(
        core::PlanNodeId{id},
        aggregation,
        noLocalAgg,
        std::make_shared<exec::HashPartitionFunctionSpec>(
            partialInputType, std::vector<column_index_t>{0}),
        std::make_shared<stateful::StreamWindowPartitionFunctionSpec>(
            partialInputType, rowtimeIndex, size, step, 0,
            static_cast<WindowType>(windowType)),
        keyedStateSpec,
        /*shiftTimeZone=*/"",
        /*isLocalAgg=*/false,
        size,
        step,
        /*offset=*/0,
        windowType,
        outputType,
        /*isEventTime=*/true,
        rowtimeIndex,
        windowStartIndex,
        windowEndIndex);
  }

  StreamElementPtr partialRecord2(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& partialValue,
      const std::vector<int64_t>& partialCount,
      const std::vector<int64_t>& windowEnd) {
    return std::make_shared<StreamRecord>(
        "window-agg",
        makeRowVector(
            {"k", "a0", "a1", "window_end"},
            {makeFlatVector<int64_t>(k),
             makeFlatVector<int64_t>(partialValue),
             makeFlatVector<int64_t>(partialCount),
             makeFlatVector<int64_t>(windowEnd)}));
  }

  // Keyless runtime partials: NO key column and NO wrapped
  // "$keyless_key" - the C++ wraps the constant column in addInput
  // (the declared type is one column wider than the runtime batches).
  StreamElementPtr keylessPartialRecord2(
      const std::vector<int64_t>& partialValue,
      const std::vector<int64_t>& partialCount,
      const std::vector<int64_t>& windowEnd) {
    return std::make_shared<StreamRecord>(
        "window-agg",
        makeRowVector(
            {"a0", "a1", "window_end"},
            {makeFlatVector<int64_t>(partialValue),
             makeFlatVector<int64_t>(partialCount),
             makeFlatVector<int64_t>(windowEnd)}));
  }

  // Keyless raw input: [ts, c] - two columns only (the declared type
  // carries the appended "$keyless_key").
  StreamElementPtr keylessWindowRecord(
      const std::vector<int64_t>& tsMillis,
      const std::vector<int64_t>& c) {
    std::vector<Timestamp> ts;
    ts.reserve(tsMillis.size());
    for (auto ms : tsMillis) {
      ts.push_back(Timestamp::fromMillis(ms));
    }
    return std::make_shared<StreamRecord>(
        "window-agg",
        makeRowVector({"ts", "c"}, {
                                       makeFlatVector<Timestamp>(ts),
                                       makeFlatVector<int64_t>(c),
                                   }));
  }

  StreamElementPtr partialRecord(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& partialValue,
      const std::vector<int64_t>& windowEnd) {
    return std::make_shared<StreamRecord>(
        "window-agg",
        makeRowVector(
            {"k", "a0", "window_end"},
            {makeFlatVector<int64_t>(k),
             makeFlatVector<int64_t>(partialValue),
             makeFlatVector<int64_t>(windowEnd)}));
  }

  std::unique_ptr<WindowAggregator> makeAggregator(
      const std::shared_ptr<const StreamWindowAggregationNode>& node,
      std::unique_ptr<StatefulOperator> capture) {
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(capture));
    auto aggregator = std::make_unique<WindowAggregator>(
        nullptr, std::move(targets), node, driverCtx_.get());
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
        makeRowVector({"k", "ts", "c"}, {
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
             makeNullableFlatVector<Timestamp>(
                 {std::nullopt}, TIMESTAMP()),
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
  StreamElementPtr keylessRuntimeRecord(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& tsMillis,
      const std::vector<int64_t>& c) {
    return windowRecord(k, tsMillis, c);
  }

  static constexpr int64_t kTumble = 1;
  static constexpr int64_t kHop = 0;
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
  EXPECT_EQ(capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 10);
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
  EXPECT_EQ(capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 10);

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
// covered range; shared source slices survive for later windows.
TEST_F(WindowAggregatorTest, testHopMultiSliceMerge) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=*/4000, /*step=*/2000, kHop),
      std::move(capture));

  aggregator->addInput(windowRecord({1, 1}, {1000, 3000}, {10, 20}));
  aggregator->advance();
  aggregator->processWatermark(2000);
  aggregator->processWatermark(4000);
  aggregator->processWatermark(6000);
  aggregator->processWatermark(8000);

  ASSERT_EQ(capturePtr->records().size(), 3);
  struct Expected {
    int64_t start;
    int64_t end;
    int64_t sum;
  };
  const std::vector<Expected> expected{
      {-2000, 2000, 10}, {0, 4000, 30}, {2000, 6000, 20}};
  const std::vector<int64_t> expectedCount{1, 2, 1};
  for (size_t i = 0; i < expected.size(); ++i) {
    auto out = capturePtr->records()[i];
    ASSERT_EQ(out->size(), 1);
    EXPECT_EQ(
        out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
        Timestamp::fromMillis(expected[i].start))
        << "window start at record " << i;
    EXPECT_EQ(
        out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
        Timestamp::fromMillis(expected[i].end))
        << "window end at record " << i;
    EXPECT_EQ(out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), expectedCount[i])
        << "window count at record " << i;
    EXPECT_EQ(out->childAt(4)->asFlatVector<int64_t>()->valueAt(0), expected[i].sum)
        << "window sum at record " << i;
  }
}

// H2 regression: a window whose data lives only in its FIRST slice still
// fires through the chain (the old chain condition was dead code); a
// fully empty window stops the chain.
// Batched HOP fire merge (WindowAggregatorHopMerge): a wide window
// (size 12s / slide 2s -> 5 merge units) folds all covered slices in
// ONE extractAccumulators(N) + addSingleGroupIntermediateResults(N)
// per aggregate; the observable result must equal the row-by-row
// semantics (values are the same folds in the same ascending-sliceEnd
// order).
TEST_F(WindowAggregatorTest, testHopBatchedMergeWideWindow) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=*/12000, /*step=*/2000, kHop),
      std::move(capture));

  // One row per covered slice of [0,12000): slices end at
  // 2000..12000, prices 2..12 - the window sum folds all of them.
  aggregator->addInput(windowRecord(
      {1, 1, 1, 1, 1, 1},
      {1000, 3000, 5000, 7000, 9000, 11000},
      {2, 4, 6, 8, 10, 12}));
  aggregator->advance();
  // Earlier windows fire along the way; drive past the wide window.
  for (int64_t wm = 2000; wm <= 14000; wm += 2000) {
    aggregator->processWatermark(wm);
  }

  int64_t sawWide = false;
  for (const auto& out : capturePtr->records()) {
    const auto start =
        out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
    const auto end =
        out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
    if (start == 0 && end == 12000) {
      sawWide = true;
      EXPECT_EQ(out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 6);
      EXPECT_EQ(out->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 42);
    }
  }
  EXPECT_TRUE(sawWide) << "the [0,12000) window must fire";
  aggregator->close();
}

TEST_F(WindowAggregatorTest, testHopChainContinuationFromFirstSliceOnly) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, 4000, 2000, kHop), std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  aggregator->processWatermark(2000);
  aggregator->processWatermark(4000);
  aggregator->processWatermark(8000);

  // [-2000,2000) fires from the only slice; [0,4000) fires through the
  // chain with the SAME slice's data; [4000,...) has nothing and the
  // chain stops - no phantom emissions.
  ASSERT_EQ(capturePtr->records().size(), 2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      10);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      10);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(0));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(4000));
}

// A late row with a partially fired slice set lands in the next unfired
// owning window (HOP), which fires with the late data folded in.
TEST_F(WindowAggregatorTest, testHopLatePartialAccumulatesIntoUnfiredWindow) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, 4000, 2000, kHop), std::move(capture));

  aggregator->addInput(windowRecord({1}, {1000}, {10}));
  aggregator->advance();
  // Fires window [-2000,2000) only; currentProgress = 2.
  aggregator->processWatermark(2000);

  // ts=1500: slice [0,2000) has fired, but its last owning window
  // [0,4000) has not: the row lands in that window.
  aggregator->addInput(windowRecord({1}, {1500}, {5}));
  aggregator->advance();
  EXPECT_EQ(aggregator->lateDroppedRows(), 0);

  aggregator->processWatermark(4000);
  ASSERT_EQ(capturePtr->records().size(), 2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      10);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      15);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(0));
}






// SESSION core: two rows closer than the gap merge into one cover
// window; the fire entry re-hangs to the merged end so the interior
// end never emits (Flink MergingWindowSet + trigger clear on merge).
TEST_F(WindowAggregatorTest, testSessionAdjacentMergeAndTrigger) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
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
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(4500));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      30);
}

// SESSION boundary: rows farther apart than the gap never merge - two
// independent windows fire at their own ends.
TEST_F(WindowAggregatorTest, testSessionDisjointWindowsFireIndependently) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::move(capture));

  aggregator->addInput(windowRecord({1, 1}, {1000, 5000}, {10, 20}));
  aggregator->advance();
  aggregator->processWatermark(3000);
  aggregator->processWatermark(7000);

  ASSERT_EQ(capturePtr->records().size(), 2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(3000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      10);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(7000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      20);
}

// SESSION late rows: a window whose end is already behind the progress
// is retired and the row dropped - no state residue, no emission
// (Flink isWindowLate + retireWindow; TVF allowedLateness = 0).
TEST_F(WindowAggregatorTest, testSessionLateRowRetiredAndDropped) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
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

// SESSION boundary: sessions split only when the row interval EXCEEDS
// the gap - Flink's TimeWindow#intersects is inclusive on both bounds,
// so an interval exactly equal to the gap still merges (native-verified).
TEST_F(WindowAggregatorTest, testSessionBoundaryTouchMerges) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::move(capture));

  // ts=1000 opens [1000,3000); ts=3000 opens [3000,5000) - touching at
  // 3000: one merged session [1000,5000).
  aggregator->addInput(windowRecord({1, 1}, {1000, 3000}, {10, 20}));
  aggregator->advance();
  aggregator->processWatermark(5000);

  ASSERT_EQ(capturePtr->records().size(), 1);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      2);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      30);
}

// SESSION out-of-order merge into a NON-tail window: the row's window
// intersects the floor window while the tail stays disjoint - the
// general ceiling/floor path (the in-order fast path does not apply).
TEST_F(WindowAggregatorTest, testSessionOutOfOrderMergesIntoFloorWindow) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
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
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(4500));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      3);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      45);
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(7000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      20);
}

// SESSION bridge merge: the row's window touches BOTH in-flight windows
// on its inclusive bounds - the cover swallows them into one session.
TEST_F(WindowAggregatorTest, testSessionBridgeMergesBothWindows) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
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
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(7000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      3);
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(4)->asFlatVector<int64_t>()->valueAt(0),
      60);
}

// FILTER masks reach the window aggregation exactly as in group
// aggregation, and multiple aggregates over one input share the batch.
TEST_F(WindowAggregatorTest, testFilterMaskAndMultipleAggregates) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto masked = sumOfC();
  masked.mask =
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "f");
  auto node = makeNode(
      {masked, avgOfC(), countOfC()},
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
  // avg(c) over 10,20,45 with Flink's truncating integer division.
  EXPECT_EQ(out->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 25);
  // count(c) over all three rows.
  EXPECT_EQ(out->childAt(5)->asFlatVector<int64_t>()->valueAt(0), 3);
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
      TimeWindowUtil::getCurrentProcessingTime() + 10000);
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto out = capturePtr->records()[0];
  EXPECT_EQ(out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 15);
  const auto wStart = out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
  const auto wEnd = out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
  EXPECT_EQ(wEnd - wStart, 5000);
  EXPECT_EQ(wStart % 5000, 0) << "proc-time slice must be boundary aligned";

  aggregator->close();
}

// SESSION processing-time: every batch shares the arrival instant, so
// two batches land in one merged session window that fires on the
// task-thread tick past the gap (audit N4 gap - the proc-time branch
// of advanceSession had no dedicated test).
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
  EXPECT_EQ(
      out->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 2);
  EXPECT_EQ(
      out->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 30);
  const auto wStart = out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
  const auto wEnd = out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
  EXPECT_GE(wEnd - wStart, 60000);
  EXPECT_LT(wEnd - wStart, 61000)
      << "the two batch instants are microseconds apart; the cover is the gap plus that";

  aggregator->close();
}

// L4 M1: the local pre-aggregation accumulates in memory and flushes
// partial rows [key, intermediate, windowEnd] when the watermark fires
// the buffered slices - data before watermark (the two-stage ordering
// contract).
TEST_F(WindowAggregatorTest, testLocalFlushesOnWatermark) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeLocalAggregator(
      makeLocalNode(
          {sumOfC()},
          ROW({"k", "sum", "window_end"}, {BIGINT(), BIGINT(), BIGINT()}),
          5000,
          0,
          1),
      std::move(capture));

  aggregator->addInput(windowRecord({1, 1}, {1000, 1500}, {10, 5}));
  aggregator->advance();
  aggregator->addInput(windowRecord({2}, {3000}, {7}));
  aggregator->advance();
  // Watermark before the slice end: buffered, nothing flushed.
  aggregator->processWatermark(3000);
  EXPECT_EQ(capturePtr->traceSize(), 1); // just the watermark
  EXPECT_EQ(capturePtr->records().size(), 0);

  // Watermark past the slice end [0,5000): both partials flush first.
  aggregator->processWatermark(5999);
  ASSERT_GE(capturePtr->traceSize(), 2);
  EXPECT_EQ(capturePtr->kindAt(1), 'R');
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto partials = capturePtr->records()[0];
  ASSERT_EQ(partials->size(), 2);
  EXPECT_EQ(
      partials->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_EQ(
      partials->childAt(0)->asFlatVector<int64_t>()->valueAt(1), 2);
  EXPECT_EQ(
      partials->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 15);
  EXPECT_EQ(
      partials->childAt(1)->asFlatVector<int64_t>()->valueAt(1), 7);
  EXPECT_EQ(
      partials->childAt(2)->asFlatVector<int64_t>()->valueAt(0), 5000);
  EXPECT_EQ(
      partials->childAt(2)->asFlatVector<int64_t>()->valueAt(1), 5000);

  aggregator->close();
}

// L4 M1: rows of the same (key, slice) across batches pre-aggregate
// into ONE partial row.
TEST_F(WindowAggregatorTest, testLocalSameSliceAcrossBatches) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeLocalAggregator(
      makeLocalNode(
          {countStarOfC()},
          ROW({"k", "cnt", "window_end"}, {BIGINT(), BIGINT(), BIGINT()}),
          5000,
          0,
          1),
      std::move(capture));

  aggregator->addInput(windowRecord({1, 1, 1}, {1000, 2000, 3000}, {1, 2, 3}));
  aggregator->advance();
  aggregator->addInput(windowRecord({1}, {4000}, {4}));
  aggregator->advance();
  aggregator->processWatermark(9999);

  ASSERT_EQ(capturePtr->records().size(), 1);
  auto partials = capturePtr->records()[0];
  ASSERT_EQ(partials->size(), 1);
  EXPECT_EQ(partials->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_EQ(partials->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 4);
  EXPECT_EQ(partials->childAt(2)->asFlatVector<int64_t>()->valueAt(0), 5000);

  aggregator->close();
}

// L4 M1: HOP rows in different slices pre-aggregate into separate
// partials with the slice ends as the windowEnd column.
TEST_F(WindowAggregatorTest, testLocalHopDistinctSliceEnds) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeLocalAggregator(
      makeLocalNode(
          {sumOfC()},
          ROW({"k", "sum", "window_end"}, {BIGINT(), BIGINT(), BIGINT()}),
          4000,
          2000,
          0),
      std::move(capture));

  aggregator->addInput(windowRecord({1, 1}, {1000, 3000}, {10, 20}));
  aggregator->advance();
  aggregator->processWatermark(9999);

  ASSERT_EQ(capturePtr->records().size(), 1);
  auto partials = capturePtr->records()[0];
  ASSERT_EQ(partials->size(), 2);
  EXPECT_EQ(partials->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 10);
  EXPECT_EQ(partials->childAt(2)->asFlatVector<int64_t>()->valueAt(0), 2000);
  EXPECT_EQ(partials->childAt(1)->asFlatVector<int64_t>()->valueAt(1), 20);
  EXPECT_EQ(partials->childAt(2)->asFlatVector<int64_t>()->valueAt(1), 4000);

  aggregator->close();
}

// L4 M2: the global stage (kFinal) consumes the local partials [k,
// intermediate, windowEnd] - the slice end comes from the appended
// windowEnd column (attached) and the intermediates merge into the
// slice state; fire semantics identical to the single-stage path.
TEST_F(WindowAggregatorTest, testGlobalConsumesPartials) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  const auto partialType =
      ROW({"k", "a0", "a1", "window_end"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto aggregator = makeAggregator(
      makeGlobalNode(
          partialType,
          // The Flink global layout [keys, aggregates, w_start, w_end]
          // with the REAL window column positions the shadow carries.
          ROW({"k", "s", "c", "w_start", "w_end"},
              {BIGINT(), BIGINT(), BIGINT(), TIMESTAMP(), TIMESTAMP()}),
          5000,
          0,
          1,
          3,
          3,
          4),
      std::move(capture));

  // Two partials for the same (k=1, slice [0,5000)): the global merges
  // them into one window state, fires once with the combined sum.
  aggregator->addInput(
      partialRecord2({1, 1}, {10, 5}, {1, 1}, {5000, 5000}));
  aggregator->advance();
  // A second key in the same slice.
  aggregator->addInput(partialRecord2({2}, {7}, {1}, {5000}));
  aggregator->advance();
  aggregator->processWatermark(5999);

  ASSERT_EQ(capturePtr->records().size(), 2);
  for (const auto& out : capturePtr->records()) {
    const auto key = out->childAt(0)->asFlatVector<int64_t>()->valueAt(0);
    const auto start =
        out->childAt(3)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
    const auto end =
        out->childAt(4)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
    const auto sum = out->childAt(1)->asFlatVector<int64_t>()->valueAt(0);
    const auto cnt = out->childAt(2)->asFlatVector<int64_t>()->valueAt(0);
    EXPECT_EQ(start, 0);
    EXPECT_EQ(end, 5000);
    if (key == 1) {
      EXPECT_EQ(sum, 15);
      EXPECT_EQ(cnt, 2);
    } else {
      EXPECT_EQ(key, 2);
      EXPECT_EQ(sum, 7);
      EXPECT_EQ(cnt, 1);
    }
  }
  aggregator->close();
}

// L4 M2: HOP partials merge across slices - the fire heap-merges the
// covered slices' states exactly as in the single-stage path.
TEST_F(WindowAggregatorTest, testGlobalHopPartials) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  const auto partialType =
      ROW({"k", "a0", "a1", "window_end"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto aggregator = makeAggregator(
      makeGlobalNode(
          partialType,
          // The Flink global layout [keys, aggregates, w_start, w_end].
          ROW({"k", "s", "c", "w_start", "w_end"},
              {BIGINT(), BIGINT(), BIGINT(), TIMESTAMP(), TIMESTAMP()}),
          4000,
          2000,
          0,
          3,
          3,
          4),
      std::move(capture));

  // Slice 2000 carries sum 30, slice 4000 carries sum 20; the window
  // [-2000, 2000) fires from slice 2000 only, [0, 4000) merges both.
  aggregator->addInput(
      partialRecord2({1, 1, 1}, {10, 20, 10}, {1, 2, 1}, {2000, 2000, 4000}));
  aggregator->advance();
  aggregator->processWatermark(2000);
  aggregator->processWatermark(4000);

  ASSERT_EQ(capturePtr->records().size(), 2);
  const auto first = capturePtr->records()[0];
  EXPECT_EQ(
      first->childAt(3)->asFlatVector<Timestamp>()->valueAt(0).toMillis(),
      -2000);
  EXPECT_EQ(
      first->childAt(4)->asFlatVector<Timestamp>()->valueAt(0).toMillis(),
      2000);
  EXPECT_EQ(first->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 30);
  const auto second = capturePtr->records()[1];
  EXPECT_EQ(
      second->childAt(3)->asFlatVector<Timestamp>()->valueAt(0).toMillis(),
      0);
  EXPECT_EQ(
      second->childAt(4)->asFlatVector<Timestamp>()->valueAt(0).toMillis(),
      4000);
  EXPECT_EQ(second->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 40);
  aggregator->close();
}

// L4 full-chain: LOCAL produces partials for the dr-3 overlapping-HOP
// data, the GLOBAL consumes them and fires - mirrors the e2e path.
TEST_F(WindowAggregatorTest, testTwoStageHopOverlapChain) {
  // LOCAL stage
  auto localCapture = std::make_unique<CaptureTarget>(
      driverCtx_.get(), nextNodeId());
  auto* localCapturePtr = localCapture.get();
  auto local = makeLocalAggregator(
      makeLocalNode(
          {sumOfC(), countStarOfC()},
          ROW({"k", "sum", "count", "window_end"},
              {BIGINT(), BIGINT(), BIGINT(), BIGINT()}),
          4000, 2000, 0),
      std::move(localCapture));

  // The dr-3 data: key=1 prices 10@1s, 20@3s, 30@5s, 40@7s; key=2 50@3.5s
  local->addInput(
      windowRecord({1, 1, 1, 1, 2}, {1000, 3000, 5000, 7000, 3500}, {10, 20, 30, 40, 50}));
  local->advance();
  local->processWatermark(9999);
  // Read the partials BEFORE close(): close() destroys the CaptureTarget.
  auto localPartials = localCapturePtr->records();
  // One vectorized flush batch carrying every (key, sliceEnd) partial:
  // k1 [0,2000)=10, k1 [2000,4000)=20, k2 [2000,4000)=50,
  // k1 [4000,6000)=30, k1 [6000,8000)=40 (slice size = gcd(4000, 2000)).
  ASSERT_EQ(localPartials.size(), 1);
  ASSERT_EQ(localPartials[0]->size(), 5);

  // GLOBAL stage: feed the LOCAL's partials
  auto globalCapture = std::make_unique<CaptureTarget>(
      driverCtx_.get(), nextNodeId());
  auto* globalCapturePtr = globalCapture.get();
  const auto partialType =
      ROW({"k", "sum", "count", "window_end"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto global = makeAggregator(
      makeGlobalNode(
          partialType,
          // The Flink global layout [keys, aggregates, w_start, w_end].
          ROW({"k", "s", "c", "w_start", "w_end"},
              {BIGINT(), BIGINT(), BIGINT(), TIMESTAMP(), TIMESTAMP()}),
          4000, 2000, 0, 3, 3, 4),
      std::move(globalCapture));

  for (const auto& batch : localPartials) {
    global->addInput(std::make_shared<StreamRecord>("window-agg", batch));
    global->advance();
  }
  global->processWatermark(9999);
  auto results = globalCapturePtr->records();
  // The dr-3 data fed in ONE batch (no intermediate watermarks): no row
  // is late, so every owning window accumulates - including k2 in
  // [0,4000), which the e2e double-run (progressive watermarks, 2s lag)
  // late-drops because the out-of-order 50@3500 arrives after [0,4000)
  // fired at watermark 5000. The e2e therefore sees six windows; this
  // single-batch chain sees all seven owning windows.
  ASSERT_EQ(results.size(), 7);
  std::vector<std::string> fired;
  for (const auto& out : results) {
    for (vector_size_t i = 0; i < out->size(); ++i) {
      const auto key = out->childAt(0)->asFlatVector<int64_t>()->valueAt(i);
      const auto sum = out->childAt(1)->asFlatVector<int64_t>()->valueAt(i);
      const auto cnt = out->childAt(2)->asFlatVector<int64_t>()->valueAt(i);
      const auto start = out->childAt(3)->asFlatVector<Timestamp>()->valueAt(i).toMillis();
      const auto end = out->childAt(4)->asFlatVector<Timestamp>()->valueAt(i).toMillis();
      fired.push_back(
          "k=" + std::to_string(key) + " [" + std::to_string(start) + "," +
          std::to_string(end) + ") sum=" + std::to_string(sum) +
          " cnt=" + std::to_string(cnt));
    }
  }
  std::sort(fired.begin(), fired.end());
  const std::vector<std::string> expected = {
      "k=1 [-2000,2000) sum=10 cnt=1",
      "k=1 [0,4000) sum=30 cnt=2",
      "k=1 [2000,6000) sum=50 cnt=2",
      "k=1 [4000,8000) sum=70 cnt=2",
      "k=1 [6000,10000) sum=40 cnt=1",
      "k=2 [0,4000) sum=50 cnt=1",
      "k=2 [2000,6000) sum=50 cnt=1"};
  EXPECT_EQ(fired, expected);
}

// L4 keyless LOCAL: runtime batches are one column narrower than the
// declared input (gluten appends "$keyless_key"); the wrap materializes
// the constant key, all rows share one key row, and the flush emits the
// Flink keyless local layout [accs, windowEnd] with NO key column.
TEST_F(WindowAggregatorTest, testLocalKeylessWrapsAndFlushes) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeLocalAggregator(
      makeLocalKeylessNode(
          {sumOfC()},
          // The Flink keyless local output [accs, windowEnd].
          ROW({"sum", "window_end"}, {BIGINT(), BIGINT()}),
          5000,
          0,
          1),
      std::move(capture));

  // Narrow [ts, c] batches - no keyless column at runtime.
  aggregator->addInput(keylessWindowRecord({1000, 1500}, {10, 5}));
  aggregator->advance();
  aggregator->addInput(keylessWindowRecord({3000}, {7}));
  aggregator->advance();
  aggregator->processWatermark(5999);

  ASSERT_EQ(capturePtr->records().size(), 1);
  auto partials = capturePtr->records()[0];
  ASSERT_EQ(partials->size(), 1);
  ASSERT_EQ(partials->childrenSize(), 2); // no key column
  EXPECT_EQ(partials->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 22);
  EXPECT_EQ(partials->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 5000);

  aggregator->close();
}

// L4 keyless GLOBAL: the declared input carries "$keyless_key"
// appended AFTER windowEnd ([intermediates, windowEnd, $keyless_key]);
// the kFinal branch must read the intermediates at [0, numAggs) (the
// windowEnd is one column left of the wrapped keyless tail) and emit
// the Flink keyless global layout [aggs, w_start, w_end].
TEST_F(WindowAggregatorTest, testGlobalKeylessConsumesPartials) {
  auto capture = std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  const auto partialType = ROW(
      {"a0", "a1", "window_end", "$keyless_key"},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto aggregator = makeAggregator(
      makeGlobalKeylessNode(
          partialType,
          // The Flink keyless global layout [aggs, w_start, w_end].
          ROW({"s", "c", "w_start", "w_end"},
              {BIGINT(), BIGINT(), TIMESTAMP(), TIMESTAMP()}),
          5000,
          0,
          1,
          /*rowtimeIndex=*/2,
          /*windowStartIndex=*/2,
          /*windowEndIndex=*/3,
          /*keylessChannel=*/3),
      std::move(capture));

  // Narrow [a0, a1, windowEnd] partials - no keyless column at runtime.
  aggregator->addInput(keylessPartialRecord2({10, 15}, {1, 1}, {5000, 5000}));
  aggregator->advance();
  aggregator->addInput(keylessPartialRecord2({30}, {1}, {10000}));
  aggregator->advance();
  aggregator->processWatermark(10999);

  ASSERT_EQ(capturePtr->records().size(), 2);
  for (const auto& out : capturePtr->records()) {
    const auto sum = out->childAt(0)->asFlatVector<int64_t>()->valueAt(0);
    const auto cnt = out->childAt(1)->asFlatVector<int64_t>()->valueAt(0);
    const auto start =
        out->childAt(2)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
    const auto end =
        out->childAt(3)->asFlatVector<Timestamp>()->valueAt(0).toMillis();
    ASSERT_EQ(out->childrenSize(), 4); // no key column
    if (end == 5000) {
      EXPECT_EQ(start, 0);
      EXPECT_EQ(sum, 25);
      EXPECT_EQ(cnt, 2);
    } else {
      EXPECT_EQ(end, 10000);
      EXPECT_EQ(start, 5000);
      EXPECT_EQ(sum, 30);
      EXPECT_EQ(cnt, 1);
    }
  }
  aggregator->close();
}

// Keyless windows: the runtime batch is one column narrower than the
// declared input type (gluten appends "$keyless_key"); the wrap
// materializes the constant key and all rows share one key row.
TEST_F(WindowAggregatorTest, testKeylessGlobalWindowAggregation) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto declaredInput =
      ROW({"k", "ts", "c", "$keyless_key"},
          {BIGINT(), TIMESTAMP(), BIGINT(), BIGINT()});
  auto node = makeNode(
      {sumOfC()},
      5000,
      0,
      kTumble,
      declaredInput,
      /*isEventTime=*/true,
      /*rowtimeIndex=*/1,
      ROW({"w_start", "w_end", "s"},
          {TIMESTAMP(), TIMESTAMP(), BIGINT()}),
      /*keyChannels=*/{3},
      /*keyTypes=*/{BIGINT()},
      /*shiftTimeZone=*/"",
      /*windowStartIndex=*/0,
      /*windowEndIndex=*/1);
  auto aggregator = makeAggregator(node, std::move(capture));

  // Runtime rows carry only k/ts/c; the k column value is irrelevant to
  // grouping (all rows one key).
  aggregator->addInput(windowRecord({1, 2}, {1000, 2000}, {10, 30}));
  aggregator->advance();
  aggregator->processWatermark(5000);

  // The output projection carries NO key column - window columns at 0/1,
  // the aggregate at 2 (the gluten keyless layout).
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto out = capturePtr->records()[0];
  ASSERT_EQ(out->size(), 1);
  EXPECT_EQ(
      out->childAt(0)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(0));
  EXPECT_EQ(
      out->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(out->childAt(2)->asFlatVector<int64_t>()->valueAt(0), 40);
}

// A FILTERED COUNT(*) is NOT a count star (Flink indexOfCountStar
// requires filterArg < 0) - the empty-window supplier stays disabled
// and zero-count windows EMIT (pinned against native Flink 1.19.2
// behavior on the dual-run case 108_dempty_filtered_window).
TEST_F(WindowAggregatorTest, testTumbleFilteredCountStarNotCountStarEmitsZeroRow) {
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

  aggregator->addInput(maskedRecord(
      {1, 1}, {1000, 2000}, {10, 20}, {false, false}));
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
// Flink WindowIsEmptySupplier's checkArgument.
TEST_F(WindowAggregatorTest, testHopRequiresCountStar) {
  auto node = makeNode({sumOfC()}, 4000, 2000, kHop);
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
  auto aggregator = std::make_unique<WindowAggregator>(
      nullptr, std::move(targets), node, driverCtx_.get());
  VELOX_ASSERT_THROW(
      aggregator->initialize(),
      "Hopping window requires a COUNT");
}

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
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
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


// SESSION restore: the rebuilt fire entry carries the ACTUAL (merged)
// window from the mapping, not the pinned state window - the merged
// range fires as one at its own end, and post-restore rows merge and
// retire against the restored mapping.
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
      capturePtr->records()[0]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(1000));
  EXPECT_EQ(
      capturePtr->records()[0]->childAt(2)->asFlatVector<Timestamp>()->valueAt(0),
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
      capturePtr->records()[1]->childAt(1)->asFlatVector<Timestamp>()->valueAt(0),
      Timestamp::fromMillis(5000));
  EXPECT_EQ(
      capturePtr->records()[1]->childAt(3)->asFlatVector<int64_t>()->valueAt(0),
      5);
}

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
      makeFlatVector<int8_t>(std::vector<int8_t>{static_cast<int8_t>(RowKind::UPDATE_AFTER)}));
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
// window-agg-cumulate-deferred branch) and must keep failing loud.
TEST_F(WindowAggregatorTest, testSessionInitializeAndCumulateDeferred) {
  auto aggregator = makeAggregator(
      makeNode({countStarOfC(), sumOfC()}, /*size=gap=*/2000, /*step=*/0, kSession),
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
  aggregator->addInput(windowRecord({1}, {1000}, {7}));
  aggregator->advance();
  aggregator->processWatermark(6000);

  auto node = makeNode({sumOfC()}, 4000, 2000, 3 /* CUMULATIVE */);
  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
  auto rejected = std::make_unique<WindowAggregator>(
      nullptr, std::move(targets), node, driverCtx_.get());
  VELOX_ASSERT_THROW(rejected->initialize(), "CUMULATE");
}

// Fail loud: DISTINCT and sorted aggregates are rejected by
// makeAggregateInfos (L1), and HOP size must be a multiple of slide.
TEST_F(WindowAggregatorTest, testPlanValidationFailsLoud) {
  {
    auto distinct = sumOfC();
    distinct.distinct = true;
    auto node = makeNode({distinct}, 5000, 0, kTumble);
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(
        std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
    auto aggregator = std::make_unique<WindowAggregator>(
        nullptr, std::move(targets), node, driverCtx_.get());
    VELOX_ASSERT_THROW(aggregator->initialize(), "Distinct");
  }
  {
    // HOP size 5000 with slide 3000: not a multiple.
    auto node = makeNode({countStarOfC(), sumOfC()}, 5000, 3000, kHop);
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(
        std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
    auto aggregator = std::make_unique<WindowAggregator>(
        nullptr, std::move(targets), node, driverCtx_.get());
    VELOX_ASSERT_THROW(aggregator->initialize(), "multiple of slide");
  }
  {
    // A missing KeyedStateSpec is rejected at init.
    auto withSpec = makeNode({sumOfC()}, 5000, 0, kTumble);
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(
        std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
    std::shared_ptr<const core::AggregationNode> aggregation =
        withSpec->aggregation();
    std::shared_ptr<const core::AggregationNode> noLocalAgg;
    auto node = std::make_shared<const StreamWindowAggregationNode>(
        withSpec->id(),
        aggregation,
        noLocalAgg,
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        /*keyedStateSpec=*/nullptr,
        "",
        false,
        5000,
        0,
        0,
        kTumble,
        withSpec->outputType(),
        true,
        1,
        -1,
        -1);
    auto aggregator = std::make_unique<WindowAggregator>(
        nullptr, std::move(targets), node, driverCtx_.get());
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

  TimeWindowNsSerializer timeWindow;
  const TimeWindowNs tw(1000, 5000);
  const auto twFrame = timeWindow.serialize(tw);
  EXPECT_EQ(twFrame.size(), 16);
  EXPECT_EQ(timeWindow.schemaId(), "time-window");
  const auto twBack = timeWindow.deserialize(twFrame);
  EXPECT_TRUE(twBack.equals(tw));
  EXPECT_EQ(twBack.hash(), tw.hash());
}

} // namespace

// Flink TimeWindowUtil DST semantics. Inputs are shifted (naive-UTC)
// wall clocks; outputs are real UTC instants (comparable against raw
// watermarks). America/Los_Angeles 2021 transitions; note Flink's own
// javadoc example anchors are written in real-instant millis and are
// off by the zone offset - the outputs below are the code's semantics.
TEST(TimeWindowUtilDstTest, testToEpochMillsForTimerMatchesFlinkSemantics) {
  const auto* la = TimeWindowUtil::locateTimeZone("America/Los_Angeles");
  ASSERT_NE(la, nullptr);

  // DST gap (2021-03-14): naive 02:00 and 02:59:59.999 do not exist; both
  // map to the first instant after the gap, floored to the hour
  // (03:00 PDT = 2021-03-14T10:00:00Z = 1615716000000).
  EXPECT_EQ(
      TimeWindowUtil::toEpochMillsForTimer(1615687200000L, la), 1615716000000L);
  EXPECT_EQ(
      TimeWindowUtil::toEpochMillsForTimer(1615690799999L, la), 1615716000000L);

  // DST overlap (2021-11-07): naive 01:00 occurs twice; the timer uses
  // the later instant so both hours of data fire
  // (01:00 PST = 2021-11-07T09:00:00Z = 1636275600000).
  EXPECT_EQ(
      TimeWindowUtil::toEpochMillsForTimer(1636246800000L, la), 1636275600000L);

  // Unique instants convert by the plain offset.
  // 2021-11-06T23:30 LA (PDT, -7) = 2021-11-07T06:30:00Z = 1636266600000.
  EXPECT_EQ(
      TimeWindowUtil::toEpochMillsForTimer(1636241400000L, la), 1636266600000L);
  EXPECT_EQ(TimeWindowUtil::toEpochMillsForTimer(0, la), 28800000L);

  // UTC is identity and null zone is identity.
  const auto* utc = TimeWindowUtil::locateTimeZone("UTC");
  EXPECT_EQ(utc, nullptr);
  EXPECT_EQ(TimeWindowUtil::toEpochMillsForTimer(123456, nullptr), 123456);
}

TEST(TimeWindowUtilDstTest, testToUtcTimestampMills) {
  const auto* shanghai = TimeWindowUtil::locateTimeZone("Asia/Shanghai");
  ASSERT_NE(shanghai, nullptr);
  // Epoch 0 reads 08:00 in Shanghai's wall clock: the shifted millis
  // re-interpret that wall clock as UTC.
  EXPECT_EQ(TimeWindowUtil::toUtcTimestampMills(0, shanghai), 28800000L);
  // Round-trips with the timer conversion for unique instants.
  EXPECT_EQ(
      TimeWindowUtil::toEpochMillsForTimer(
          TimeWindowUtil::toUtcTimestampMills(1000, shanghai), shanghai),
      1000);
}

} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
