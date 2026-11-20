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

#include "velox/experimental/stateful/GroupWindowAggregator.h"

#include <atomic>
#include <set>
#include <utility>

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Driver.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StatefulPlanner.h"
#include "velox/experimental/stateful/StatefulTask.h"
#include "velox/experimental/stateful/state/HashMapStateBackend.h"
#include "velox/experimental/stateful/window/MergingWindowSet.h"
#include "velox/experimental/stateful/window/WindowProcessFunction.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful::test {
namespace {

using Agg = core::AggregationNode::Aggregate;

// exec::Operator shell with no data-plane behavior for the collecting
// downstream target.
class NoOutputOperator : public exec::Operator {
 public:
  NoOutputOperator(
      exec::DriverCtx* driverCtx,
      int32_t operatorId,
      const std::string& planNodeId)
      : Operator(
            driverCtx,
            ROW({"c"}, {BIGINT()}),
            operatorId,
            std::move(planNodeId),
            "Collect") {}

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

// Downstream collector: captures every emitted StreamElement (the
// collecting-target shape of the non-keyed typed heap backend chain,
// evidence-backed by the 022 e2e form).
class CollectingTarget : public StatefulOperator {
 public:
  CollectingTarget(
      exec::DriverCtx* driverCtx,
      int32_t operatorId,
      const std::string& planNodeId)
      : StatefulOperator(
            std::make_unique<NoOutputOperator>(
                driverCtx,
                operatorId,
                planNodeId),
            std::vector<StatefulOperatorPtr>()) {}

  void addInput(StreamElementPtr input) override {
    collected_.push_back(input);
  }

  std::vector<StreamElementPtr> collected_;
};

// Builds one aggregate call over the "v" input column, or the no-argument
// COUNT(*).
Agg aggregateCall(
    const std::string& name,
    const TypePtr& inputType,
    const TypePtr& resultType,
    const std::string& column = "v") {
  std::vector<core::TypedExprPtr> inputs;
  std::vector<TypePtr> rawInputTypes;
  if (!column.empty()) {
    inputs.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(inputType, column));
    rawInputTypes.push_back(inputType);
  }
  return {
      std::make_shared<core::CallTypedExpr>(resultType, inputs, name),
      rawInputTypes,
      nullptr,
      {},
      {},
      false};
}

// One emitted window row as seen at the output: key, aggregate values and
// the window start / end (in millis).
struct Emitted {
  std::vector<std::optional<int32_t>> keys;
  std::vector<std::optional<int64_t>> count;
  std::vector<std::optional<int32_t>> sum;
  int64_t windowStart = -1;
  int64_t windowEnd = -1;

  bool sameAs(const Emitted& other) const {
    return keys == other.keys && count == other.count && sum == other.sum &&
        windowStart == other.windowStart && windowEnd == other.windowEnd;
  }
};

// FunctionContext double for the process-function-level units: a fixed
// current key, a controllable watermark, and accumulator reads that route
// to a caller-supplied lambda (nullptr = window without state).
class MockFunctionContext : public FunctionContext<TimeWindow> {
 public:
  MockFunctionContext(
      RowContainerStateKey key,
      int64_t watermark,
      std::function<char*(TimeWindow)> windowAccumulators =
          [](TimeWindow) { return (char*)nullptr; })
      : key_(std::move(key)),
        watermark_(watermark),
        windowAccumulators_(std::move(windowAccumulators)) {}

  const RowContainerStateKey& currentKey() override {
    return key_;
  }

  int64_t currentProcessingTime() override {
    return 0;
  }

  int64_t currentWatermark() override {
    return watermark_;
  }

  int getShiftTimeZone() override {
    return 0;
  }

  char* getWindowAccumulators(TimeWindow window) override {
    return windowAccumulators_(window);
  }

  char* getOrCreateWindowAccumulators(TimeWindow window) override {
    return windowAccumulators_(window);
  }

  void clearWindowState(TimeWindow) override {}
  void clearPreviousState(TimeWindow) override {}
  void clearTrigger(TimeWindow) override {}
  void onMerge(TimeWindow, std::vector<TimeWindow>&) override {}
  void deleteCleanupTimer(TimeWindow) override {}

 private:
  RowContainerStateKey key_;
  int64_t watermark_;
  std::function<char*(TimeWindow)> windowAccumulators_;
};

// Session window assigner with a 10 ms gap, on the event-time flag.
std::shared_ptr<MergingWindowAssigner> sessionAssigner() {
  return std::make_shared<SessionWindowAssigner>(10, true);
}

class GroupWindowAggregatorTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    aggregate::prestosql::registerAllAggregateFunctions();
    // Flink aggregate semantics overwrite the prestosql baseline (bare
    // names), exactly as the velox4j Flink preset initializes them.
    aggregate::flinksql::registerAggregateFunctions();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = StatefulTask::create(
        "GroupWindowAggregatorTest_task",
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

  // Default shape: GROUP BY k, SESSION(ts, 10), COUNT(*) - the q11 form.
  // 'withSum' adds SUM(v) over the INT column: the flinksql sum keeps the
  // input type where the prestosql baseline widens to BIGINT.
  static RowTypePtr intInputType(bool withSum = false) {
    if (withSum) {
      return ROW({"k", "ts", "v"}, {INTEGER(), TIMESTAMP(), INTEGER()});
    }
    return ROW({"k", "ts"}, {INTEGER(), TIMESTAMP()});
  }

  static RowTypePtr intOutputType(bool withSum = false) {
    if (withSum) {
      return ROW(
          {"k", "cnt", "s", "wstart", "wend"},
          {INTEGER(), BIGINT(), INTEGER(), TIMESTAMP(), TIMESTAMP()});
    }
    return ROW(
        {"k", "cnt", "wstart", "wend"},
        {INTEGER(), BIGINT(), TIMESTAMP(), TIMESTAMP()});
  }

  // The q11 production shape: the planner pushes all four window
  // properties of a rowtime window into the aggregate node's declared
  // output (WindowPropertiesRule orders them [start, end, rowtime,
  // proctime]), so the declared width is keys + aggs + 4 property columns.
  static RowTypePtr q11OutputType() {
    return ROW(
        {"k", "cnt", "wstart", "wend", "wrowtime", "wproctime"},
        {INTEGER(),
         BIGINT(),
         TIMESTAMP(),
         TIMESTAMP(),
         TIMESTAMP(),
         TIMESTAMP()});
  }

  static std::vector<Agg> windowAggregates(bool withSum) {
    std::vector<Agg> aggregates{aggregateCall("count", BIGINT(), BIGINT(), "")};
    if (withSum) {
      aggregates.push_back(aggregateCall("sum", INTEGER(), INTEGER()));
    }
    return aggregates;
  }

  std::shared_ptr<const GroupWindowAggregationNode> makeNode(
      bool withSum = false,
      int64_t gap = 10,
      std::vector<Agg> aggregates = {},
      int32_t countStarIndex = 0,
      int windowType = static_cast<int>(WindowType::SESSION),
      int64_t size = 0,
      int64_t slide = 0) {
    if (aggregates.empty()) {
      aggregates = windowAggregates(withSum);
    }
    const auto serial = operatorSerial_++;
    auto handler = std::make_shared<GroupWindowAggsHandlerNode>(
        core::PlanNodeId{"window-handler-" + std::to_string(serial)},
        intOutputType(withSum),
        aggregates,
        countStarIndex);
    auto inputType = intInputType(withSum);
    auto spec = std::make_shared<KeyedStateSpec>(
        std::vector<column_index_t>{0},
        std::vector<TypePtr>{inputType->childAt(0)});
    const int numKeys = 1;
    const int numAggs = static_cast<int>(aggregates.size());
    return std::make_shared<GroupWindowAggregationNode>(
        core::PlanNodeId{"window-agg-" + std::to_string(serial)},
        handler,
        spec,
        inputType,
        /*allowedLateness=*/0,
        /*produceUpdates=*/false,
        /*rowtimeIndex=*/1,
        /*isEventTime=*/true,
        /*windowType=*/windowType,
        gap,
        /*size=*/size,
        /*slide=*/slide,
        /*windowStartIndex=*/numKeys + numAggs,
        /*windowEndIndex=*/numKeys + numAggs + 1,
        intOutputType(withSum));
  }

  // The q11-shaped node: the same default aggregate (COUNT(*), one key),
  // but the six-column declared output the planner actually produces for
  // a query referencing SESSION_START / SESSION_END.
  std::shared_ptr<const GroupWindowAggregationNode> makeQ11Node() {
    const auto serial = operatorSerial_++;
    auto handler = std::make_shared<GroupWindowAggsHandlerNode>(
        core::PlanNodeId{"window-handler-" + std::to_string(serial)},
        q11OutputType(),
        windowAggregates(false),
        /*countStarIndex=*/0);
    auto inputType = intInputType(false);
    auto spec = std::make_shared<KeyedStateSpec>(
        std::vector<column_index_t>{0},
        std::vector<TypePtr>{inputType->childAt(0)});
    return std::make_shared<GroupWindowAggregationNode>(
        core::PlanNodeId{"window-agg-" + std::to_string(serial)},
        handler,
        spec,
        inputType,
        /*allowedLateness=*/0,
        /*produceUpdates=*/false,
        /*rowtimeIndex=*/1,
        /*isEventTime=*/true,
        /*windowType=*/static_cast<int>(WindowType::SESSION),
        /*gap=*/10,
        /*size=*/0,
        /*slide=*/0,
        /*windowStartIndex=*/2,
        /*windowEndIndex=*/3,
        q11OutputType());
  }

  // Operator under test plus its collecting target. The state backend
  // defaults to the task-level heap backend over the full key-group range;
  // rescale-style restore passes a narrowed one.
  struct Harness {
    std::unique_ptr<GroupWindowAggregator> aggregator;
    CollectingTarget* target;
    std::unique_ptr<HashMapStateBackend> stateBackend;
  };

  Harness makeHarness(
      const std::shared_ptr<const GroupWindowAggregationNode>& node,
      std::unique_ptr<HashMapStateBackend> stateBackend = nullptr) {
    if (stateBackend == nullptr) {
      stateBackend = std::make_unique<HashMapStateBackend>(
          std::make_shared<const KeyedStateBackendParameters>(
              StateBackendType::HEAP, "job", "group-window-agg"));
    }
    // Every harness needs fresh operator ids and plan node ids: operator
    // memory pools are named after (planNodeId, operatorType) and the task
    // rejects duplicates.
    const auto serial = operatorSerial_++;
    const auto collectId = "collect-" + std::to_string(serial);
    auto target =
        std::make_unique<CollectingTarget>(driverCtx_.get(), serial, collectId);
    auto* targetPtr = target.get();
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(target));
    auto handler = std::make_unique<GroupWindowAggsHandler>(
        serial, driverCtx_.get(), node->aggregation());
    auto aggregator = std::make_unique<GroupWindowAggregator>(
        std::move(handler), std::move(targets), node);
    aggregator->initialize();
    aggregator->initializeStateBackend(stateBackend.get());
    return Harness{std::move(aggregator), targetPtr, std::move(stateBackend)};
  }

  // Feeds one batch of (key, rowtime-millis, value) rows. The rowtime column
  // is a velox TIMESTAMP - the form the rowtime attribute arrives in.
  StreamRecordPtr feed(
      GroupWindowAggregator& aggregator,
      const std::vector<int32_t>& keys,
      const std::vector<int64_t>& tsMillis,
      const std::vector<std::optional<int32_t>>& values = {},
      const std::vector<std::optional<RowKind>>& kinds = {},
      bool withSum = false) {
    std::vector<VectorPtr> children;
    children.push_back(makeFlatVector<int32_t>(keys));
    std::vector<Timestamp> timestamps;
    timestamps.reserve(tsMillis.size());
    for (auto millis : tsMillis) {
      timestamps.push_back(Timestamp::fromMillis(millis));
    }
    children.push_back(makeFlatVector<Timestamp>(timestamps));
    if (withSum) {
      children.push_back(makeNullableFlatVector<int32_t>(values));
    }
    auto rowVector = std::make_shared<RowVector>(
        pool(),
        intInputType(withSum),
        BufferPtr(nullptr),
        static_cast<vector_size_t>(keys.size()),
        std::move(children));
    if (kinds.empty()) {
      // appendOnly: no rowKind column, the filesystem-source form.
      auto record = std::make_shared<StreamRecord>("source", rowVector);
      aggregator.addInput(record);
      return record;
    }
    std::vector<int8_t> kindBytes;
    kindBytes.reserve(kinds.size());
    for (const auto& kind : kinds) {
      kindBytes.push_back(static_cast<int8_t>(kind.value_or(RowKind::INSERT)));
    }
    auto record = std::make_shared<StreamRecord>(
        "source",
        rowVector,
        std::static_pointer_cast<SimpleVector<int8_t>>(
            makeFlatVector<int8_t>(kindBytes)));
    aggregator.addInput(record);
    return record;
  }

  // Drains the collected emissions into parsed window rows.
  std::vector<Emitted> takeEmitted(
      CollectingTarget* target,
      bool withSum = false) {
    std::vector<Emitted> emitted;
    for (auto& element : target->collected_) {
      if (!element->isRecord()) {
        continue;
      }
      auto record = std::static_pointer_cast<StreamRecord>(element);
      const auto& output = *record->record();
      Emitted row;
      for (auto i = 0; i < output.size(); ++i) {
        row.keys.push_back(
            output.childAt(0)->as<SimpleVector<int32_t>>()->valueAt(i));
        row.count.push_back(
            output.childAt(1)->as<SimpleVector<int64_t>>()->valueAt(i));
        if (withSum) {
          row.sum.push_back(
              output.childAt(2)->as<SimpleVector<int32_t>>()->valueAt(i));
        }
        const int startIndex = withSum ? 3 : 2;
        row.windowStart = output.childAt(startIndex)
                              ->as<SimpleVector<Timestamp>>()
                              ->valueAt(i)
                              .toMillis();
        row.windowEnd = output.childAt(startIndex + 1)
                            ->as<SimpleVector<Timestamp>>()
                            ->valueAt(i)
                            .toMillis();
      }
      emitted.push_back(std::move(row));
    }
    target->collected_.clear();
    return emitted;
  }

  // A typed mapping handle over one probed key, for the process-function
  // level units.
  struct MappingHarness {
    std::unique_ptr<HeapKeyedStateBackend<RowContainerStateKey>> backend;
    std::unique_ptr<KeySelector> keySelector;
    std::shared_ptr<SessionWindowMappingState> mapping;
    RowContainerStateKey key;
    std::shared_ptr<MockFunctionContext> ctx;
  };

  MappingHarness makeMappingHarness(
      int64_t watermark,
      std::function<char*(TimeWindow)> windowAccumulators = [](TimeWindow) {
        return (char*)nullptr;
      }) {
    HashMapStateBackend taskBackend(
        std::make_shared<const KeyedStateBackendParameters>(
            StateBackendType::HEAP, "job", "group-window-agg"));
    auto spec = std::make_shared<KeyedStateSpec>(
        std::vector<column_index_t>{0}, std::vector<TypePtr>{INTEGER()});
    auto backend = createKeyedStateBackend<RowContainerStateKey>(
        taskBackend, *spec, pool());
    auto keySelector =
        std::make_unique<KeySelector>(backend->keyIndex(), spec->keyChannels());
    auto input = makeRowVector({makeFlatVector<int32_t>({1})});
    keySelector->probe(input);
    auto key = keySelector->keys()[0];
    MapStateDescriptor<TimeWindow, TimeWindow> descriptor(
        "session-window-mapping",
        std::make_shared<TimeWindowSerializer>(),
        std::make_shared<TimeWindowSerializer>(),
        pool());
    auto mapping =
        backend->getOrCreateMapState<VoidNamespace, TimeWindow, TimeWindow>(
            descriptor);
    auto ctx = std::make_shared<MockFunctionContext>(
        key, watermark, std::move(windowAccumulators));
    return MappingHarness{
        std::move(backend),
        std::move(keySelector),
        std::move(mapping),
        key,
        std::move(ctx)};
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<StatefulTask> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
  std::atomic<int32_t> operatorSerial_{0};
};

// TUMBLE: size-aligned non-overlapping windows. Two events inside one
// 10 ms window accumulate together; a third event in the next window
// keeps its own accumulator; each window fires exactly at its end - 1.
TEST_F(GroupWindowAggregatorTest, tumbleFiresAlignedWindows) {
  auto node = makeNode(
      /*withSum=*/false,
      /*gap=*/0,
      {},
      /*countStarIndex=*/0,
      static_cast<int>(WindowType::TUMBLE),
      /*size=*/10);
  auto harness = makeHarness(node);
  feed(*harness.aggregator, {1, 1, 1}, {100, 105, 111});
  // [100, 110) fires at maxTimestamp 109.
  harness.aggregator->processWatermark(109);
  auto emitted = takeEmitted(harness.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].keys[0], 1);
  EXPECT_EQ(emitted[0].count[0], 2);
  EXPECT_EQ(emitted[0].windowStart, 100);
  EXPECT_EQ(emitted[0].windowEnd, 110);
  // [110, 120) fires at 119.
  harness.aggregator->processWatermark(119);
  emitted = takeEmitted(harness.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].count[0], 1);
  EXPECT_EQ(emitted[0].windowStart, 110);
  EXPECT_EQ(emitted[0].windowEnd, 120);
  EXPECT_TRUE(takeEmitted(harness.target).empty());
}

// HOP: size 10 / slide 5 - one event at ts=100 lands in both [95, 105)
// and [100, 110); each overlapping window keeps its own accumulator and
// fires at its own end - 1, in watermark order.
TEST_F(GroupWindowAggregatorTest, hopOverlappingWindows) {
  auto node = makeNode(
      /*withSum=*/false,
      /*gap=*/0,
      {},
      /*countStarIndex=*/0,
      static_cast<int>(WindowType::HOP),
      /*size=*/10,
      /*slide=*/5);
  auto harness = makeHarness(node);
  feed(*harness.aggregator, {1}, {100});
  // [95, 105) fires at maxTimestamp 104.
  harness.aggregator->processWatermark(104);
  auto emitted = takeEmitted(harness.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].count[0], 1);
  EXPECT_EQ(emitted[0].windowStart, 95);
  EXPECT_EQ(emitted[0].windowEnd, 105);
  // [100, 110) fires at 109.
  harness.aggregator->processWatermark(109);
  emitted = takeEmitted(harness.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].count[0], 1);
  EXPECT_EQ(emitted[0].windowStart, 100);
  EXPECT_EQ(emitted[0].windowEnd, 110);
  EXPECT_TRUE(takeEmitted(harness.target).empty());
}

// Append-only input (filesystem source: no rowKind column), gap-adjacent
// events merge into one window, the watermark fires it exactly at its end
// and the start / end columns carry the merged boundaries.
TEST_F(GroupWindowAggregatorTest, sessionAppendOnly) {
  auto node = makeNode();
  auto harness = makeHarness(node);
  // Two events 5 ms apart inside the 10 ms gap: one merged window.
  feed(*harness.aggregator, {1, 1}, {100, 105});
  // Between the first window's own end (110) and the merged end (115).
  harness.aggregator->processWatermark(110);
  EXPECT_TRUE(takeEmitted(harness.target).empty());
  harness.aggregator->processWatermark(113);
  EXPECT_TRUE(takeEmitted(harness.target).empty());
  // [100, 115) fires at maxTimestamp 114.
  harness.aggregator->processWatermark(114);
  auto emitted = takeEmitted(harness.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].keys[0], 1);
  EXPECT_EQ(emitted[0].count[0], 2);
  EXPECT_EQ(emitted[0].windowStart, 100);
  EXPECT_EQ(emitted[0].windowEnd, 115);
}

// Explicit INSERT row kinds (the $row_kind trailing-column bridge form)
// behave exactly like the append-only form.
TEST_F(GroupWindowAggregatorTest, sessionExplicitInsert) {
  auto node = makeNode();
  auto harness = makeHarness(node);
  feed(*harness.aggregator, {1}, {100}, {}, {RowKind::INSERT, RowKind::INSERT});
  feed(*harness.aggregator, {1}, {103}, {}, {RowKind::INSERT});
  harness.aggregator->processWatermark(200);
  auto emitted = takeEmitted(harness.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].count[0], 2);
  EXPECT_EQ(emitted[0].windowStart, 100);
  EXPECT_EQ(emitted[0].windowEnd, 113);
}

// P0 insert-only mode: non-INSERT row kinds fail loud with zero output.
TEST_F(GroupWindowAggregatorTest, unsupportedKindFailsLoud) {
  auto node = makeNode();
  auto harness = makeHarness(node);
  VELOX_ASSERT_USER_THROW(
      feed(*harness.aggregator, {1}, {100}, {}, {RowKind::UPDATE_BEFORE}),
      "insert-only");
  VELOX_ASSERT_USER_THROW(
      feed(*harness.aggregator, {1}, {100}, {}, {RowKind::DELETE}),
      "insert-only");
  EXPECT_TRUE(takeEmitted(harness.target).empty());
}

// Session merging across a batch: in-gap events fold into one window and
// per-key windows stay isolated; the flinksql SUM keeps the INT result type
// where the prestosql baseline widens to BIGINT.
TEST_F(GroupWindowAggregatorTest, sessionMergeAndSum) {
  auto node = makeNode(/*withSum=*/true);
  auto harness = makeHarness(node);
  feed(*harness.aggregator, {1, 1, 2}, {100, 104, 300}, {10, 5, 7}, {}, true);
  harness.aggregator->processWatermark(500);
  auto emitted = takeEmitted(harness.target, /*withSum=*/true);
  ASSERT_EQ(emitted.size(), 2);
  EXPECT_EQ(emitted[0].keys[0], 1);
  EXPECT_EQ(emitted[0].count[0], 2);
  EXPECT_EQ(emitted[0].sum[0], 15);
  EXPECT_EQ(emitted[0].windowStart, 100);
  EXPECT_EQ(emitted[0].windowEnd, 114);
  EXPECT_EQ(emitted[1].keys[0], 2);
  EXPECT_EQ(emitted[1].count[0], 1);
  EXPECT_EQ(emitted[1].sum[0], 7);
  EXPECT_EQ(emitted[1].windowStart, 300);
  EXPECT_EQ(emitted[1].windowEnd, 310);
}

// The merge guard: an in-flight window set whose merge result would end at
// or before the current watermark fails loud (the retained VELOX_CHECK,
// user gate 1 decision #15) with zero output.
TEST_F(GroupWindowAggregatorTest, sessionMergeGuard) {
  auto mappingHarness = makeMappingHarness(/*watermark=*/500);
  MergingWindowSet mergingWindows(sessionAssigner(), mappingHarness.mapping);
  // The in-flight window enters the mapping first, then the cache loads it
  // (the runtime order: initializeCache reads the mapping of the key).
  mappingHarness.mapping->put(
      mappingHarness.key,
      VoidNamespace::instance(),
      TimeWindow(100, 110),
      TimeWindow(100, 110));
  mergingWindows.initializeCache(mappingHarness.key);
  auto consumer =
      std::make_shared<DefaultAccMergingConsumer>(mappingHarness.ctx, nullptr);
  auto mergingFunction =
      std::make_shared<MergingFunction>(consumer, mappingHarness.ctx, 0, true);
  VELOX_ASSERT_RUNTIME_THROW(
      (void)mergingWindows.addWindow(
          mappingHarness.key, TimeWindow(105, 115), mergingFunction),
      "cannot become earlier");
}

// Late elements: a window whose end is at or before the last watermark is
// dropped with the late counter, no output, no state.
TEST_F(GroupWindowAggregatorTest, lateDrop) {
  auto node = makeNode();
  auto harness = makeHarness(node);
  feed(*harness.aggregator, {1}, {100});
  harness.aggregator->processWatermark(200);
  EXPECT_EQ(takeEmitted(harness.target).size(), 1);
  // Everything about this element's window is in the past: dropped.
  feed(*harness.aggregator, {1}, {100});
  harness.aggregator->processWatermark(300);
  auto emitted = takeEmitted(harness.target);
  EXPECT_TRUE(emitted.empty());
  EXPECT_EQ(harness.aggregator->numLateRecordsDropped(), 1);
}

// Empty-window suppression (D1 scheme A): a window entry without state
// reports nullptr through the non-materializing read and emits nothing.
TEST_F(GroupWindowAggregatorTest, emptyWindowSuppress) {
  auto mappingHarness = makeMappingHarness(/*watermark=*/0);
  MergingWindowProcessFunction processFunction(
      sessionAssigner(), nullptr, 0, mappingHarness.mapping);
  processFunction.open(mappingHarness.ctx);
  // A mapped window whose state window has no accumulator row: the
  // existence read returns nullptr, never materializing state.
  mappingHarness.mapping->put(
      mappingHarness.key,
      VoidNamespace::instance(),
      TimeWindow(100, 110),
      TimeWindow(100, 110));
  EXPECT_EQ(
      processFunction.prepareAggregateAccumulatorForEmit(
          mappingHarness.key, TimeWindow(100, 110)),
      nullptr);

  // Operator level: the fired window emits exactly one row - no spurious
  // empty emission rides along.
  auto node = makeNode();
  auto harness = makeHarness(node);
  feed(*harness.aggregator, {1}, {100});
  harness.aggregator->processWatermark(109);
  auto emitted = takeEmitted(harness.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].count[0], 1);
}

// Timer-driven firing: the emission lands exactly when the watermark
// crosses the window's end, not before, and the order follows the
// watermark.
TEST_F(GroupWindowAggregatorTest, watermarkFire) {
  auto node = makeNode();
  auto harness = makeHarness(node);
  feed(*harness.aggregator, {1, 2}, {100, 200});
  harness.aggregator->processWatermark(108);
  EXPECT_TRUE(takeEmitted(harness.target).empty());
  // Window [100, 110) fires at maxTimestamp 109.
  harness.aggregator->processWatermark(109);
  auto first = takeEmitted(harness.target);
  ASSERT_EQ(first.size(), 1);
  EXPECT_EQ(first[0].windowStart, 100);
  harness.aggregator->processWatermark(209);
  auto second = takeEmitted(harness.target);
  ASSERT_EQ(second.size(), 1);
  EXPECT_EQ(second[0].windowStart, 200);
}

// Cleanup exactness: after the fire+cleanup pop the window is retired from
// the in-flight set, so a later in-gap element opens a NEW window instead
// of extending the old one; before the pop the same element merges.
TEST_F(GroupWindowAggregatorTest, cleanupExact) {
  // Before the pop (watermark under the window end): the new element merges.
  auto liveNode = makeNode();
  auto live = makeHarness(liveNode);
  feed(*live.aggregator, {1}, {100});
  live.aggregator->processWatermark(105);
  feed(*live.aggregator, {1}, {105});
  live.aggregator->processWatermark(300);
  auto merged = takeEmitted(live.target);
  ASSERT_EQ(merged.size(), 1);
  EXPECT_EQ(merged[0].windowStart, 100);
  EXPECT_EQ(merged[0].windowEnd, 115);
  EXPECT_EQ(merged[0].count[0], 2);

  // After the pop (watermark exactly at maxTimestamp 109): [100, 110) is
  // fired and cleaned, so the same element opens a fresh [105, 115) window.
  auto firedNode = makeNode();
  auto fired = makeHarness(firedNode);
  feed(*fired.aggregator, {1}, {100});
  fired.aggregator->processWatermark(109);
  takeEmitted(fired.target);
  feed(*fired.aggregator, {1}, {105});
  fired.aggregator->processWatermark(300);
  auto separate = takeEmitted(fired.target);
  ASSERT_EQ(separate.size(), 1);
  EXPECT_EQ(separate[0].windowStart, 105);
  EXPECT_EQ(separate[0].windowEnd, 115);
  EXPECT_EQ(separate[0].count[0], 1);
}

// Window namespace state round trip: snapshot -> new operator -> restore ->
// continued feeding behaves like the uninterrupted run (keys, counts and
// merged windows all equal).
TEST_F(GroupWindowAggregatorTest, nsStateRoundtrip) {
  auto referenceNode = makeNode();
  auto reference = makeHarness(referenceNode);
  feed(*reference.aggregator, {1, 2}, {100, 300});
  feed(*reference.aggregator, {1}, {104});
  reference.aggregator->processWatermark(500);
  auto referenceEmitted = takeEmitted(reference.target);

  auto node = makeNode();
  auto live = makeHarness(node);
  feed(*live.aggregator, {1, 2}, {100, 300});
  auto records = live.aggregator->snapshotKeyedState();
  ASSERT_EQ(records.size(), 1);

  auto restoredNode = makeNode();
  auto restored = makeHarness(restoredNode);
  restored.aggregator->restoreKeyedState(records);
  // The timer queue lives outside the snapshot plane but the rebuild derived
  // it back from the restored state: two live windows (one per key) pending.
  EXPECT_EQ(restored.aggregator->timersSize(), 2);
  feed(*restored.aggregator, {1}, {104});
  restored.aggregator->processWatermark(500);
  auto restoredEmitted = takeEmitted(restored.target);

  ASSERT_EQ(restoredEmitted.size(), referenceEmitted.size());
  for (size_t i = 0; i < referenceEmitted.size(); ++i) {
    EXPECT_TRUE(restoredEmitted[i].sameAs(referenceEmitted[i]))
        << "restored row " << i << " diverged from the uninterrupted run";
  }
}

// Restore under a narrower key-group range picks up the overlapping part of
// the stream and skips the rest (rescale-style restore).
TEST_F(GroupWindowAggregatorTest, rescaleRestoreKeepsOnlyOverlappingKeyGroups) {
  auto node = makeNode();
  auto harness = makeHarness(node);
  // Enough keys that their key groups spread over both halves of the 128
  // key-group space.
  std::vector<int32_t> keys;
  std::vector<int64_t> timestamps;
  for (int32_t k = 0; k < 64; ++k) {
    keys.push_back(k);
    timestamps.push_back(100);
  }
  feed(*harness.aggregator, keys, timestamps);
  takeEmitted(harness.target);

  auto records = harness.aggregator->snapshotKeyedState();
  ASSERT_EQ(records.size(), 1);

  // Key-group ownership of every key, as the upstream exchange would compute
  // it: probe a lab table with the same key schema and maxParallelism.
  KeyTable labTable({INTEGER()}, 128, pool());
  KeySelector labSelector(labTable, {0});
  auto labInput = makeRowVector({makeFlatVector<int32_t>(keys)});
  labSelector.probe(labInput);
  std::vector<int32_t> firstHalfKeys;
  std::vector<int32_t> secondHalfKeys;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (labSelector.keys()[i].keyGroup() < 64) {
      firstHalfKeys.push_back(keys[i]);
    } else {
      secondHalfKeys.push_back(keys[i]);
    }
  }
  ASSERT_EQ(firstHalfKeys.size() + secondHalfKeys.size(), 64);
  ASSERT_FALSE(firstHalfKeys.empty());
  ASSERT_FALSE(secondHalfKeys.empty());

  // Split the snapshot by restoring under the first and second key-group
  // halves: each half keeps only the keys whose key groups fall inside its
  // range, and together the halves keep every key exactly once.
  auto firstHalfBackend = std::make_unique<HashMapStateBackend>(
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP, "job", "group-window-agg-a", 128, 0, 64));
  auto firstHalf = makeHarness(makeNode(), std::move(firstHalfBackend));
  firstHalf.aggregator->restoreKeyedState(records);

  auto secondHalfBackend = std::make_unique<HashMapStateBackend>(
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP, "job", "group-window-agg-b", 128, 64, 64));
  auto secondHalf = makeHarness(makeNode(), std::move(secondHalfBackend));
  secondHalf.aggregator->restoreKeyedState(records);

  // Continue each half with its own keys only: an in-gap second event
  // merges with the restored window, so the fired count proves the snapshot
  // accumulators survived the half restore (an un-restored key would fire
  // with count 1).
  std::vector<int64_t> secondTs(firstHalfKeys.size(), 105);
  feed(*firstHalf.aggregator, firstHalfKeys, secondTs);
  firstHalf.aggregator->processWatermark(200);
  auto firstEmitted = takeEmitted(firstHalf.target);
  std::vector<int64_t> secondHalfTs(secondHalfKeys.size(), 105);
  feed(*secondHalf.aggregator, secondHalfKeys, secondHalfTs);
  secondHalf.aggregator->processWatermark(200);
  auto secondEmitted = takeEmitted(secondHalf.target);

  ASSERT_EQ(firstEmitted.size(), firstHalfKeys.size());
  ASSERT_EQ(secondEmitted.size(), secondHalfKeys.size());
  std::set<int32_t> seenKeys;
  for (const auto& row : firstEmitted) {
    seenKeys.insert(row.keys[0].value());
    EXPECT_EQ(row.count[0], 2);
    EXPECT_EQ(row.windowStart, 100);
    EXPECT_EQ(row.windowEnd, 115);
  }
  for (const auto& row : secondEmitted) {
    seenKeys.insert(row.keys[0].value());
    EXPECT_EQ(row.count[0], 2);
    EXPECT_EQ(row.windowStart, 100);
    EXPECT_EQ(row.windowEnd, 115);
  }
  EXPECT_EQ(seenKeys.size(), 64);
}

// Timer restore semantics (A2, C3): the timer queue lives outside the
// snapshot plane and restore rebuilds it from the restored state - the
// rebuilt set equals the pre-snapshot pending set (fire = the actual
// window's end - 1, cleanup at the same point, one deduped entry per live
// window) and the due items fire and clean as the watermark advances,
// without any re-feeding (the gap the rebuild closes).
TEST_F(GroupWindowAggregatorTest, timerRestoreSemantics) {
  auto node = makeNode();
  auto live = makeHarness(node);
  // Window [100, 110): fire / cleanup timer at 109, watermark at 105
  // leaves it pending.
  feed(*live.aggregator, {1}, {100});
  live.aggregator->processWatermark(105);
  const size_t pendingBeforeSnapshot = live.aggregator->timersSize();
  ASSERT_EQ(pendingBeforeSnapshot, 1);
  auto records = live.aggregator->snapshotKeyedState();
  ASSERT_EQ(records.size(), 1);

  auto restoredNode = makeNode();
  auto restored = makeHarness(restoredNode);
  restored.aggregator->restoreKeyedState(records);
  // The rebuilt set equals the pre-snapshot pending set.
  EXPECT_EQ(restored.aggregator->timersSize(), pendingBeforeSnapshot);
  EXPECT_FALSE(restored.aggregator->timersEmpty());
  // The rebuilt fire timer lands exactly at the actual window's end - 1
  // (109): below it nothing emits.
  restored.aggregator->processWatermark(108);
  EXPECT_TRUE(takeEmitted(restored.target).empty());
  // The watermark alone fires the restored window - no new input needed -
  // and the cleanup at the same point retires it.
  restored.aggregator->processWatermark(109);
  auto emitted = takeEmitted(restored.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].count[0], 1);
  EXPECT_EQ(emitted[0].windowStart, 100);
  EXPECT_EQ(emitted[0].windowEnd, 110);
  // Cleanup was in place with the fire: the window is retired, so the same
  // in-gap element is late against the advanced watermark and dropped (an
  // un-retired window would have absorbed it into a merged window instead).
  restored.aggregator->processWatermark(200);
  feed(*restored.aggregator, {1}, {104});
  restored.aggregator->processWatermark(300);
  EXPECT_TRUE(takeEmitted(restored.target).empty());
  EXPECT_EQ(restored.aggregator->numLateRecordsDropped(), 1);
}

// TUMBLE restore-rebuild (A2 on the non-merging path): the window IS the
// state namespace and no mapping state exists, so the timers are rebuilt
// from the aggregating state's (key, window) entries alone; the watermark
// alone fires the restored window.
TEST_F(GroupWindowAggregatorTest, tumbleRestoreRebuildsTimersFromState) {
  auto node = makeNode(
      /*withSum=*/false,
      /*gap=*/0,
      {},
      /*countStarIndex=*/0,
      static_cast<int>(WindowType::TUMBLE),
      /*size=*/10);
  auto live = makeHarness(node);
  // Window [100, 110): fire / cleanup timer at 109, watermark at 105
  // leaves it pending.
  feed(*live.aggregator, {1}, {100});
  live.aggregator->processWatermark(105);
  ASSERT_EQ(live.aggregator->timersSize(), 1);
  auto records = live.aggregator->snapshotKeyedState();
  ASSERT_EQ(records.size(), 1);

  auto restoredNode = makeNode(
      /*withSum=*/false,
      /*gap=*/0,
      {},
      /*countStarIndex=*/0,
      static_cast<int>(WindowType::TUMBLE),
      /*size=*/10);
  auto restored = makeHarness(restoredNode);
  // No session mapping is registered on this path; the rebuild must not
  // touch one (the pre-fix null dereference).
  restored.aggregator->restoreKeyedState(records);
  EXPECT_EQ(restored.aggregator->timersSize(), 1);
  restored.aggregator->processWatermark(108);
  EXPECT_TRUE(takeEmitted(restored.target).empty());
  restored.aggregator->processWatermark(109);
  auto emitted = takeEmitted(restored.target);
  ASSERT_EQ(emitted.size(), 1);
  EXPECT_EQ(emitted[0].count[0], 1);
  EXPECT_EQ(emitted[0].windowStart, 100);
  EXPECT_EQ(emitted[0].windowEnd, 110);
}

// HOP restore-rebuild: every overlapping window of the event is a live
// (key, window) entry, each rebuilt with its own fire / cleanup pair, in
// watermark order.
TEST_F(GroupWindowAggregatorTest, hopRestoreRebuildsTimersFromState) {
  auto liveNode = makeNode(
      /*withSum=*/false,
      /*gap=*/0,
      {},
      /*countStarIndex=*/0,
      static_cast<int>(WindowType::HOP),
      /*size=*/10,
      /*slide=*/5);
  auto live = makeHarness(liveNode);
  // Windows [95, 105) and [100, 110): timers at 104 and 109.
  feed(*live.aggregator, {1}, {100});
  live.aggregator->processWatermark(103);
  ASSERT_EQ(live.aggregator->timersSize(), 2);
  auto records = live.aggregator->snapshotKeyedState();
  ASSERT_EQ(records.size(), 1);

  auto restoredNode = makeNode(
      /*withSum=*/false,
      /*gap=*/0,
      {},
      /*countStarIndex=*/0,
      static_cast<int>(WindowType::HOP),
      /*size=*/10,
      /*slide=*/5);
  auto restored = makeHarness(restoredNode);
  restored.aggregator->restoreKeyedState(records);
  EXPECT_EQ(restored.aggregator->timersSize(), 2);
  restored.aggregator->processWatermark(104);
  auto first = takeEmitted(restored.target);
  ASSERT_EQ(first.size(), 1);
  EXPECT_EQ(first[0].windowStart, 95);
  EXPECT_EQ(first[0].windowEnd, 105);
  restored.aggregator->processWatermark(109);
  auto second = takeEmitted(restored.target);
  ASSERT_EQ(second.size(), 1);
  EXPECT_EQ(second[0].windowStart, 100);
  EXPECT_EQ(second[0].windowEnd, 110);
}

// The per-key in-flight window cache is an LRU of 10000 keys: after an
// eviction the evicted key's window set reloads from the mapping state, and
// the merge math still sees the in-flight windows.
TEST_F(GroupWindowAggregatorTest, mergingSetLru) {
  auto node = makeNode();
  auto harness = makeHarness(node);
  // k=0's window goes into the cache first, then 10000 other keys fill the
  // cache and evict it; k=0's second row (same batch) must still merge with
  // its in-flight [100, 110) window reloaded from the mapping state.
  std::vector<int32_t> keys;
  std::vector<int64_t> timestamps;
  keys.push_back(0);
  timestamps.push_back(100);
  for (int32_t k = 1; k <= 10000; ++k) {
    keys.push_back(k);
    timestamps.push_back(100);
  }
  keys.push_back(0);
  timestamps.push_back(105);
  feed(*harness.aggregator, keys, timestamps);
  harness.aggregator->processWatermark(500);
  auto emitted = takeEmitted(harness.target);
  ASSERT_EQ(emitted.size(), 10001);
  int mergedCount = 0;
  for (const auto& row : emitted) {
    if (row.keys[0].value() == 0) {
      ++mergedCount;
      EXPECT_EQ(row.windowStart, 100);
      EXPECT_EQ(row.windowEnd, 115);
      EXPECT_EQ(row.count[0], 2);
    }
  }
  ASSERT_EQ(mergedCount, 1);
}

// The ghost-window fix: assignStateNamespace returns the state windows of
// the kept actual windows only - no default TimeWindow(-1, -1) entries ever
// reach state resolution.
TEST_F(GroupWindowAggregatorTest, sessionMergeNoGhost) {
  auto mappingHarness = makeMappingHarness(/*watermark=*/0);
  MergingWindowProcessFunction processFunction(
      sessionAssigner(), nullptr, 0, mappingHarness.mapping);
  processFunction.open(mappingHarness.ctx);
  auto stateWindows = processFunction.assignStateNamespace(
      mappingHarness.key, /*timestamp=*/100);
  auto actualWindows = processFunction.assignActualWindows();
  ASSERT_EQ(actualWindows.size(), 1);
  EXPECT_EQ(actualWindows[0].start(), 100);
  EXPECT_EQ(actualWindows[0].end(), 110);
  ASSERT_EQ(stateWindows.size(), 1);
  EXPECT_EQ(stateWindows[0].start(), 100);
  EXPECT_EQ(stateWindows[0].end(), 110);
  for (const auto& window : stateWindows) {
    EXPECT_TRUE(window.valid())
        << "a default TimeWindow(-1, -1) leaked into state resolution";
  }
}

// Real-path: the StatefulPlanner dispatch face. The makeNode product is
// wrapped in a StatefulPlanNode and planned through StatefulPlanner::plan
// itself - the same dispatch the runtime driver drives. The makeHarness
// units construct the operator directly and never touch the planner, so a
// dispatch-face wiring error (a broken handler cast, a dropped node
// pointer, a targets mismatch) would only surface in e2e otherwise.
TEST_F(GroupWindowAggregatorTest, realPlannerPathBuildsGroupWindowAggregator) {
  auto node = makeNode();
  core::PlanFragment planFragment;
  planFragment.planNode = std::make_shared<StatefulPlanNode>(
      node, std::vector<core::PlanNodePtr>{});
  auto chain = StatefulPlanner::plan(planFragment, driverCtx_.get(), nullptr);

  // The dispatch face routes the GroupWindowAggregationNode into a
  // GroupWindowAggregator wrapping the transformed GroupWindowAggsHandler,
  // with no downstream targets.
  auto* aggregator = dynamic_cast<GroupWindowAggregator*>(chain.get());
  ASSERT_NE(aggregator, nullptr);
  auto* handler = dynamic_cast<GroupWindowAggsHandler*>(aggregator->op().get());
  ASSERT_NE(handler, nullptr);
  EXPECT_TRUE(aggregator->targets().empty());

  // initialize resolves the plan-declared aggregates on the handler (the
  // flinksql COUNT(*) the default node declares).
  aggregator->initialize();
  ASSERT_EQ(handler->aggregates().size(), 1);
  EXPECT_NE(handler->aggregates()[0].function, nullptr);

  // The planned chain runs the state lifecycle too: feed one session
  // window, the fire timer registers, and the watermark pops it (the
  // chain has no downstream target, so the timer queue is the observable
  // surface). The backend is declared after the chain, so it destructs
  // first (the reverse of the declaration order) - the same order as
  // makeHarness, whose Harness members declare the aggregator before the
  // backend.
  auto stateBackend = std::make_unique<HashMapStateBackend>(
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP, "job", "group-window-agg"));
  aggregator->initializeStateBackend(stateBackend.get());
  feed(*aggregator, {1, 1}, {100, 105});
  EXPECT_EQ(aggregator->timersSize(), 1);
  aggregator->processWatermark(114);
  EXPECT_TRUE(aggregator->timersEmpty());
}

// The production q11 shape: the planner pushes all four window properties
// into the aggregate node's declared output (keys + aggs + [start, end,
// rowtime, proctime]), so the emission must assemble every declared column
// - the Java bridge slices rowType.size() children off the emitted row, and
// a narrower emission (the pre-fix form assembled only start / end) crashes
// the first window's emission. The time attributes carry the Flink
// semantics: rowtime = the window's maxTimestamp, the proctime attribute
// null at the position.
TEST_F(GroupWindowAggregatorTest, emitMatchesFullPropertyDeclaredOutput) {
  auto node = makeQ11Node();
  auto harness = makeHarness(node);
  // Two events 5 ms apart inside the 10 ms gap: one merged window
  // [100, 115), firing at its maxTimestamp 114.
  feed(*harness.aggregator, {1, 1}, {100, 105});
  harness.aggregator->processWatermark(114);

  std::vector<RowVectorPtr> rows;
  for (auto& element : harness.target->collected_) {
    if (element->isRecord()) {
      rows.push_back(std::static_pointer_cast<StreamRecord>(element)->record());
    }
  }
  ASSERT_EQ(rows.size(), 1);
  const auto& output = *rows[0];
  // The emission width matches the declared width (6 columns).
  ASSERT_EQ(output.type()->size(), 6);
  ASSERT_EQ(output.childrenSize(), 6);
  EXPECT_EQ(output.childAt(0)->as<SimpleVector<int32_t>>()->valueAt(0), 1);
  EXPECT_EQ(output.childAt(1)->as<SimpleVector<int64_t>>()->valueAt(0), 2);
  EXPECT_EQ(
      output.childAt(2)->as<SimpleVector<Timestamp>>()->valueAt(0).toMillis(),
      100);
  EXPECT_EQ(
      output.childAt(3)->as<SimpleVector<Timestamp>>()->valueAt(0).toMillis(),
      115);
  // The rowtime attribute: window.maxTimestamp() = end - 1.
  EXPECT_EQ(
      output.childAt(4)->as<SimpleVector<Timestamp>>()->valueAt(0).toMillis(),
      114);
  // The proctime attribute: null at the position (Flink semantics).
  EXPECT_TRUE(output.childAt(5)->isNullAt(0));
}

// The layout guard: a hand-built node whose window start column sits among
// the key columns (an interleaved order the planner never produces) fails
// loud at emission instead of silently assembling mismatched names and
// types.
TEST_F(GroupWindowAggregatorTest, emitRejectsInterleavedWindowColumns) {
  const auto serial = operatorSerial_++;
  auto handler = std::make_shared<GroupWindowAggsHandlerNode>(
      core::PlanNodeId{"window-handler-" + std::to_string(serial)},
      intOutputType(false),
      windowAggregates(false),
      /*countStarIndex=*/0);
  auto inputType = intInputType(false);
  auto spec = std::make_shared<KeyedStateSpec>(
      std::vector<column_index_t>{0},
      std::vector<TypePtr>{inputType->childAt(0)});
  // windowStartIndex = 0: the start column interleaved with the keys
  // (numKeys = 1, numAggs = 1, so the valid tail positions are 2 and 3).
  auto node = std::make_shared<GroupWindowAggregationNode>(
      core::PlanNodeId{"window-agg-" + std::to_string(serial)},
      handler,
      spec,
      inputType,
      /*allowedLateness=*/0,
      /*produceUpdates=*/false,
      /*rowtimeIndex=*/1,
      /*isEventTime=*/true,
      /*windowType=*/static_cast<int>(WindowType::SESSION),
      /*gap=*/10,
      /*size=*/0,
      /*slide=*/0,
      /*windowStartIndex=*/0,
      /*windowEndIndex=*/3,
      intOutputType(false));
  auto harness = makeHarness(node);
  feed(*harness.aggregator, {1}, {100});
  VELOX_ASSERT_RUNTIME_THROW(
      harness.aggregator->processWatermark(200), "output tail");
}

} // namespace
} // namespace facebook::velox::stateful::test
