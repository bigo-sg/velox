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

#include "velox/experimental/stateful/agg/GroupAggregator.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/EmptyOperator.h"
#include "velox/experimental/stateful/KeyedStateSpec.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StatefulTask.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/agg/RetractableAggregate.h"
#include "velox/experimental/stateful/state/HashMapStateBackend.h"
#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/SimpleNumericAggregate.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::stateful::test {
namespace {

// A count with retraction support, registered under the SQL name "count"
// (both the no-argument count(*) and the one-argument count(x) forms) with
// overwrite, so this test binary resolves every count - the plan's own and
// the operator's hidden record counter - to one function that implements
// RetractableAggregate. The production count families land with the flinksql
// aggregates module; the operator path only needs a retract-capable count,
// which this fixture stands in for. Semantics follow Flink's
// CountAggFunction/Count1AggFunction: count(*) counts every row, count(x)
// counts non-null arguments, both filters apply to accumulation and
// retraction alike, and retraction does not stop at zero.
class RetractableCountForTest
    : public functions::aggregate::
          SimpleNumericAggregate<bool, int64_t, int64_t>,
      public stateful::RetractableAggregate {
  using BaseAggregate =
      functions::aggregate::SimpleNumericAggregate<bool, int64_t, int64_t>;

 public:
  RetractableCountForTest() : BaseAggregate(BIGINT()) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::doExtractValues(groups, numGroups, result, [&](char* group) {
      return *value<int64_t>(group);
    });
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    countSelected(groups, rows, args, 1);
  }

  void retractRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    countSelected(groups, rows, args, -1);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      addToGroup(group, rows.countSelected());
      return;
    }
    DecodedVector decoded(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        addToGroup(group, rows.countSelected());
      }
    } else if (decoded.mayHaveNulls()) {
      int64_t nonNullCount = 0;
      rows.applyToSelected([&](vector_size_t i) {
        if (!decoded.isNullAt(i)) {
          ++nonNullCount;
        }
      });
      addToGroup(group, nonNullCount);
    } else {
      addToGroup(group, rows.countSelected());
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t i) {
      addToGroup(groups[i], decodedIntermediate_.valueAt<int64_t>(i));
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);
    int64_t count = 0;
    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedIntermediate_.isNullAt(i)) {
        count += decodedIntermediate_.valueAt<int64_t>(i);
      }
    });
    addToGroup(group, count);
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      // The result of count is never null.
      *value<int64_t>(groups[i]) = static_cast<int64_t>(0);
    }
  }

 private:
  // Adds 'delta' (+1 accumulation, -1 retraction) per selected row, with
  // the argument's null filter applying to both directions.
  void countSelected(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      int64_t delta) {
    if (args.empty()) {
      rows.applyToSelected(
          [&](vector_size_t i) { addToGroup(groups[i], delta); });
      return;
    }
    DecodedVector decoded(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        rows.applyToSelected(
            [&](vector_size_t i) { addToGroup(groups[i], delta); });
      }
    } else if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decoded.isNullAt(i)) {
          addToGroup(groups[i], delta);
        }
      });
    } else {
      rows.applyToSelected(
          [&](vector_size_t i) { addToGroup(groups[i], delta); });
    }
  }

  inline void addToGroup(char* group, int64_t count) {
    *value<int64_t>(group) += count;
  }

  DecodedVector decodedIntermediate_;
};

void registerRetractableCountForTest() {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("bigint")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("bigint")
          .argumentType("T")
          .build(),
  };
  exec::registerAggregateFunction(
      "count",
      std::move(signatures),
      [](core::AggregationNode::Step /*step*/,
         const std::vector<TypePtr>& argTypes,
         const TypePtr& /*resultType*/,
         const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(argTypes.size(), 1, "count takes at most one argument");
        return std::make_unique<RetractableCountForTest>();
      },
      false /*registerCompanionFunctions*/,
      true /*overwrite*/);
}

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

// Downstream collector: keeps every record the aggregator pushes. The spy
// plan node id must be unique per driver: two collectors with the same id
// would register duplicate operator memory pools.
class CaptureTarget : public StatefulOperator {
 public:
  CaptureTarget(exec::DriverCtx* driverCtx, std::string spyNodeId)
      : StatefulOperator(
            std::make_unique<SpyOperator>(
                driverCtx,
                ROW({"k", "c"}, {BIGINT(), BIGINT()}),
                std::move(spyNodeId)),
            {}) {}

  void addInput(StreamElementPtr input) override {
    records_.push_back(std::move(input));
  }

  void advance() override {}

  const std::vector<StreamElementPtr>& records() const {
    return records_;
  }

  void clear() {
    records_.clear();
  }

 private:
  std::vector<StreamElementPtr> records_;
};

class GroupAggregatorTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    aggregate::prestosql::registerAllAggregateFunctions();
    // The retract-capable count fixture replaces the baseline count for this
    // binary: the operator's retraction path needs it, and count(x) keeps
    // the baseline's null-skipping semantics.
    registerRetractableCountForTest();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"},
        std::vector<RowVectorPtr>{
            makeRowVector({"k", "c"}, {bigints({1}), bigints({1})})});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "GroupAggregatorTest_task",
        std::move(planFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kParallel);
    driver_ = exec::Driver::testingCreate();
    driverCtx_ = std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0);
    driverCtx_->driver = driver_.get();

    taskBackend_ = std::make_unique<HashMapStateBackend>(
        std::make_shared<const KeyedStateBackendParameters>(
            StateBackendType::HEAP, "job", "group-agg"));
  }

  void TearDown() override {
    driverCtx_.reset();
    driver_.reset();
    task_.reset();
    executor_.reset();
    OperatorTestBase::TearDown();
  }

  // One sum(BIGINT) over channel 1, grouped by key channel 0.
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

  // count(*): the qualifying record-count column a retracting plan can
  // carry itself.
  core::AggregationNode::Aggregate countStar() {
    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::make_shared<core::CallTypedExpr>(
        BIGINT(), std::vector<core::TypedExprPtr>{}, "count");
    aggregate.rawInputTypes = {};
    return aggregate;
  }

  // The baseline avg(BIGINT): DOUBLE result through a row(double, bigint)
  // accumulator, the multi-column intermediate the operator must carry.
  core::AggregationNode::Aggregate avgOfC() {
    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::make_shared<core::CallTypedExpr>(
        DOUBLE(), std::vector<core::TypedExprPtr>{fieldC()}, "avg");
    aggregate.rawInputTypes = {BIGINT()};
    return aggregate;
  }

  // sum(c) with a FILTER(mask) over the boolean input column 'f'.
  core::AggregationNode::Aggregate sumOfCMasked() {
    auto aggregate = sumOfC();
    aggregate.mask =
        std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "f");
    return aggregate;
  }

  // sum(<constant 10>): the argument resolves to kConstantChannel with a
  // shared constant vector instead of an input column.
  core::AggregationNode::Aggregate sumOfConstant() {
    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::make_shared<core::CallTypedExpr>(
        BIGINT(),
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::ConstantTypedExpr>(
                BIGINT(), variant(int64_t(10)))},
        "sum");
    aggregate.rawInputTypes = {BIGINT()};
    return aggregate;
  }

  // The default input schema is ROW<k BIGINT, c BIGINT> with key channel 0;
  // tests with masks or composite keys pass their own schema and key channels.
  // The node id (and the handler's) is drawn from the per-test counter, so the
  // two aggregators of a snapshot round-trip never collide on pool names.
  std::shared_ptr<const GroupAggregationNode> makeNode(
      const std::vector<core::AggregationNode::Aggregate>& aggregates,
      const RowTypePtr& outputType,
      bool generateUpdateBefore,
      bool needRetraction = false,
      const RowTypePtr& inputType = ROW({"k", "c"}, {BIGINT(), BIGINT()}),
      const std::vector<column_index_t>& keyChannels = {0},
      const std::vector<TypePtr>& keyTypes = {BIGINT()}) {
    const auto id = nextNodeId();
    auto handler = std::make_shared<const GroupAggsHandlerNode>(
        core::PlanNodeId{id + "-handler"},
        inputType,
        outputType,
        aggregates,
        generateUpdateBefore,
        needRetraction);
    auto keyedStateSpec =
        std::make_shared<const KeyedStateSpec>(keyChannels, keyTypes);
    return std::make_shared<const GroupAggregationNode>(
        core::PlanNodeId{id},
        handler,
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        keyedStateSpec,
        outputType);
  }

  std::unique_ptr<GroupAggregator> makeAggregator(
      const std::shared_ptr<const GroupAggregationNode>& node,
      std::unique_ptr<StatefulOperator> capture) {
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(capture));
    auto op = std::make_unique<EmptyOperator>(0, driverCtx_.get(), node);
    auto aggregator = std::make_unique<GroupAggregator>(
        std::move(op), std::move(targets), node);
    // The production lifecycle order: the task initializes the operator
    // chain before handing the state backend over.
    aggregator->initialize();
    aggregator->initializeStateBackend(taskBackend_.get());
    return aggregator;
  }

  // An append-only record (no rowKind column).
  StreamElementPtr appendOnlyRecord(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& c) {
    return std::make_shared<StreamRecord>(
        "group-agg", makeRowVector({"k", "c"}, {bigints(k), bigints(c)}));
  }

  // An INSERT-only record carrying an explicit rowKind column.
  StreamElementPtr insertRecord(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& c) {
    return std::make_shared<StreamRecord>(
        "group-agg",
        makeRowVector({"k", "c"}, {bigints(k), bigints(c)}),
        makeFlatVector<int8_t>(std::vector<int8_t>(k.size(), 0)));
  }

  StreamElementPtr recordWithKinds(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& c,
      const std::vector<RowKind>& kinds) {
    std::vector<int8_t> rawKinds;
    rawKinds.reserve(kinds.size());
    for (auto kind : kinds) {
      rawKinds.push_back(static_cast<int8_t>(kind));
    }
    return std::make_shared<StreamRecord>(
        "group-agg",
        makeRowVector({"k", "c"}, {bigints(k), bigints(c)}),
        makeFlatVector<int8_t>(rawKinds));
  }

  // ROW<k BIGINT, c BIGINT, f BOOLEAN> input for masked aggregation.
  StreamElementPtr maskedRecord(
      const std::vector<int64_t>& k,
      const std::vector<int64_t>& c,
      const std::vector<std::optional<bool>>& f) {
    return std::make_shared<StreamRecord>(
        "group-agg",
        makeRowVector(
            {"k", "c", "f"},
            {bigints(k), bigints(c), makeNullableFlatVector<bool>(f)}));
  }

  // ROW<k1 BIGINT, k2 VARCHAR, c BIGINT> input for composite-key grouping.
  StreamElementPtr compositeKeyRecord(
      const std::vector<int64_t>& k1,
      const std::vector<std::string>& k2,
      const std::vector<int64_t>& c) {
    return std::make_shared<StreamRecord>(
        "group-agg",
        makeRowVector(
            {"k1", "k2", "c"},
            {bigints(k1), makeFlatVector<std::string>(k2), bigints(c)}));
  }

  StreamRecordPtr recordAt(const CaptureTarget& capture, size_t index) {
    VELOX_CHECK_LT(index, capture.records().size());
    auto record =
        std::static_pointer_cast<StreamRecord>(capture.records()[index]);
    EXPECT_TRUE(record->isRecord());
    return record;
  }

  void assertRowKinds(
      const StreamRecordPtr& record,
      const std::vector<RowKind>& kinds) {
    ASSERT_NE(record->rowKind(), nullptr);
    ASSERT_EQ(record->record()->size(), kinds.size());
    for (size_t i = 0; i < kinds.size(); ++i) {
      EXPECT_EQ(record->rowKind()->valueAt(i), static_cast<int8_t>(kinds[i]))
          << "at row " << i;
    }
  }

  core::TypedExprPtr fieldC() {
    return std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c");
  }

  // Operator pool names are keyed by plan node id, so every operator a test
  // puts on one driver needs a distinct node id.
  std::string nextNodeId() {
    return "node_" + std::to_string(nextNodeId_++);
  }

  int32_t nextNodeId_{0};

  // A single-element braced argument to makeFlatVector resolves to the
  // (vector_size_t size) overload and builds a garbage vector; routing
  // through these helpers always picks the value-list overload.
  VectorPtr bigints(const std::vector<int64_t>& values) {
    return makeFlatVector<int64_t>(values);
  }

  VectorPtr doubles(const std::vector<double>& values) {
    return makeFlatVector<double>(values);
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
  std::unique_ptr<HashMapStateBackend> taskBackend_;
};

// The full changelog protocol over three batches: +I for first appearances,
// -U followed by +U for changed keys, nothing for keys whose final values
// did not change (sum + 0).
TEST_F(GroupAggregatorTest, testSumChangelogWithUpdateBefore) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true),
      std::move(capture));

  // Batch 1: two new keys emit +I with their initial sums.
  aggregator->addInput(appendOnlyRecord({1, 2}, {10, 5}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT, RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({10, 5}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 2: an existing key's sum changes: one record pairing the -U row
  // with the previous value and the +U row with the new one, adjacent.
  aggregator->addInput(appendOnlyRecord({1}, {7}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({10, 17}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 3: adding 0 leaves the sum unchanged: no output at all.
  aggregator->addInput(appendOnlyRecord({2}, {0}));
  EXPECT_TRUE(capturePtr->records().empty());

  // Batch 4: a new key alongside an unchanged key: only the new key emits.
  aggregator->addInput(appendOnlyRecord({3, 1}, {100, 0}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({3}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({100}), record->record()->childAt(1));
}

// Without generateUpdateBefore a changed key emits only the +U record.
TEST_F(GroupAggregatorTest, testNoUpdateBeforeEmitsUpdateAfterOnly) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/false),
      std::move(capture));

  aggregator->addInput(appendOnlyRecord({1}, {10}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  capturePtr->clear();

  aggregator->addInput(appendOnlyRecord({1}, {7}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({17}), record->record()->childAt(1));
}

// Explicitly row-kind-carrying INSERT input behaves like append-only input.
TEST_F(GroupAggregatorTest, testInsertRowKindInput) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true),
      std::move(capture));

  aggregator->addInput(insertRecord({1, 1}, {10, 20}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({30}), record->record()->childAt(1));
}

// Three aggregates in one operator, including avg's multi-column accumulator
// and final conversion; avg(BIGINT) yields DOUBLE (40/3 -> 13.333...), and
// null input rows leave every final value unchanged.
TEST_F(GroupAggregatorTest, testMultipleAggregatesAndNulls) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC(), countOfC(), avgOfC()},
          ROW({"k", "s", "cnt", "a"}, {BIGINT(), BIGINT(), BIGINT(), DOUBLE()}),
          /*generateUpdateBefore=*/true),
      std::move(capture));

  // Batch 1: +I (1, 30, 2, 15.0) and (2, 5, 1, 5.0).
  aggregator->addInput(appendOnlyRecord({1, 1, 2}, {10, 20, 5}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT, RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({30, 5}), record->record()->childAt(1));
  facebook::velox::test::assertEqualVectors(
      bigints({2, 1}), record->record()->childAt(2));
  facebook::velox::test::assertEqualVectors(
      makeFlatVector<double>(std::vector<double>{15.0, 5.0}),
      record->record()->childAt(3));
  capturePtr->clear();

  // Batch 2: a null row changes nothing: no output.
  aggregator->addInput(std::make_shared<StreamRecord>(
      "group-agg",
      makeRowVector(
          {"k", "c"},
          {bigints({2}), makeNullableFlatVector<int64_t>({std::nullopt})})));
  EXPECT_TRUE(capturePtr->records().empty());

  // Batch 3: key 1 advances: one record pairing the -U row (1, 30, 2, 15.0)
  // with the +U row (1, 40, 3, 40/3) adjacent.
  aggregator->addInput(appendOnlyRecord({1}, {10}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({30, 40}), record->record()->childAt(1));
  facebook::velox::test::assertEqualVectors(
      bigints({2, 3}), record->record()->childAt(2));
  facebook::velox::test::assertEqualVectors(
      makeFlatVector<double>(std::vector<double>{15.0, 40.0 / 3}),
      record->record()->childAt(3));
}

// sum(c) FILTER (WHERE f): masked-out rows (false or null f) never
// accumulate, but a key whose every row is masked still materializes its
// state and emits +I with a null sum on first sight - matching Flink, whose
// GroupAggFunction emits on the first record of a key regardless of the
// filter.
TEST_F(GroupAggregatorTest, testMaskFiltersAccumulation) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfCMasked()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/false,
          ROW({"k", "c", "f"}, {BIGINT(), BIGINT(), BOOLEAN()})),
      std::move(capture));

  // Batch 1: key 1 passes the filter, key 2 is masked out entirely: +I (1, 10)
  // and +I (2, null).
  aggregator->addInput(maskedRecord({1, 2}, {10, 5}, {true, false}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT, RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<int64_t>({10, std::nullopt}),
      record->record()->childAt(1));
  capturePtr->clear();

  // Batch 2: key 2 finally passes the filter: null -> 5 emits -U null next to
  // +U 5.
  aggregator->addInput(maskedRecord({2}, {5}, {true}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({2, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<int64_t>({std::nullopt, 5}),
      record->record()->childAt(1));
  capturePtr->clear();

  // Batch 3: every row masked (false or null f): key 1 is unchanged, no
  // output.
  aggregator->addInput(maskedRecord({1, 1}, {7, 9}, {false, std::nullopt}));
  EXPECT_TRUE(capturePtr->records().empty());
}

// The key pair 391/32728 collides in the key table's hash; both keys in one
// batch must stay isolated, whatever their order in the batch.
TEST_F(GroupAggregatorTest, testCollidingKeysStayIsolated) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/false),
      std::move(capture));

  // Batch 1: two colliding keys, +I in first-occurrence order.
  aggregator->addInput(appendOnlyRecord({391, 32728}, {3, 4}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT, RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({391, 32728}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({3, 4}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 2: same keys in reversed order with asymmetric deltas; any cross
  // contamination between the colliding rows would swap or merge the sums.
  aggregator->addInput(appendOnlyRecord({32728, 391}, {10, 200}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_AFTER, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({32728, 391}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({14, 203}), record->record()->childAt(1));
}

// A null grouping key forms its own group and shares batches with non-null
// keys: both accumulate independently, and the null key round-trips through
// the key table into the emitted key column.
TEST_F(GroupAggregatorTest, testNullKeyAggregatesIndependently) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true),
      std::move(capture));

  // Batch 1: the null key and key 1 both emit +I in first-occurrence order.
  aggregator->addInput(std::make_shared<StreamRecord>(
      "group-agg",
      makeRowVector(
          {"k", "c"},
          {makeNullableFlatVector<int64_t>({std::nullopt, 1}),
           bigints({10, 5})})));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT, RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<int64_t>({std::nullopt, 1}),
      record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({10, 5}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 2: both keys change; the null key keeps its own sum (10 -> 110)
  // and its -U/+U pair carries the null key through the key column.
  aggregator->addInput(std::make_shared<StreamRecord>(
      "group-agg",
      makeRowVector(
          {"k", "c"},
          {makeNullableFlatVector<int64_t>({1, std::nullopt}),
           bigints({2, 100})})));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(
      record,
      {RowKind::UPDATE_BEFORE,
       RowKind::UPDATE_AFTER,
       RowKind::UPDATE_BEFORE,
       RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<int64_t>({1, 1, std::nullopt, std::nullopt}),
      record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({5, 7, 10, 110}), record->record()->childAt(1));
}

// A composite (BIGINT, VARCHAR) key: groups are keyed by both columns - (1,a)
// and (2,a) stay distinct despite sharing k2 - and every emitted row carries
// both key columns, VARCHAR keys included.
TEST_F(GroupAggregatorTest, testCompositeKeyAggregates) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k1", "k2", "s"}, {BIGINT(), VARCHAR(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/false,
          ROW({"k1", "k2", "c"}, {BIGINT(), VARCHAR(), BIGINT()}),
          {0, 1},
          {BIGINT(), VARCHAR()}),
      std::move(capture));

  // Batch 1: three distinct keys sharing k2 values, +I in first-occurrence
  // order.
  aggregator->addInput(
      compositeKeyRecord({1, 1, 2}, {"a", "b", "a"}, {10, 5, 7}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 1, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      makeFlatVector<std::string>({"a", "b", "a"}),
      record->record()->childAt(1));
  facebook::velox::test::assertEqualVectors(
      bigints({10, 5, 7}), record->record()->childAt(2));
  capturePtr->clear();

  // Batch 2: (2,a) grows 7 -> 8 and emits -U/+U with both key columns; (1,a)
  // is unchanged (c=0) and stays silent.
  aggregator->addInput(compositeKeyRecord({2, 1}, {"a", "a"}, {1, 0}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({2, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      makeFlatVector<std::string>({"a", "a"}), record->record()->childAt(1));
  facebook::velox::test::assertEqualVectors(
      bigints({7, 8}), record->record()->childAt(2));
}

// sum(<constant 10>) never reads an input column: every accumulated row adds
// 10, so the sums track per-key row counts (3 rows -> 30, 1 row -> 10).
TEST_F(GroupAggregatorTest, testConstantArgumentAggregate) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfConstant()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true),
      std::move(capture));

  // Batch 1: key 1 sees three rows, key 2 one: +I (1, 30), +I (2, 10). The c
  // values are irrelevant to the aggregate.
  aggregator->addInput(appendOnlyRecord({1, 1, 1, 2}, {1, 2, 3, 4}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT, RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({30, 10}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 2: key 1 adds one more row: 30 -> 40 emits -U/+U.
  aggregator->addInput(appendOnlyRecord({1}, {9}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({30, 40}), record->record()->childAt(1));
}

// A handler output column whose type disagrees with what the aggregate's
// signature produces fails at initialization with the function and both
// types named, instead of crashing later at value extraction. The baseline
// avg(BIGINT) produces DOUBLE; declaring a BIGINT column mismatches.
TEST_F(GroupAggregatorTest, testMismatchedResultTypeFailsLoud) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  VELOX_ASSERT_THROW(
      makeAggregator(
          makeNode(
              {avgOfC()},
              ROW({"k", "a"}, {BIGINT(), BIGINT()}),
              /*generateUpdateBefore=*/true),
          std::move(capture)),
      "avg(BIGINT) produces DOUBLE, but the declared output column carries "
      "BIGINT");
}

// A mask column whose input type is not BOOLEAN fails at initialization,
// naming the column.
TEST_F(GroupAggregatorTest, testNonBooleanMaskFailsLoud) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  VELOX_ASSERT_THROW(
      makeAggregator(
          makeNode(
              {sumOfCMasked()},
              ROW({"k", "s"}, {BIGINT(), BIGINT()}),
              /*generateUpdateBefore=*/true,
              /*needRetraction=*/false,
              ROW({"k", "c", "f"}, {BIGINT(), BIGINT(), BIGINT()})),
          std::move(capture)),
      "Aggregation mask column 'f' must be BOOLEAN");
}

// A retracting row on a plan that does not ask for retraction is a planner
// contract violation and fails loud; an accumulate row (+U) accumulates
// like +I, as Flink's isAccumulateMsg split does.
TEST_F(GroupAggregatorTest, testRetractRowOnInsertOnlyPlanFailsLoud) {
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true),
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));

  VELOX_ASSERT_THROW(
      aggregator->addInput(
          recordWithKinds({1}, {10}, {RowKind::UPDATE_BEFORE})),
      "non-INSERT row");
  VELOX_ASSERT_THROW(
      aggregator->addInput(recordWithKinds({1}, {10}, {RowKind::DELETE})),
      "non-INSERT row");
}

// A retraction plan probes every aggregate for RetractableAggregate at
// initialization: sum does not implement it, so the plan fails loud before
// the first input.
TEST_F(GroupAggregatorTest, testNeedRetractionPlanFailsLoud) {
  auto node = makeNode(
      {sumOfC()},
      ROW({"k", "s"}, {BIGINT(), BIGINT()}),
      /*generateUpdateBefore=*/true,
      /*needRetraction=*/true);
  auto op = std::make_unique<EmptyOperator>(0, driverCtx_.get(), node);
  auto aggregator = std::make_unique<GroupAggregator>(
      std::move(op), std::vector<StatefulOperatorPtr>{}, node);
  VELOX_ASSERT_THROW(aggregator->initialize(), "does not support retraction");
}

// One batch mixing row kinds for the same key, retract rows ahead of the
// accumulate row: the split applies both halves and the net effect lands
// (count 2 - 2 + 1 = 1); a batch whose halves cancel emits nothing. The plan
// carries no count(*), so a hidden count column carries the record count.
TEST_F(GroupAggregatorTest, testMixedRowKindBatchRetractsAndAccumulates) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countOfC()},
          ROW({"k", "cnt"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/true),
      std::move(capture));

  // Batch 1: two rows accumulate: +I (1, 2).
  aggregator->addInput(appendOnlyRecord({1, 1}, {10, 20}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({2}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 2: two -U rows ahead of one +U row in one batch: order inside the
  // batch does not matter, the update pair carries 2 -> 1.
  aggregator->addInput(recordWithKinds(
      {1, 1, 1},
      {20, 10, 5},
      {RowKind::UPDATE_BEFORE, RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({2, 1}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 3: one accumulate and one retract cancel: count returns to its
  // batch-before value, no output.
  aggregator->addInput(
      recordWithKinds({1, 1}, {5, 5}, {RowKind::INSERT, RowKind::DELETE}));
  EXPECT_TRUE(capturePtr->records().empty());
}

// A full retraction takes the record count to zero: -D carrying the
// batch-before values, then the key's state is cleared - the reborn key
// starts from zero (1, not 3) and emits +I again.
TEST_F(GroupAggregatorTest, testRetractToZeroEmitsDeleteAndClearsState) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countOfC()},
          ROW({"k", "cnt"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/true),
      std::move(capture));

  // Batch 1: +I (1, 2).
  aggregator->addInput(appendOnlyRecord({1, 1}, {10, 20}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  capturePtr->clear();

  // Batch 2: both rows retract: the record count hits zero -> -D (1, 2).
  aggregator->addInput(
      recordWithKinds({1, 1}, {10, 20}, {RowKind::DELETE, RowKind::DELETE}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::DELETE});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({2}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 3: reborn on cleared state: +I (1, 1) - 1, not 2 + 1.
  aggregator->addInput(appendOnlyRecord({1}, {7}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(1));
}

// A key whose first-ever batch only retracts is dropped, as Flink drops a
// first message that retracts: no emission, and the next batch starts from
// clean state (+I 1, not an update from a negative residue).
TEST_F(GroupAggregatorTest, testNewKeyPureRetractDroppedSilently) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countOfC()},
          ROW({"k", "cnt"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/true),
      std::move(capture));

  // Batch 1: first sight of the key, retract rows only: dropped.
  aggregator->addInput(
      recordWithKinds({1, 1}, {10, 20}, {RowKind::DELETE, RowKind::DELETE}));
  EXPECT_TRUE(capturePtr->records().empty());

  // Batch 2: clean state: +I (1, 1).
  aggregator->addInput(appendOnlyRecord({1}, {7}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(1));
}

// A first batch whose accumulate and retract halves cancel (record count
// back to zero, count(c) back to nothing) emits nothing and keeps no state.
TEST_F(GroupAggregatorTest, testNewKeyBatchNetZeroEmitsNothing) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countOfC()},
          ROW({"k", "cnt"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/true),
      std::move(capture));

  aggregator->addInput(
      recordWithKinds({1, 1}, {10, 10}, {RowKind::INSERT, RowKind::DELETE}));
  EXPECT_TRUE(capturePtr->records().empty());

  aggregator->addInput(appendOnlyRecord({1}, {7}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(1));
}

// Over-retracting a live key takes the record count negative: zero is the
// only death, so the key stays live and emits its update pair (count(c)
// wraps to -1). One more accumulated row then takes the record count from -1
// through zero: -D with the batch-before value (-1).
TEST_F(GroupAggregatorTest, testOverRetractedKeyKeepsNegativeCount) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countOfC()},
          ROW({"k", "cnt"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/true),
      std::move(capture));

  // Batch 1: +I (1, 2).
  aggregator->addInput(appendOnlyRecord({1, 1}, {10, 20}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  capturePtr->clear();

  // Batch 2: three retractions over a two-row key: record count -1, count(c)
  // 2 - 3 = -1, live update pair.
  aggregator->addInput(recordWithKinds(
      {1, 1, 1},
      {10, 20, 5},
      {RowKind::DELETE, RowKind::DELETE, RowKind::DELETE}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({2, -1}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 3: one row takes the record count from -1 to exactly zero: -D
  // (1, -1).
  aggregator->addInput(appendOnlyRecord({1}, {5}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::DELETE});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({-1}), record->record()->childAt(1));
}

// The plan's own qualifying count(*) column carries the record count (no
// hidden one needed): full retraction reads zero there and drives -D with
// every previous value, rebirth emits +I.
TEST_F(GroupAggregatorTest, testReusedCountStarDrivesDelete) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countStar(), countOfC()},
          ROW({"k", "cnt", "c"}, {BIGINT(), BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/true),
      std::move(capture));

  // Batch 1: +I (1, 2, 2).
  aggregator->addInput(appendOnlyRecord({1, 1}, {10, 20}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({2}), record->record()->childAt(1));
  facebook::velox::test::assertEqualVectors(
      bigints({2}), record->record()->childAt(2));
  capturePtr->clear();

  // Batch 2: count(*) 2 -> 0: -D (1, 2, 2).
  aggregator->addInput(
      recordWithKinds({1, 1}, {10, 20}, {RowKind::DELETE, RowKind::DELETE}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::DELETE});
  facebook::velox::test::assertEqualVectors(
      bigints({2}), record->record()->childAt(1));
  facebook::velox::test::assertEqualVectors(
      bigints({2}), record->record()->childAt(2));
  capturePtr->clear();

  // Batch 3: reborn: +I (1, 1, 1).
  aggregator->addInput(appendOnlyRecord({1}, {7}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(1));
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(2));
}

// The hidden record counter is the zero-argument count: it counts rows the
// null-filtering count(c) skips. A null-argument-only first batch still
// births the key (+I with cnt 0); retracting that null row later drives -D
// while count(c) itself has nothing to subtract.
TEST_F(GroupAggregatorTest, testHiddenCountCountsRowsWhileCountSkipsNulls) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {countOfC()},
          ROW({"k", "cnt"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/true),
      std::move(capture));

  // Batch 1: one row with a null argument: count(c) skips it (0), the row
  // counter counts it (1): +I (1, 0).
  aggregator->addInput(std::make_shared<StreamRecord>(
      "group-agg",
      makeRowVector(
          {"k", "c"},
          {bigints({1}), makeNullableFlatVector<int64_t>({std::nullopt})})));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  facebook::velox::test::assertEqualVectors(
      bigints({0}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 2: a non-null row: count 0 -> 1: -U (1, 0), +U (1, 1).
  aggregator->addInput(appendOnlyRecord({1}, {10}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({0, 1}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 3: retract the non-null row: count 1 -> 0, row count 2 -> 1: the
  // key stays live, update to 0.
  aggregator->addInput(recordWithKinds({1}, {10}, {RowKind::DELETE}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 0}), record->record()->childAt(1));
  capturePtr->clear();

  // Batch 4: retract the null-argument row: count(c) subtracts nothing, the
  // row counter reaches zero and kills the key: -D (1, 0).
  aggregator->addInput(std::make_shared<StreamRecord>(
      "group-agg",
      makeRowVector(
          {"k", "c"},
          {bigints({1}), makeNullableFlatVector<int64_t>({std::nullopt})}),
      makeFlatVector<int8_t>(
          std::vector<int8_t>{static_cast<int8_t>(RowKind::DELETE)})));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::DELETE});
  facebook::velox::test::assertEqualVectors(
      bigints({1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({0}), record->record()->childAt(1));
}

// Snapshot a warmed-up aggregation into a fresh operator: both the key table
// and the accumulators survive, so restored keys keep their histories.
TEST_F(GroupAggregatorTest, testSnapshotRestoreRoundTrip) {
  auto aggregatorA = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true),
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
  aggregatorA->addInput(appendOnlyRecord({1, 2}, {10, 5}));
  aggregatorA->addInput(appendOnlyRecord({1}, {7}));
  const auto bytes = aggregatorA->ownedKeyedStateBackend()->snapshotBytes();

  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregatorB = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"k", "s"}, {BIGINT(), BIGINT()}),
          /*generateUpdateBefore=*/true),
      std::move(capture));
  aggregatorB->ownedKeyedStateBackend()->restoreBytes(bytes);

  // Both keys exist with their pre-snapshot sums (1 -> 17, 2 -> 5): the
  // batch emits each key's -U row with the pre-batch value and +U row with
  // the incremented one, adjacent rows in one record.
  aggregatorB->addInput(appendOnlyRecord({1, 2}, {1, 1}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(
      record,
      {RowKind::UPDATE_BEFORE,
       RowKind::UPDATE_AFTER,
       RowKind::UPDATE_BEFORE,
       RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 1, 2, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({17, 18, 5, 6}), record->record()->childAt(1));
}

// Snapshot round-trip for avg(BIGINT): the accumulator serializes through the
// row(double, bigint) intermediate and keeps its sum and count, so the
// restored average continues (21/2 -> 10.5, then 22/3 -> 7.333...).
TEST_F(GroupAggregatorTest, testAvgSnapshotRestoreRoundTrip) {
  auto aggregatorA = makeAggregator(
      makeNode(
          {avgOfC()},
          ROW({"k", "a"}, {BIGINT(), DOUBLE()}),
          /*generateUpdateBefore=*/true),
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId()));
  aggregatorA->addInput(appendOnlyRecord({1}, {10}));
  aggregatorA->addInput(appendOnlyRecord({1}, {11}));
  const auto bytes = aggregatorA->ownedKeyedStateBackend()->snapshotBytes();

  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregatorB = makeAggregator(
      makeNode(
          {avgOfC()},
          ROW({"k", "a"}, {BIGINT(), DOUBLE()}),
          /*generateUpdateBefore=*/true),
      std::move(capture));
  aggregatorB->ownedKeyedStateBackend()->restoreBytes(bytes);

  aggregatorB->addInput(appendOnlyRecord({1}, {1}));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 1}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      makeFlatVector<double>(std::vector<double>{10.5, 22.0 / 3}),
      record->record()->childAt(1));
}

// Recovery through the task checkpoint path: StatefulTask::snapshotState
// collects the operator-owned backend's bytes through the operator-chain
// walk, and StatefulTask::initializeState routes them back, so a new task
// deployed over the same plan resumes from the checkpointed accumulators
// instead of starting from empty ones.
TEST_F(GroupAggregatorTest, testSnapshotRestoreThroughTaskCheckpoint) {
  // One aggregation node shared by both tasks, as a recovery redeploys the
  // same plan: the walk frames the record with the node's id, and the
  // restore walk routes it to the operator carrying the same id.
  auto aggregationNode = makeNode(
      {sumOfC()},
      ROW({"k", "s"}, {BIGINT(), BIGINT()}),
      /*generateUpdateBefore=*/true);
  const auto params = std::make_shared<const KeyedStateBackendParameters>(
      StateBackendType::HEAP, "job", "group-agg");
  auto makeTask = [&](const std::string& taskId,
                      const std::vector<RowVectorPtr>& input) {
    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<StatefulPlanNode>(
        std::make_shared<core::ValuesNode>(
            core::PlanNodeId{taskId + "-values"}, input),
        std::vector<core::PlanNodePtr>{std::make_shared<StatefulPlanNode>(
            aggregationNode, std::vector<core::PlanNodePtr>{})});
    auto task = StatefulTask::create(
        taskId,
        std::move(planFragment),
        core::QueryCtx::create(executor_.get()));
    task->init();
    return task;
  };

  // Task A: two batches warm the aggregation up (1 -> 17, 2 -> 5).
  auto taskA = makeTask(
      "group-agg-checkpoint-a",
      {makeRowVector({"k", "c"}, {bigints({1, 2}), bigints({10, 5})}),
       makeRowVector({"k", "c"}, {bigints({1}), bigints({7})})});
  taskA->initializeState(params);
  int32_t retCode = 0;
  // Drain the two changelog records before finishing the task; the walk
  // needs the operator chain alive to snapshot it.
  ASSERT_NE(nullptr, taskA->next(retCode));
  ASSERT_NE(nullptr, taskA->next(retCode));
  const auto records = taskA->snapshotState(1);
  ASSERT_EQ(1, records.size());
  // The drained task finishes with its outputs consumed.
  ASSERT_EQ(nullptr, taskA->next(retCode));
  ASSERT_EQ(1, retCode);

  // Task B: initialized from A's checkpoint records, then fed one more row
  // per key.
  auto taskB = makeTask(
      "group-agg-restore-b",
      {makeRowVector({"k", "c"}, {bigints({1, 2}), bigints({1, 1})})});
  taskB->initializeState(params, records);
  auto output = taskB->next(retCode);
  ASSERT_NE(nullptr, output);
  auto record = std::static_pointer_cast<StreamRecord>(output);
  ASSERT_TRUE(record->isRecord());
  // The restored groups continue from their pre-snapshot accumulators:
  // 1 -> 17 -> 18 and 2 -> 5 -> 6, each key's -U row ahead of its +U row.
  assertRowKinds(
      record,
      {RowKind::UPDATE_BEFORE,
       RowKind::UPDATE_AFTER,
       RowKind::UPDATE_BEFORE,
       RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({1, 1, 2, 2}), record->record()->childAt(0));
  facebook::velox::test::assertEqualVectors(
      bigints({17, 18, 5, 6}), record->record()->childAt(1));
  ASSERT_EQ(nullptr, taskB->next(retCode));
  ASSERT_EQ(1, retCode);
}

// Global aggregation without a key: an empty key set declares global state
// (Flink's keyless aggregation), every input row maps to the backend's one
// key row, and the changelog carries the aggregate columns only.
TEST_F(GroupAggregatorTest, testGlobalAggregateWithoutKeys) {
  auto capture =
      std::make_unique<CaptureTarget>(driverCtx_.get(), nextNodeId());
  auto* capturePtr = capture.get();
  auto aggregator = makeAggregator(
      makeNode(
          {sumOfC()},
          ROW({"s"}, {BIGINT()}),
          /*generateUpdateBefore=*/true,
          /*needRetraction=*/false,
          ROW({"c"}, {BIGINT()}),
          /*keyChannels=*/{},
          /*keyTypes=*/{}),
      std::move(capture));

  // Batch 1: +I 15 (10 + 5 across the whole input) - the global key is new
  // on its first sight, like any other key.
  aggregator->addInput(std::make_shared<StreamRecord>(
      "group-agg", makeRowVector({"c"}, {bigints({10, 5})})));
  ASSERT_EQ(capturePtr->records().size(), 1);
  auto record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::INSERT});
  ASSERT_EQ(1, record->record()->size());
  ASSERT_EQ(1, record->record()->childrenSize());
  facebook::velox::test::assertEqualVectors(
      bigints({15}), record->record()->childAt(0));
  capturePtr->clear();

  // Batch 2: -U 15, +U 16 adjacent for the one global key.
  aggregator->addInput(std::make_shared<StreamRecord>(
      "group-agg", makeRowVector({"c"}, {bigints({1})})));
  ASSERT_EQ(capturePtr->records().size(), 1);
  record = recordAt(*capturePtr, 0);
  assertRowKinds(record, {RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER});
  facebook::velox::test::assertEqualVectors(
      bigints({15, 16}), record->record()->childAt(0));
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
