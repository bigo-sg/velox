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
#include "velox/experimental/stateful/StatefulCalcOperator.h"

#include <thread>

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/functions/flinksql/RecordClock.h"
#include "velox/functions/flinksql/TemporalFunctions.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::stateful::test {
namespace {

// Same spy shape as StatefulCalcOperatorTest: hands out pre-built batches
// from getOutput and remembers the last input it received.
class SpyOperator : public exec::Operator {
 public:
  SpyOperator(
      exec::DriverCtx* driverCtx,
      std::vector<RowVectorPtr> outputs,
      const RowTypePtr& inputType,
      std::string planNodeId = "clock_spy")
      : Operator(driverCtx, inputType, 0, std::move(planNodeId), "Spy"),
        outputs_(std::move(outputs)) {}

  bool needsInput() const override {
    return nextOutput_ < outputs_.size();
  }

  void addInput(RowVectorPtr input) override {
    lastInput_ = std::move(input);
  }

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
  RowVectorPtr lastInput_;
};

// Terminal target that samples the record clock at the moment its addInput
// runs — i.e. after pushOutput refreshed it and before any downstream
// evaluation would read it.
class SamplingCaptureTarget : public StatefulOperator {
 public:
  explicit SamplingCaptureTarget(exec::DriverCtx* driverCtx)
      : StatefulOperator(
            std::make_unique<SpyOperator>(
                driverCtx,
                std::vector<RowVectorPtr>{},
                ROW({"c"}, {BIGINT()}),
                "capture_spy"),
            {}) {}

  void addInput(StreamElementPtr input) override {
    sampled_ = functions::flinksql::RecordClock::current();
    lastInput_ = std::move(input);
  }

  void advance() override {}

  Timestamp sampled() const {
    return sampled_;
  }

  StreamRecord* lastRecord() const {
    if (!lastInput_ || !lastInput_->isRecord()) {
      return nullptr;
    }
    return std::static_pointer_cast<StreamRecord>(lastInput_).get();
  }

 private:
  Timestamp sampled_{};
  StreamElementPtr lastInput_;
};

class RecordClockRefreshTest : public exec::test::OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    parse::registerTypeResolver();
    // The flinksql temporal functions are registered for W4's real
    // projection evaluation; W1-W3 only sample the clock and stay
    // registration-independent.
    functions::flinksql::registerFunctions("");
  }

  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{plainBatch()});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "RecordClockRefreshTest_task",
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
    return makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4})});
  }

  // Drops any pin left by a test before the fixture is torn down: the
  // thread_local state must not leak into other test binaries' threads.
  void TearDownPin() {
    functions::flinksql::RecordClock::reset();
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

// W1: a stale pin (as if the thread last crossed an emission boundary long
// ago) must not survive the next emission: advance() drives
// getOutput -> pushOutput -> target addInput, and the clock the target
// observes is re-pinned at that boundary, strictly past the stale value.
TEST_F(RecordClockRefreshTest, advanceEmissionRepinsStaleClock) {
  functions::flinksql::RecordClock::reset();
  functions::flinksql::RecordClock::refresh();
  const auto stalePinned = functions::flinksql::RecordClock::current();
  std::this_thread::sleep_for(std::chrono::milliseconds(3));

  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{plainBatch()},
      ROW({"c"}, {BIGINT()}));

  auto capture = std::make_unique<SamplingCaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();

  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));
  StatefulOperator emitter(std::move(spy), std::move(targets));

  emitter.advance();

  const auto sampled = capturePtr->sampled();
  EXPECT_GT(sampled.toMillis(), stalePinned.toMillis());
  // Sanity: the re-pinned value is a plausible wall-clock reading (around
  // the test's own now, not a wrapped or zeroed clock).
  const auto nowMillis = Timestamp::now().toMillis();
  EXPECT_LE(sampled.toMillis(), nowMillis + 1'000);
  EXPECT_GE(sampled.toMillis(), nowMillis - 60'000);

  TearDownPin();
}

// W2: the finish() drain emission path goes through the same pushOutput
// funnel — a batch that only leaves the operator during finish() is still
// preceded by a clock refresh.
TEST_F(RecordClockRefreshTest, finishDrainEmissionRepinsClock) {
  functions::flinksql::RecordClock::reset();
  functions::flinksql::RecordClock::refresh();
  const auto pinned = functions::flinksql::RecordClock::current();
  std::this_thread::sleep_for(std::chrono::milliseconds(3));

  // Two batches: advance() emits the first, finish() drains the second.
  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{plainBatch(), plainBatch()},
      ROW({"c"}, {BIGINT()}));

  auto capture = std::make_unique<SamplingCaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();

  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));
  StatefulCalcOperator calcOp(std::move(spy), std::move(targets));

  calcOp.advance();
  const auto advanceSampled = capturePtr->sampled();

  calcOp.finish();
  const auto finishSampled = capturePtr->sampled();

  EXPECT_GT(advanceSampled.toMillis(), pinned.toMillis());
  EXPECT_GT(finishSampled.toMillis(), pinned.toMillis());
  // The drain emission re-pins as well (at or after the advance emission).
  EXPECT_GE(finishSampled.toMillis(), advanceSampled.toMillis());

  TearDownPin();
}

// W3: the refresh is thread-local — a background thread crossing an
// emission boundary (the processing-time timer shape) re-pins only its own
// clock; the main thread's pin is untouched.
TEST_F(RecordClockRefreshTest, refreshIsThreadLocal) {
  functions::flinksql::RecordClock::reset();
  functions::flinksql::RecordClock::refresh();
  const auto mainPinned = functions::flinksql::RecordClock::current();
  std::this_thread::sleep_for(std::chrono::milliseconds(3));

  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{plainBatch()},
      ROW({"c"}, {BIGINT()}));

  auto capture = std::make_unique<SamplingCaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();

  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(capture));
  StatefulOperator emitter(std::move(spy), std::move(targets));

  std::thread worker([&emitter]() { emitter.advance(); });
  worker.join();

  const auto workerSampled = capturePtr->sampled();
  // The worker thread's emission refreshed its own clock past the stale
  // main-thread pin...
  EXPECT_GT(workerSampled.toMillis(), mainPinned.toMillis());
  // ...and the main thread's pin survived the worker's emission untouched.
  EXPECT_EQ(functions::flinksql::RecordClock::current(), mainPinned);

  TearDownPin();
}

// W4: end to end through a real stateful chain — a spy emission drives a
// StatefulCalcOperator projecting all four temporal functions, and the
// captured record shows one shared instant: ct == now, localtimestamp ==
// ct (UTC session), and localtime == the floor-mod milliseconds-of-day of
// localtimestamp.
TEST_F(RecordClockRefreshTest, emissionDrivenTemporalProjectionIsConsistent) {
  functions::flinksql::RecordClock::reset();

  core::PlanNodePtr valuesSource = std::make_shared<core::ValuesNode>(
      core::PlanNodeId{"vals"},
      std::vector<RowVectorPtr>{
          makeRowVector({"c"}, {makeFlatVector<int64_t>({1, 2, 3, 4})})});

  auto noArg = [](const std::string& name, const TypePtr& type) {
    return std::make_shared<core::CallTypedExpr>(
        type, std::vector<core::TypedExprPtr>{}, name);
  };
  auto project = std::make_shared<core::ProjectNode>(
      core::PlanNodeId{"proj"},
      std::vector<std::string>{"ct", "nw", "lt", "ltime"},
      std::vector<core::TypedExprPtr>{
          noArg("current_timestamp", TIMESTAMP()),
          noArg("now", TIMESTAMP()),
          noArg("localtimestamp", TIMESTAMP()),
          noArg("localtime", INTEGER())},
      valuesSource);

  auto capture = std::make_unique<SamplingCaptureTarget>(driverCtx_.get());
  auto* capturePtr = capture.get();

  std::vector<StatefulOperatorPtr> calcTargets;
  calcTargets.push_back(std::move(capture));
  auto calcOp = std::make_unique<StatefulCalcOperator>(
      0,
      driverCtx_.get(),
      nullptr,
      project,
      std::move(calcTargets));

  auto spy = std::make_unique<SpyOperator>(
      driverCtx_.get(),
      std::vector<RowVectorPtr>{plainBatch()},
      ROW({"c"}, {BIGINT()}));

  std::vector<StatefulOperatorPtr> targets;
  targets.push_back(std::move(calcOp));
  StatefulOperator emitter(std::move(spy), std::move(targets));

  // Initializes the chain: the spy and, through the target recursion, the
  // StatefulCalcOperator's real FilterProject.
  emitter.initialize();
  emitter.advance();

  auto* record = capturePtr->lastRecord();
  ASSERT_NE(record, nullptr);
  ASSERT_NE(record->record(), nullptr);
  const auto& output = record->record();
  ASSERT_EQ(output->type()->size(), 4);

  auto* ct = output->childAt(0)->asFlatVector<Timestamp>();
  auto* nw = output->childAt(1)->asFlatVector<Timestamp>();
  auto* lt = output->childAt(2)->asFlatVector<Timestamp>();
  auto* ltime = output->childAt(3)->asFlatVector<int32_t>();
  ASSERT_EQ(ct->size(), 4);
  for (auto i = 0; i < 4; ++i) {
    // One shared instant per row across all four functions.
    EXPECT_EQ(ct->valueAt(i), nw->valueAt(i)) << "row: " << i;
    EXPECT_EQ(lt->valueAt(i), ct->valueAt(i)) << "row: " << i;
    // LOCALTIME is the floor-mod milliseconds-of-day of LOCALTIMESTAMP.
    const auto v = ltime->valueAt(i);
    EXPECT_GE(v, 0) << "row: " << i;
    EXPECT_LT(v, 86'400'000) << "row: " << i;
    EXPECT_EQ(
        static_cast<int64_t>(v),
        functions::flinksql::millisOfDay(lt->valueAt(i).toMillis()))
        << "row: " << i;
  }

  TearDownPin();
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}
