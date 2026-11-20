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
#include "velox/experimental/stateful/rank/RankOperator.h"

#include <folly/Conv.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <map>
#include <optional>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/KeyedStateSpec.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StatefulTask.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/HashMapStateBackend.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"

namespace facebook::velox::stateful::test {
namespace {

// Minimal exec::Operator stub for the collecting downstream operator
// (implementation contract: downstream operators are collecting operators).
class CollectingExecOperator : public exec::Operator {
 public:
  explicit CollectingExecOperator(
      exec::DriverCtx* driverCtx,
      const std::string& planNodeId)
      : Operator(
            driverCtx,
            ROW({"c"}, {BIGINT()}),
            0,
            planNodeId,
            "CollectingExecOperator") {}

  bool needsInput() const override {
    return true;
  }

  void addInput(RowVectorPtr /*input*/) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }
};

class CollectingOperator : public StatefulOperator {
 public:
  explicit CollectingOperator(
      exec::DriverCtx* driverCtx,
      const std::string& planNodeId)
      : StatefulOperator(
            std::make_unique<CollectingExecOperator>(driverCtx, planNodeId),
            std::vector<StatefulOperatorPtr>()) {}

  void addInput(StreamElementPtr input) override {
    collected_.push_back(std::static_pointer_cast<StreamRecord>(input));
  }

  void advance() override {}

  const std::vector<StreamRecordPtr>& collected() const {
    return collected_;
  }

 private:
  std::vector<StreamRecordPtr> collected_;
};

// One decoded changelog row of a test output: the Flink row kind, the k and
// v columns and the rank column when the plan outputs it. The v column type
// V is int64_t for the BIGINT/short-decimal fixtures, int32_t for DATE and
// int128_t for long decimal.
template <typename V>
struct TypedOutRow {
  int8_t kind = 0;
  int64_t k = 0;
  V v{};
  bool vNull = false;
  std::optional<int64_t> rank;
};

using OutRow = TypedOutRow<int64_t>;

template <typename V>
using TypedOutRows = std::vector<TypedOutRow<V>>;

using OutRows = TypedOutRows<int64_t>;

struct RankTestSpec {
  int64_t rankStart = 1;
  int64_t rankEnd = 2;
  std::vector<column_index_t> keyChannels = {0};
  std::vector<column_index_t> sortChannels = {1};
  std::vector<bool> ascending = {true};
  std::vector<bool> nullsFirst = {true};
  bool outputRankNumber = true;
  bool generateUpdateBefore = true;
  bool firstN = false;
  bool global = false;
  // The TopN cache bound crossing the plan (design S3.3): the
  // table.exec.rank.topn-cache-size row equivalent, default 10000
  // (ExecutionConfigOptions.java:501-505).
  int64_t cacheSize = 10000;
  // Null for the default (k, v) BIGINT fixture; the DECIMAL/DATE sort-key
  // tests supply their own (k, typed-column) row types.
  RowTypePtr inputTypeOverride;
};

} // namespace

// At named scope (not the anonymous namespace) so the friend declaration in
// RankOperator.h names this exact class.
class RankOperatorTest : public exec::test::OperatorTestBase {
 protected:
  static constexpr uint32_t kMaxParallelism = 128;

  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"},
        std::vector<RowVectorPtr>{
            makeRowVector({makeFlatVector<int64_t>({1})})});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = StatefulTask::create(
        "RankOperatorTest_task",
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

  RowTypePtr inputType() const {
    return ROW({"k", "v"}, {BIGINT(), BIGINT()});
  }

  RowTypePtr inputTypeOf(const RankTestSpec& spec) const {
    return spec.inputTypeOverride ? spec.inputTypeOverride : inputType();
  }

  RowTypePtr outputType(bool outputRankNumber) const {
    return outputTypeOf(inputType(), outputRankNumber);
  }

  static RowTypePtr outputTypeOf(
      const RowTypePtr& inputType,
      bool outputRankNumber) {
    if (outputRankNumber) {
      auto names = inputType->names();
      auto types = inputType->children();
      names.emplace_back("rn");
      types.emplace_back(BIGINT());
      return ROW(std::move(names), std::move(types));
    }
    return inputType;
  }

  std::shared_ptr<const RankNode> makeNode(
      const RankTestSpec& spec,
      const std::string& planNodeId) const {
    const auto inType = inputTypeOf(spec);
    std::vector<column_index_t> keyChannels;
    std::vector<TypePtr> keyTypes;
    if (spec.global) {
      // Sentinel key channel: one past the input schema (design D3).
      keyChannels = {static_cast<column_index_t>(inType->size())};
      keyTypes = {BIGINT()};
    } else {
      for (const auto channel : spec.keyChannels) {
        keyChannels.push_back(channel);
        keyTypes.push_back(inType->childAt(channel));
      }
    }
    return std::make_shared<const RankNode>(
        core::PlanNodeId{planNodeId},
        spec.rankStart,
        spec.rankEnd,
        spec.sortChannels,
        spec.ascending,
        spec.nullsFirst,
        std::make_shared<const KeyedStateSpec>(
            std::move(keyChannels), std::move(keyTypes)),
        spec.outputRankNumber,
        spec.generateUpdateBefore,
        spec.firstN,
        spec.cacheSize,
        inType,
        outputTypeOf(inType, spec.outputRankNumber));
  }

  // Builds a full rank chain: RankOperator with one collecting downstream
  // operator, initialized the production way (initialize then
  // initializeStateBackend, per the implementation contract).
  struct RankChain {
    std::unique_ptr<RankOperator> rank;
    CollectingOperator* collector;
    std::unique_ptr<HashMapStateBackend> backend;
  };

  RankChain makeChain(
      const RankTestSpec& spec,
      uint32_t startKeyGroup = 0,
      uint32_t numKeyGroups = kMaxParallelism) {
    // Unique plan node id per chain: several chains share one task in the
    // round-trip tests, and operators name their memory pools after the plan
    // node id + operator id.
    static std::atomic<int> nextChain{0};
    const auto planNodeId = fmt::format("rank-{}", nextChain.fetch_add(1));
    auto node = makeNode(spec, planNodeId);
    auto backend = std::make_unique<HashMapStateBackend>(
        std::make_shared<const KeyedStateBackendParameters>(
            StateBackendType::HEAP,
            "job",
            "rank",
            kMaxParallelism,
            startKeyGroup,
            numKeyGroups));
    auto checkpointOperator =
        std::make_unique<RankCheckpointOperator>(0, driverCtx_.get(), node);
    auto collector = std::make_unique<CollectingOperator>(
        driverCtx_.get(), planNodeId + "-collector");
    auto* collectorPtr = collector.get();
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(collector));
    auto rank = std::make_unique<RankOperator>(
        std::move(checkpointOperator), std::move(targets), node);
    rank->initialize();
    rank->initializeStateBackend(backend.get());
    return RankChain{std::move(rank), collectorPtr, std::move(backend)};
  }

  RowVectorPtr batch(const std::vector<std::pair<int64_t, int64_t>>& rows) {
    std::vector<int64_t> k;
    std::vector<int64_t> v;
    for (const auto& [key, value] : rows) {
      k.push_back(key);
      v.push_back(value);
    }
    return makeRowVector(
        {makeFlatVector<int64_t>(k), makeFlatVector<int64_t>(v)});
  }

  RowVectorPtr batchWithNullV(const std::vector<std::optional<int64_t>>& rows) {
    std::vector<int64_t> k;
    std::vector<std::optional<int64_t>> v;
    for (size_t i = 0; i < rows.size(); ++i) {
      k.push_back(1);
      v.push_back(rows[i]);
    }
    return makeRowVector(
        {makeFlatVector<int64_t>(k), makeNullableFlatVector<int64_t>(v)});
  }

  SimpleVectorPtr<int8_t> kinds(const std::vector<int8_t>& values) {
    auto buffer = AlignedBuffer::allocate<int8_t>(values.size(), pool());
    auto* raw = buffer->asMutable<int8_t>();
    for (size_t i = 0; i < values.size(); ++i) {
      raw[i] = values[i];
    }
    return std::make_shared<FlatVector<int8_t>>(
        pool(),
        TINYINT(),
        nullptr,
        values.size(),
        std::move(buffer),
        std::vector<BufferPtr>{});
  }

  void feed(RankOperator& rank, const RowVectorPtr& input) {
    rank.addInput(std::make_shared<StreamRecord>("rank", input));
  }

  void feedWithKinds(
      RankOperator& rank,
      const RowVectorPtr& input,
      const std::vector<int8_t>& kindValues) {
    rank.addInput(
        std::make_shared<StreamRecord>("rank", input, kinds(kindValues)));
  }

  // Decodes records [fromRecord, end) of the collector; the collector keeps
  // its full history, so callers drain in slices as batches are fed. The v
  // column decodes as V (int64_t BIGINT/short-decimal, int32_t DATE,
  // int128_t long decimal).
  template <typename V>
  TypedOutRows<V> drainTyped(
      const CollectingOperator& collector,
      bool withRank,
      size_t fromRecord = 0) {
    TypedOutRows<V> rows;
    const auto& collected = collector.collected();
    for (size_t r = fromRecord; r < collected.size(); ++r) {
      const auto& record = collected[r];
      if (record->rowKind() == nullptr) {
        ADD_FAILURE() << "rank output record carries no row kinds";
        continue;
      }
      const auto& vector = record->record();
      auto kVector = vector->childAt(0)->as<SimpleVector<int64_t>>();
      auto vVector = vector->childAt(1)->as<SimpleVector<V>>();
      SimpleVector<int64_t>* rankVector = nullptr;
      if (withRank) {
        rankVector = vector->childAt(2)->as<SimpleVector<int64_t>>();
      }
      for (auto i = 0; i < vector->size(); ++i) {
        TypedOutRow<V> row;
        row.kind = record->rowKind()->valueAt(i);
        row.k = kVector->valueAt(i);
        row.vNull = vVector->isNullAt(i);
        if (!row.vNull) {
          row.v = vVector->valueAt(i);
        }
        if (rankVector != nullptr) {
          row.rank = rankVector->valueAt(i);
        }
        rows.push_back(row);
      }
    }
    return rows;
  }

  OutRows drain(
      const CollectingOperator& collector,
      bool withRank,
      size_t fromRecord = 0) {
    return drainTyped<int64_t>(collector, withRank, fromRecord);
  }

  template <typename V>
  std::string formatValue(const TypedOutRow<V>& row) const {
    if (row.vNull) {
      return "null";
    }
    if constexpr (std::is_same_v<V, int128_t>) {
      return folly::to<std::string>(row.v);
    } else {
      return std::to_string(row.v);
    }
  }

  template <typename V>
  std::string outRowsToString(const TypedOutRows<V>& rows) {
    std::string out;
    for (const auto& row : rows) {
      out += fmt::format(
          "[{} k={} v={} rn={}]",
          static_cast<int>(row.kind),
          row.k,
          formatValue(row),
          row.rank.has_value() ? std::to_string(*row.rank) : std::string("-"));
    }
    return out;
  }

  template <typename V>
  void expectTypedRows(
      const TypedOutRows<V>& actual,
      const TypedOutRows<V>& expected,
      const std::string& context) {
    if (testing::Test::HasFailure()) {
      return;
    }
    EXPECT_EQ(expected.size(), actual.size())
        << context << ": " << outRowsToString(actual);
    for (size_t i = 0; i < expected.size() && i < actual.size(); ++i) {
      EXPECT_EQ(expected[i].kind, actual[i].kind)
          << context << " row " << i << ": " << outRowsToString(actual);
      EXPECT_EQ(expected[i].k, actual[i].k)
          << context << " row " << i << ": " << outRowsToString(actual);
      EXPECT_EQ(expected[i].vNull, actual[i].vNull)
          << context << " row " << i << ": " << outRowsToString(actual);
      EXPECT_EQ(expected[i].v, actual[i].v)
          << context << " row " << i << ": " << outRowsToString(actual);
      EXPECT_EQ(expected[i].rank, actual[i].rank)
          << context << " row " << i << ": " << outRowsToString(actual);
    }
  }

  void expectOutRows(
      const OutRows& actual,
      const OutRows& expected,
      const std::string& context) {
    expectTypedRows<int64_t>(actual, expected, context);
  }

  // State introspection (friend access): the number of rows stored under
  // each key of 'keys' (one fresh probe per key).
  std::map<int64_t, size_t> storedRowCounts(
      RankOperator& rank,
      const std::vector<int64_t>& keys) {
    std::map<int64_t, size_t> counts;
    for (const auto key : keys) {
      rank.keySelector_->probe(batch({{key, 0}}));
      auto rows = rank.listState_->get(rank.keySelector_->keys()[0], rank.ns_);
      counts[key] = rows.size();
    }
    return counts;
  }

  // The key group of 'key' (one fresh probe).
  uint32_t keyGroupOf(RankOperator& rank, int64_t key) {
    rank.keySelector_->probe(batch({{key, 0}}));
    return rank.keySelector_->keys()[0].keyGroup();
  }

  // The FirstN counter state of 'key' (one fresh probe): the raw stored
  // vector - empty for a key the backend has never seen, exactly one
  // element holding the counter otherwise.
  std::vector<int64_t> counterStateOf(
      RankOperator& rank,
      const std::vector<int64_t>& keys) {
    std::vector<int64_t> counters;
    for (const auto key : keys) {
      rank.keySelector_->probe(batch({{key, 0}}));
      const auto stored =
          rank.counterState_->get(rank.keySelector_->keys()[0], rank.ns_);
      counters.insert(counters.end(), stored.begin(), stored.end());
    }
    return counters;
  }

  // The stored v values of 'key' in state order (one fresh probe) - the
  // full-prefix assertion of the offset-window tests reads the actual rows.
  std::vector<int64_t> storedSortValues(
      RankOperator& rank,
      const std::vector<int64_t>& keys) {
    std::vector<int64_t> values;
    for (const auto key : keys) {
      rank.keySelector_->probe(batch({{key, 0}}));
      const auto rows =
          rank.listState_->get(rank.keySelector_->keys()[0], rank.ns_);
      for (const auto& row : rows) {
        values.push_back(rank.compactLayout_->valueAt<int64_t>(*row, 1));
      }
    }
    return values;
  }

  // The stored row byte sizes of 'keys' (one fresh probe): one entry per
  // stored row, read through the compact layout (the storage-byte budget
  // assertions of the design test matrix).
  std::map<int64_t, std::vector<size_t>> storedRowBytes(
      RankOperator& rank,
      const std::vector<int64_t>& keys) {
    std::map<int64_t, std::vector<size_t>> sizes;
    for (const auto key : keys) {
      rank.keySelector_->probe(batch({{key, 0}}));
      const auto rows =
          rank.listState_->get(rank.keySelector_->keys()[0], rank.ns_);
      for (const auto& row : rows) {
        sizes[key].push_back(row->size());
      }
    }
    return sizes;
  }

  // The TopN cache entry count (friend access): the LRU residency bound
  // assertions of the design test matrix.
  size_t cacheEntriesOf(RankOperator& rank) {
    return rank.cache_->size();
  }

  // A key whose key group falls inside [startKeyGroup, startKeyGroup +
  // numKeyGroups) - the deterministic hash makes one of the first few keys
  // land in any sizable range.
  int64_t keyInRange(
      RankOperator& rank,
      uint32_t startKeyGroup,
      uint32_t numKeyGroups) {
    for (int64_t key = 1; key < 64; ++key) {
      const uint32_t keyGroup = keyGroupOf(rank, key);
      if (keyGroup >= startKeyGroup &&
          keyGroup < startKeyGroup + numKeyGroups) {
        return key;
      }
    }
    ADD_FAILURE() << "no probe key landed in the key-group range";
    return 1;
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<StatefulTask> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

// Design matrix: appendOnly input (no rowKind column) - grouped TopN with
// the rank number, two batches, displacement chain + insert of the open
// rank (alignment #5/#9).
TEST_F(RankOperatorTest, groupedTopNWithRankNumberAppendOnly) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}}));
  OutRows expectedBatch1{
      {0, 1, 30, false, 1},
      {1, 1, 30, false, 1},
      {2, 1, 20, false, 1},
      {0, 1, 30, false, 2}};
  expectOutRows(drain(*chain.collector, true, 0), expectedBatch1, "batch1");

  feed(*chain.rank, batch({{1, 10}}));
  OutRows expectedBatch2{
      {1, 1, 20, false, 1},
      {2, 1, 10, false, 1},
      {1, 1, 30, false, 2},
      {2, 1, 20, false, 2}};
  expectOutRows(drain(*chain.collector, true, 1), expectedBatch2, "batch2");
}

// Design matrix: explicit INSERT row kinds behave exactly like appendOnly.
TEST_F(RankOperatorTest, explicitInsertKindsBehaveLikeAppendOnly) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  auto input = batch({{1, 30}, {1, 20}});
  feedWithKinds(*chain.rank, input, {0, 0});
  OutRows expected{
      {0, 1, 30, false, 1},
      {1, 1, 30, false, 1},
      {2, 1, 20, false, 1},
      {0, 1, 30, false, 2}};
  expectOutRows(drain(*chain.collector, true), expected, "insert kinds");
}

// Design matrix: mixed row kinds fail loud (insert-only rule, L3 analog).
TEST_F(RankOperatorTest, mixedRowKindsFailLoud) {
  RankTestSpec spec;
  auto chain = makeChain(spec);
  auto input = batch({{1, 30}, {1, 20}, {1, 10}});
  EXPECT_THROW(feedWithKinds(*chain.rank, input, {0, 1, 2}), VeloxUserError);
}

// Design matrix: grouped TopN with the rank number across multiple keys
// and batches with eviction (alignment #9 displacement chain).
TEST_F(RankOperatorTest, groupedTopNMultiKeyWithEviction) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {2, 50}, {1, 20}, {2, 40}}));
  feed(*chain.rank, batch({{1, 10}, {2, 60}}));

  OutRows expected{
      // batch1, row (1,30)
      {0, 1, 30, false, 1},
      // batch1, row (2,50)
      {0, 2, 50, false, 1},
      // batch1, row (1,20)
      {1, 1, 30, false, 1},
      {2, 1, 20, false, 1},
      {0, 1, 30, false, 2},
      // batch1, row (2,40)
      {1, 2, 50, false, 1},
      {2, 2, 40, false, 1},
      {0, 2, 50, false, 2},
      // batch2, row (1,10): displaces both, no +I (rank 3 out of range)
      {1, 1, 20, false, 1},
      {2, 1, 10, false, 1},
      {1, 1, 30, false, 2},
      {2, 1, 20, false, 2},
      // batch2, row (2,60): worse than the worst - rejected, no output
  };
  expectOutRows(drain(*chain.collector, true), expected, "multi key");
}

// Design matrix: grouped TopN without the rank number - eviction emits
// -D of the retired row and +I of the new row (alignment #11).
TEST_F(RankOperatorTest, groupedTopNWithoutRankNumberEvicts) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = false;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}}));
  feed(*chain.rank, batch({{1, 10}}));

  OutRows expected{
      {0, 1, 30, false, std::nullopt},
      {0, 1, 20, false, std::nullopt},
      {3, 1, 30, false, std::nullopt},
      {0, 1, 10, false, std::nullopt}};
  expectOutRows(drain(*chain.collector, false), expected, "no rank number");
}

// Design matrix: grouped Top1 - first row +I, better row -U/+U, worse or
// equal row silent (alignment #12-14, FastTop1Function).
TEST_F(RankOperatorTest, groupedTop1Changelog) {
  RankTestSpec spec;
  spec.rankStart = 1;
  spec.rankEnd = 1;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}, {1, 25}}));

  OutRows expected{
      {0, 1, 30, false, 1}, {1, 1, 30, false, 1}, {2, 1, 20, false, 1}};
  expectOutRows(drain(*chain.collector, true), expected, "top1");
}

// Design matrix: global TopN - no partition key, sentinel column injected
// by the operator (design D3), single-key behavior.
TEST_F(RankOperatorTest, globalTopNUsesSentinelKey) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  spec.global = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{10, 3}, {20, 1}, {30, 2}}));

  OutRows expected{
      {0, 10, 3, false, 1},
      {1, 10, 3, false, 1},
      {2, 20, 1, false, 1},
      {0, 10, 3, false, 2},
      {1, 10, 3, false, 2},
      {2, 30, 2, false, 2}};
  expectOutRows(drain(*chain.collector, true), expected, "global topN");
}

// Design matrix: global Top1 (shape combination).
TEST_F(RankOperatorTest, globalTop1Changelog) {
  RankTestSpec spec;
  spec.rankStart = 1;
  spec.rankEnd = 1;
  spec.outputRankNumber = false;
  spec.generateUpdateBefore = true;
  spec.global = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{10, 3}, {20, 1}, {30, 2}}));

  OutRows expected{
      {0, 10, 3, false, std::nullopt},
      {1, 10, 3, false, std::nullopt},
      {2, 20, 1, false, std::nullopt}};
  expectOutRows(drain(*chain.collector, false), expected, "global top1");
}

// Design matrix: rows with the same sort key keep arrival order and get
// consecutive ranks (alignment #21, TopNBuffer list tail insert).
TEST_F(RankOperatorTest, sameSortKeyRowsKeepArrivalOrder) {
  RankTestSpec spec;
  spec.rankEnd = 3;
  spec.outputRankNumber = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 5}, {1, 5}, {1, 5}}));

  OutRows expected{
      {0, 1, 5, false, 1}, {0, 1, 5, false, 2}, {0, 1, 5, false, 3}};
  expectOutRows(drain(*chain.collector, true), expected, "ties");
}

// Design matrix: null sort keys participate through the nullsFirst /
// nullsLast flag (alignment #20, Flink generateRowCompare null rule).
TEST_F(RankOperatorTest, nullSortKeyWithNullsFirst) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  spec.nullsFirst = {true};
  auto chain = makeChain(spec);

  feed(*chain.rank, batchWithNullV({std::nullopt, 5, std::nullopt}));

  // The third row (null) ties the first row's sort key, so it enters AFTER
  // the tie at rank 2 and displaces only the (1,5) row out of the range.
  OutRows expected{
      {0, 1, 0, true, 1},
      {0, 1, 5, false, 2},
      {1, 1, 5, false, 2},
      {2, 1, 0, true, 2}};
  expectOutRows(drain(*chain.collector, true, 0), expected, "nulls first");
}

TEST_F(RankOperatorTest, nullSortKeyWithNullsLast) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.nullsFirst = {false};
  auto chain = makeChain(spec);

  feed(*chain.rank, batchWithNullV({5, std::nullopt, 3}));

  OutRows expected{
      {0, 1, 5, false, 1},
      {0, 1, 0, true, 2},
      // (1,3): better than 5 -> displaces both; the null falls out of range
      {1, 1, 5, false, 1},
      {2, 1, 3, false, 1},
      {1, 1, 0, true, 2},
      {2, 1, 5, false, 2}};
  expectOutRows(drain(*chain.collector, true), expected, "nulls last");
}

// Design matrix: -U is gated by generateUpdateBefore (alignment #15).
TEST_F(RankOperatorTest, updateBeforeGated) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = false;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}}));

  OutRows expected{
      {0, 1, 30, false, 1}, {2, 1, 20, false, 1}, {0, 1, 30, false, 2}};
  expectOutRows(drain(*chain.collector, true), expected, "no -U");
}

// Design matrix: Top1 without the rank number still emits -U/+U, never
// -D/+I (alignment #25, design D2).
TEST_F(RankOperatorTest, top1WithoutRankNumberStillUpdates) {
  RankTestSpec spec;
  spec.rankStart = 1;
  spec.rankEnd = 1;
  spec.outputRankNumber = false;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}, {1, 25}}));

  OutRows expected{
      {0, 1, 30, false, std::nullopt},
      {1, 1, 30, false, std::nullopt},
      {2, 1, 20, false, std::nullopt}};
  expectOutRows(drain(*chain.collector, false), expected, "top1 no rank");
}

// Design matrix: snapshot/restore round trip. The checkpoint is taken after
// rows have entered but before anything was evicted ("rows that entered
// and were not evicted must survive the checkpoint"): the restored operator
// continues exactly like the uninterrupted run.
TEST_F(RankOperatorTest, snapshotRestoreRoundTripKeepsEnteredRows) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;

  auto continuous = makeChain(spec);
  feed(*continuous.rank, batch({{1, 30}, {1, 20}}));
  feed(*continuous.rank, batch({{1, 10}}));
  auto continuousOut = drain(*continuous.collector, true);

  auto interrupted = makeChain(spec);
  feed(*interrupted.rank, batch({{1, 30}, {1, 20}}));
  auto interruptedOut = drain(*interrupted.collector, true);
  auto records = interrupted.rank->snapshotRankState(1);
  ASSERT_EQ(1, records.size());

  auto restored = makeChain(spec);
  restored.rank->restoreRankState(records);
  feed(*restored.rank, batch({{1, 10}}));
  auto restoredOut = drain(*restored.collector, true);

  // The restored continuation is the trailing slice of the continuous run
  // ("rows that entered and were not evicted must survive").
  ASSERT_GE(continuousOut.size(), interruptedOut.size() + restoredOut.size());
  OutRows expectedContinuation(
      continuousOut.end() - restoredOut.size(), continuousOut.end());
  expectOutRows(restoredOut, expectedContinuation, "restored continuation");
  expectOutRows(
      interruptedOut,
      OutRows{
          {0, 1, 30, false, 1},
          {1, 1, 30, false, 1},
          {2, 1, 20, false, 1},
          {0, 1, 30, false, 2}},
      "pre-checkpoint output");
}

// Design matrix: snapshot/restore after an eviction - the trimmed state
// continues exactly like the uninterrupted run.
TEST_F(RankOperatorTest, snapshotRestoreAfterEviction) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;

  auto continuous = makeChain(spec);
  feed(*continuous.rank, batch({{1, 30}, {1, 20}}));
  feed(*continuous.rank, batch({{1, 10}}));
  feed(*continuous.rank, batch({{1, 15}}));
  auto continuousOut = drain(*continuous.collector, true);

  auto interrupted = makeChain(spec);
  feed(*interrupted.rank, batch({{1, 30}, {1, 20}}));
  feed(*interrupted.rank, batch({{1, 10}}));
  auto records = interrupted.rank->snapshotRankState(2);

  auto restored = makeChain(spec);
  restored.rank->restoreRankState(records);
  feed(*restored.rank, batch({{1, 15}}));
  auto restoredOut = drain(*restored.collector, true);

  // Both sides emitted identical history; the restored continuation is the
  // trailing slice (the third batch's changelog).
  ASSERT_GE(continuousOut.size(), restoredOut.size());
  OutRows trailing(
      continuousOut.end() - restoredOut.size(), continuousOut.end());
  expectOutRows(restoredOut, trailing, "post-eviction restore");
}

// Design matrix: the per-key state stays bounded by rankEnd after
// evictions (SPEC acceptance 7) - inspected through the restored state.
TEST_F(RankOperatorTest, stateUpperBoundPerKey) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 40}, {2, 5}, {3, 7}}));
  feed(*chain.rank, batch({{1, 30}, {1, 20}, {1, 10}, {3, 6}}));

  auto records = chain.rank->snapshotRankState(3);
  ASSERT_EQ(1, records.size());

  auto restored = makeChain(spec);
  restored.rank->restoreRankState(records);
  auto counts = storedRowCounts(*restored.rank, {1, 2, 3});
  EXPECT_EQ((std::map<int64_t, size_t>{{1, 2}, {2, 1}, {3, 2}}), counts);
  for (const auto& [key, count] : counts) {
    EXPECT_LE(count, static_cast<size_t>(spec.rankEnd)) << "key " << key;
  }
}

// Design matrix: rescale restore - a checkpoint written over the full
// key-group range restores into a narrower backend, which picks exactly its
// own key groups out of the stream. Out-of-range keys are not even readable
// on the narrow backend (that is the rescale contract: those keys belong to
// a different subtask).
TEST_F(RankOperatorTest, rescaleRestorePicksCoveredRange) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 40}, {2, 5}, {3, 7}}));
  auto records = chain.rank->snapshotRankState(4);
  ASSERT_EQ(1, records.size());

  // The record carries all keys: restoring into a full-range operator keeps
  // every key's state.
  auto full = makeChain(spec);
  full.rank->restoreRankState(records);
  auto fullCounts = storedRowCounts(*full.rank, {1, 2, 3});
  EXPECT_EQ((std::map<int64_t, size_t>{{1, 1}, {2, 1}, {3, 1}}), fullCounts);

  // The half-range backend picks its own key groups; keys outside [0, 64)
  // did not restore into it.
  auto firstHalf = makeChain(spec, 0, kMaxParallelism / 2);
  firstHalf.rank->restoreRankState(records);

  auto reference = makeChain(spec);
  for (const auto key : {1, 2, 3}) {
    const uint32_t keyGroup = keyGroupOf(*reference.rank, key);
    if (keyGroup < kMaxParallelism / 2) {
      const auto counts = storedRowCounts(*firstHalf.rank, {key});
      EXPECT_EQ(1, counts.at(key)) << "key " << key << " in covered range";
    } else {
      EXPECT_THROW(storedRowCounts(*firstHalf.rank, {key}), VeloxRuntimeError)
          << "key " << key << " outside covered range";
    }
  }
}

// Design matrix: RankNode serde round trip (fields incl. the nested
// KeyedStateSpec).
TEST_F(RankOperatorTest, rankNodeSerdeRoundTrip) {
  RankTestSpec spec;
  spec.rankEnd = 3;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = false;
  auto node = makeNode(spec, "rank-serde");

  StatefulPlanNode::registerSerDe();
  Type::registerSerDe();
  auto restored = std::dynamic_pointer_cast<const RankNode>(
      RankNode::create(node->serialize(), nullptr));
  ASSERT_NE(restored, nullptr);
  EXPECT_EQ(node->rankStart(), restored->rankStart());
  EXPECT_EQ(node->rankEnd(), restored->rankEnd());
  EXPECT_EQ(node->sortKeyChannels(), restored->sortKeyChannels());
  EXPECT_EQ(node->sortAscending(), restored->sortAscending());
  EXPECT_EQ(node->sortNullsFirst(), restored->sortNullsFirst());
  EXPECT_EQ(node->outputRankNumber(), restored->outputRankNumber());
  EXPECT_EQ(node->generateUpdateBefore(), restored->generateUpdateBefore());
  EXPECT_EQ(node->inputType()->toString(), restored->inputType()->toString());
  EXPECT_EQ(node->outputType()->toString(), restored->outputType()->toString());
  EXPECT_EQ(
      node->keyedStateSpec()->keyChannels(),
      restored->keyedStateSpec()->keyChannels());
  EXPECT_EQ(1, restored->keyedStateSpec()->keyTypes().size());
}

// The global shape's serde round trip (sentinel key channel one past the
// schema).
TEST_F(RankOperatorTest, globalRankNodeSerdeRoundTrip) {
  RankTestSpec spec;
  spec.global = true;
  spec.rankEnd = 5;
  auto node = makeNode(spec, "rank-serde");

  StatefulPlanNode::registerSerDe();
  Type::registerSerDe();
  auto restored = std::dynamic_pointer_cast<const RankNode>(
      RankNode::create(node->serialize(), nullptr));
  ASSERT_NE(restored, nullptr);
  EXPECT_EQ(
      (std::vector<column_index_t>{2}),
      restored->keyedStateSpec()->keyChannels());
  EXPECT_EQ(5, restored->rankEnd());
}

// Empty batch produces no output (alignment #19).
TEST_F(RankOperatorTest, emptyBatchProducesNoOutput) {
  RankTestSpec spec;
  auto chain = makeChain(spec);
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>{}),
           makeFlatVector<int64_t>(std::vector<int64_t>{})}));
  EXPECT_TRUE(chain.collector->collected().empty());
}

// A key channel outside the input schema (and not the single-channel
// sentinel of the global shape) fails loud at construction instead of
// inside the first probe (code review #8).
TEST_F(RankOperatorTest, keyChannelsOutsideSchemaFailLoudAtConstruction) {
  auto makeNodeWithKeyChannels = [&](std::vector<column_index_t> keyChannels,
                                     std::vector<TypePtr> keyTypes,
                                     const std::string& planNodeId) {
    return std::make_shared<const RankNode>(
        core::PlanNodeId{planNodeId},
        1,
        2,
        std::vector<column_index_t>{1},
        std::vector<bool>{true},
        std::vector<bool>{true},
        std::make_shared<const KeyedStateSpec>(
            std::move(keyChannels), std::move(keyTypes)),
        true,
        true,
        false,
        10000,
        inputType(),
        outputType(true));
  };
  auto makeOperator = [&](std::shared_ptr<const RankNode> node) {
    auto checkpointOperator =
        std::make_unique<RankCheckpointOperator>(0, driverCtx_.get(), node);
    return std::make_unique<RankOperator>(
        std::move(checkpointOperator),
        std::vector<StatefulOperatorPtr>(),
        node);
  };

  // Single channel past the sentinel position: outside the schema.
  EXPECT_THROW(
      makeOperator(makeNodeWithKeyChannels({3}, {BIGINT()}, "rank-bad-key-1")),
      VeloxRuntimeError);
  // Multi-channel spec with the sentinel position in it: the sentinel
  // exception only holds for the single-channel global shape.
  EXPECT_THROW(
      makeOperator(makeNodeWithKeyChannels(
          {0, 2}, {BIGINT(), BIGINT()}, "rank-bad-key-2")),
      VeloxRuntimeError);
  // The sentinel channel itself (single channel == input width) stays
  // constructible: that is the global shape (design D3).
  EXPECT_NO_THROW(
      makeOperator(makeNodeWithKeyChannels({2}, {BIGINT()}, "rank-sentinel")));
}

// rank2 B1/F4: the FirstN counter branch admits the first rankEnd_ rows per
// key across batches and emits +I with the rank only (alignment #26-32).
TEST_F(RankOperatorTest, firstNCountsAndEmitsOnlyInserts) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.firstN = true;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {2, 50}}));
  OutRows expectedBatch1{{0, 1, 30, false, 1}, {0, 2, 50, false, 1}};
  expectOutRows(drain(*chain.collector, true, 0), expectedBatch1, "b1");

  feed(*chain.rank, batch({{1, 20}, {2, 40}}));
  OutRows expectedBatch2{{0, 1, 20, false, 2}, {0, 2, 40, false, 2}};
  expectOutRows(drain(*chain.collector, true, 1), expectedBatch2, "b2");
}

// rank2 B1/F4: a full counter drops every further row before any state
// write (alignment #29, AppendOnlyFirstNFunction currentRank >= rankEnd).
TEST_F(RankOperatorTest, firstNDropsRowsBeyondN) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.firstN = true;
  spec.outputRankNumber = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}}));
  feed(*chain.rank, batch({{1, 10}, {1, 5}}));
  OutRows expected{{0, 1, 30, false, 1}, {0, 1, 20, false, 2}};
  expectOutRows(drain(*chain.collector, true), expected, "full counter");
}

// rank2 B1/F4: without the rank number the admitted rows emit plain +I and
// the output carries no rank column (alignment #31/#36).
TEST_F(RankOperatorTest, firstNWithoutRankNumber) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.firstN = true;
  spec.outputRankNumber = false;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}}));
  OutRows expected{
      {0, 1, 30, false, std::nullopt}, {0, 1, 20, false, std::nullopt}};
  expectOutRows(drain(*chain.collector, false), expected, "no number");
}

// rank2 B1xC3: the FirstN + offset combination - ranks below rankStart are
// swallowed by the unified emit gate, the output stays +I-only (alignment
// #33, the e2e 012 shape).
TEST_F(RankOperatorTest, firstNWithOffsetSwallowsBelowWindow) {
  RankTestSpec spec;
  spec.rankStart = 2;
  spec.rankEnd = 3;
  spec.firstN = true;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}, {1, 10}}));
  OutRows expected{{0, 1, 20, false, 2}, {0, 1, 10, false, 3}};
  expectOutRows(drain(*chain.collector, true, 0), expected, "offset window");

  feed(*chain.rank, batch({{1, 5}}));
  EXPECT_TRUE(drain(*chain.collector, true, 1).empty());
}

// rank2 design risk: the (1,1) window overlapping the FirstN shape takes
// the counter branch (native dispatch order, firstN before isTop1,
// alignment #44) - exactly one +I, never the Top1 -U/+U pairs.
TEST_F(RankOperatorTest, firstNDispatchPrecedesTop1) {
  RankTestSpec spec;
  spec.rankStart = 1;
  spec.rankEnd = 1;
  spec.firstN = true;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}, {1, 25}}));
  OutRows expected{{0, 1, 30, false, 1}};
  expectOutRows(drain(*chain.collector, true), expected, "firstN over top1");
}

// rank2 SPEC state boundary: each key holds exactly one int64 counter in
// the state - the single-element ListState maintenance of design D1.
TEST_F(RankOperatorTest, firstNStateUpperBoundSingleInt64) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.firstN = true;
  spec.outputRankNumber = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}, {1, 20}, {1, 10}, {2, 5}}));
  EXPECT_EQ((std::vector<int64_t>{2}), counterStateOf(*chain.rank, {1}));
  EXPECT_EQ((std::vector<int64_t>{1}), counterStateOf(*chain.rank, {2}));

  auto records = chain.rank->snapshotRankState(1);
  ASSERT_EQ(1, records.size());
  auto restored = makeChain(spec);
  restored.rank->restoreRankState(records);
  EXPECT_EQ((std::vector<int64_t>{2}), counterStateOf(*restored.rank, {1}));
  EXPECT_EQ((std::vector<int64_t>{1}), counterStateOf(*restored.rank, {2}));
}

// rank2 alignment #58: the counter survives a checkpoint and the restored
// operator continues exactly like the uninterrupted run - restored over the
// full range and over a non-full key-group range (the i64 handle under
// range-fragmented restore).
TEST_F(RankOperatorTest, firstNSnapshotRestoreRoundTrip) {
  RankTestSpec spec;
  spec.rankEnd = 3;
  spec.firstN = true;
  spec.outputRankNumber = true;

  // Full-range tier with key 1.
  auto continuous = makeChain(spec);
  feed(*continuous.rank, batch({{1, 30}, {1, 20}}));
  feed(*continuous.rank, batch({{1, 10}, {1, 5}}));
  auto continuousOut = drain(*continuous.collector, true);

  auto interrupted = makeChain(spec);
  feed(*interrupted.rank, batch({{1, 30}, {1, 20}}));
  auto records = interrupted.rank->snapshotRankState(2);
  ASSERT_EQ(1, records.size());

  auto restored = makeChain(spec);
  restored.rank->restoreRankState(records);
  feed(*restored.rank, batch({{1, 10}, {1, 5}}));
  auto restoredOut = drain(*restored.collector, true);

  OutRows expectedContinuation(
      continuousOut.end() - restoredOut.size(), continuousOut.end());
  expectOutRows(restoredOut, expectedContinuation, "full-range restore");

  // Non-full-range tier: a key whose key group lands inside [32, 64)
  // restores into a (128, 32, 32) backend and the counter still continues.
  auto reference = makeChain(spec);
  const auto key = keyInRange(*reference.rank, 32, 32);

  auto continuousHalf = makeChain(spec);
  feed(*continuousHalf.rank, batch({{key, 30}, {key, 20}}));
  feed(*continuousHalf.rank, batch({{key, 10}, {key, 5}}));
  auto continuousHalfOut = drain(*continuousHalf.collector, true);

  auto interruptedHalf = makeChain(spec);
  feed(*interruptedHalf.rank, batch({{key, 30}, {key, 20}}));
  auto halfRecords = interruptedHalf.rank->snapshotRankState(3);
  ASSERT_EQ(1, halfRecords.size());

  auto restoredHalf = makeChain(spec, 32, 32);
  restoredHalf.rank->restoreRankState(halfRecords);
  feed(*restoredHalf.rank, batch({{key, 10}, {key, 5}}));
  auto restoredHalfOut = drain(*restoredHalf.collector, true);

  OutRows expectedHalf(
      continuousHalfOut.end() - restoredHalfOut.size(),
      continuousHalfOut.end());
  expectOutRows(restoredHalfOut, expectedHalf, "half-range restore");
}

// rank2 C3: the offset window with the rank number - displacement pairs
// below rankStart are swallowed, in-window pairs survive, the tail insert
// enters the window as +I without -U (alignment #33/#37/#38).
TEST_F(RankOperatorTest, offsetWindowWithRowNumber) {
  RankTestSpec spec;
  spec.rankStart = 2;
  spec.rankEnd = 3;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}}));
  // The rank-1 tail insert lands below the window: the collector holds no
  // output record at all for this batch.
  EXPECT_TRUE(drain(*chain.collector, true).empty()) << "rank 1 is below";

  feed(*chain.rank, batch({{1, 20}}));
  OutRows expectedB2{{0, 1, 30, false, 2}};
  expectOutRows(drain(*chain.collector, true, 0), expectedB2, "tail enters");

  feed(*chain.rank, batch({{1, 10}}));
  OutRows expectedB3{
      {1, 1, 30, false, 2}, {2, 1, 20, false, 2}, {0, 1, 30, false, 3}};
  expectOutRows(drain(*chain.collector, true, 1), expectedB3, "displacement");

  feed(*chain.rank, batch({{1, 5}}));
  OutRows expectedB4{
      {1, 1, 20, false, 2},
      {2, 1, 10, false, 2},
      {1, 1, 30, false, 3},
      {2, 1, 20, false, 3}};
  expectOutRows(drain(*chain.collector, true, 2), expectedB4, "eviction");
}

// rank2 F6: the offset window without the rank number still runs the
// with-row-number algorithm (the without-number eviction cannot filter
// inner ranks), emits no rank column and never -D (alignment #35/#36/#41).
TEST_F(RankOperatorTest, offsetWindowWithoutRowNumber) {
  RankTestSpec spec;
  spec.rankStart = 2;
  spec.rankEnd = 3;
  spec.outputRankNumber = false;
  spec.generateUpdateBefore = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 30}}));
  EXPECT_TRUE(drain(*chain.collector, false).empty()) << "rank 1 is below";
  feed(*chain.rank, batch({{1, 20}}));
  OutRows expectedB2{{0, 1, 30, false, std::nullopt}};
  expectOutRows(drain(*chain.collector, false, 0), expectedB2, "tail enters");

  feed(*chain.rank, batch({{1, 10}}));
  OutRows expectedB3{
      {1, 1, 30, false, std::nullopt},
      {2, 1, 20, false, std::nullopt},
      {0, 1, 30, false, std::nullopt}};
  expectOutRows(drain(*chain.collector, false, 1), expectedB3, "displacement");

  feed(*chain.rank, batch({{1, 5}}));
  OutRows expectedB4{
      {1, 1, 20, false, std::nullopt},
      {2, 1, 10, false, std::nullopt},
      {1, 1, 30, false, std::nullopt},
      {2, 1, 20, false, std::nullopt}};
  expectOutRows(drain(*chain.collector, false, 2), expectedB4, "eviction");

  for (const auto& row : drain(*chain.collector, false)) {
    EXPECT_NE(3, row.kind) << "the offset window never emits -D";
  }
}

// rank2 alignment #37-40: the unpaired -U taxonomy of the offset window -
// in-window displacement emits (-U, +U) pairs, insufficient rows emit a
// bare +I, generateUpdateBefore=false emits +U only, and a row rejected by
// the full buffer emits nothing at all.
TEST_F(RankOperatorTest, offsetWindowUpdateBeforeTaxonomy) {
  RankTestSpec spec;
  spec.rankStart = 2;
  spec.rankEnd = 3;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;

  // (1) In-window displacement: fill the window, then insert at rank 2 -
  // only (-U, +U) pairs, no tail +I, the rank-4 victim leaves silently.
  {
    auto chain = makeChain(spec);
    feed(*chain.rank, batch({{1, 40}, {1, 30}, {1, 10}}));
    feed(*chain.rank, batch({{1, 20}}));
    OutRows expected{// batch 1: the window fills up (rank-1 emissions gated)
                     {0, 1, 40, false, 2},
                     {1, 1, 40, false, 2},
                     {2, 1, 30, false, 2},
                     {0, 1, 40, false, 3},
                     // batch 2: pure in-window displacement pairs
                     {1, 1, 30, false, 2},
                     {2, 1, 20, false, 2},
                     {1, 1, 40, false, 3},
                     {2, 1, 30, false, 3}};
    expectOutRows(drain(*chain.collector, true), expected, "pairs");
  }

  // (2) Insufficient rows: the row entering the open window rank from
  // below emits +I without -U.
  {
    auto chain = makeChain(spec);
    feed(*chain.rank, batch({{1, 40}}));
    feed(*chain.rank, batch({{1, 30}}));
    OutRows expected{{0, 1, 40, false, 2}};
    expectOutRows(drain(*chain.collector, true), expected, "insufficient");
  }

  // (3) generateUpdateBefore=false: displacement emits +U only.
  {
    RankTestSpec noUbSpec = spec;
    noUbSpec.generateUpdateBefore = false;
    auto chain = makeChain(noUbSpec);
    feed(*chain.rank, batch({{1, 40}, {1, 30}}));
    feed(*chain.rank, batch({{1, 20}}));
    OutRows expected{
        {0, 1, 40, false, 2}, {2, 1, 30, false, 2}, {0, 1, 40, false, 3}};
    expectOutRows(drain(*chain.collector, true), expected, "no -U");
  }

  // (4) A row worse than the full buffer's worst is rejected: zero
  // emission - no -D, no -U, nothing.
  {
    auto chain = makeChain(spec);
    feed(*chain.rank, batch({{1, 40}, {1, 30}, {1, 10}}));
    const auto before = chain.collector->collected().size();
    feed(*chain.rank, batch({{1, 50}}));
    EXPECT_EQ(before, chain.collector->collected().size())
        << "the rejected row emitted nothing";
  }
}

// rank2 alignment #42 + SPEC state boundary: the offset window keeps the
// full 1..rankEnd prefix in the state, not just the window width - rows
// below rankStart stay stored, rows beyond rankEnd are trimmed away.
TEST_F(RankOperatorTest, offsetWindowStateKeepsFullPrefixUpToRankEnd) {
  RankTestSpec spec;
  spec.rankStart = 2;
  spec.rankEnd = 3;
  spec.outputRankNumber = true;
  auto chain = makeChain(spec);

  feed(*chain.rank, batch({{1, 40}, {1, 30}, {1, 10}, {1, 5}, {2, 7}}));

  auto records = chain.rank->snapshotRankState(1);
  ASSERT_EQ(1, records.size());
  auto restored = makeChain(spec);
  restored.rank->restoreRankState(records);

  auto counts = storedRowCounts(*restored.rank, {1, 2});
  EXPECT_EQ((std::map<int64_t, size_t>{{1, 3}, {2, 1}}), counts);
  for (const auto& [key, count] : counts) {
    EXPECT_LE(count, static_cast<size_t>(spec.rankEnd)) << "key " << key;
  }
  EXPECT_EQ(
      (std::vector<int64_t>{5, 10, 30}), storedSortValues(*restored.rank, {1}));
  EXPECT_EQ((std::vector<int64_t>{7}), storedSortValues(*restored.rank, {2}));
}

// rank2 G2: DECIMAL sort keys - ShortDecimal compares through the BIGINT
// kind (alignment #49), nulls follow the nullsFirst flag.
TEST_F(RankOperatorTest, decimalSortKeys) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  spec.inputTypeOverride = ROW({"k", "d"}, {BIGINT(), DECIMAL(10, 2)});
  auto chain = makeChain(spec);

  // Unscaled int64 values: 10.50 -> 1050, 20.25 -> 2025, 5.75 -> 575.
  feed(*chain.rank, batch({{1, 1050}, {1, 2025}}));
  feed(*chain.rank, batch({{1, 575}}));
  OutRows expected{
      {0, 1, 1050, false, 1},
      {0, 1, 2025, false, 2},
      {1, 1, 1050, false, 1},
      {2, 1, 575, false, 1},
      {1, 1, 2025, false, 2},
      {2, 1, 1050, false, 2}};
  expectOutRows(drain(*chain.collector, true), expected, "short decimal");

  auto nullBatch = makeRowVector(
      {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
       makeNullableFlatVector<int64_t>(
           std::vector<std::optional<int64_t>>{std::nullopt}, DECIMAL(10, 2))});
  feed(*chain.rank, nullBatch);
  OutRows expectedNull{
      {1, 1, 575, false, 1},
      {2, 1, 0, true, 1},
      {1, 1, 1050, false, 2},
      {2, 1, 575, false, 2}};
  expectOutRows(
      drain(*chain.collector, true, 2), expectedNull, "short decimal null");
}

// rank2 G2: LongDecimal compares through the HUGEINT kind with true int128
// magnitudes (alignment #49).
TEST_F(RankOperatorTest, longDecimalSortKeys) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  spec.inputTypeOverride = ROW({"k", "d"}, {BIGINT(), DECIMAL(30, 2)});
  auto chain = makeChain(spec);

  const int128_t base = ((int128_t)1) << 70;
  auto row = [&](int64_t k, int128_t d) {
    return makeRowVector(
        {makeFlatVector<int64_t>(std::vector<int64_t>{k}),
         makeNullableFlatVector<int128_t>(
             std::vector<std::optional<int128_t>>{d}, DECIMAL(30, 2))});
  };

  feed(*chain.rank, row(1, base + 5));
  feed(*chain.rank, row(1, base + 10));
  feed(*chain.rank, row(1, base + 2));
  TypedOutRows<int128_t> expected{
      {0, 1, base + 5, false, 1},
      {0, 1, base + 10, false, 2},
      {1, 1, base + 5, false, 1},
      {2, 1, base + 2, false, 1},
      {1, 1, base + 10, false, 2},
      {2, 1, base + 5, false, 2}};
  expectTypedRows<int128_t>(
      drainTyped<int128_t>(*chain.collector, true), expected, "long decimal");

  // A magnitude below the high half still compares correctly.
  const int128_t lower = ((int128_t)1) << 69;
  feed(*chain.rank, row(1, lower + 100));
  TypedOutRows<int128_t> expectedLower{
      {1, 1, base + 2, false, 1},
      {2, 1, lower + 100, false, 1},
      {1, 1, base + 5, false, 2},
      {2, 1, base + 2, false, 2}};
  expectTypedRows<int128_t>(
      drainTyped<int128_t>(*chain.collector, true, 3),
      expectedLower,
      "long decimal high half");
}

// rank2 G2: DATE sort keys compare through the INTEGER kind (alignment
// #50), nulls follow the nullsFirst flag.
TEST_F(RankOperatorTest, dateSortKeys) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.outputRankNumber = true;
  spec.generateUpdateBefore = true;
  spec.nullsFirst = {true};
  spec.inputTypeOverride = ROW({"k", "dt"}, {BIGINT(), DATE()});
  auto chain = makeChain(spec);

  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeNullableFlatVector<int32_t>(
               std::vector<std::optional<int32_t>>{19450}, DATE())}));
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeNullableFlatVector<int32_t>(
               std::vector<std::optional<int32_t>>{19000}, DATE())}));
  TypedOutRows<int32_t> expected{
      {0, 1, 19450, false, 1},
      {1, 1, 19450, false, 1},
      {2, 1, 19000, false, 1},
      {0, 1, 19450, false, 2}};
  expectTypedRows<int32_t>(
      drainTyped<int32_t>(*chain.collector, true), expected, "date");

  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeNullableFlatVector<int32_t>(
               std::vector<std::optional<int32_t>>{std::nullopt}, DATE())}));
  TypedOutRows<int32_t> expectedNull{
      {1, 1, 19000, false, 1},
      {2, 1, 0, true, 1},
      {1, 1, 19450, false, 2},
      {2, 1, 19000, false, 2}};
  expectTypedRows<int32_t>(
      drainTyped<int32_t>(*chain.collector, true, 2),
      expectedNull,
      "date null");
}

// rank2 serde: the firstN field round-trips, and a plan JSON from before
// the field existed deserializes as the non-FirstN shapes.
TEST_F(RankOperatorTest, rankNodeSerdeRoundTripWithFirstN) {
  StatefulPlanNode::registerSerDe();
  Type::registerSerDe();

  RankTestSpec spec;
  spec.rankEnd = 3;
  spec.firstN = true;
  spec.outputRankNumber = true;
  auto node = makeNode(spec, "rank-serde-firstn");

  auto restored = std::dynamic_pointer_cast<const RankNode>(
      RankNode::create(node->serialize(), nullptr));
  ASSERT_NE(restored, nullptr);
  EXPECT_TRUE(restored->firstN());

  spec.firstN = false;
  auto plainNode = makeNode(spec, "rank-serde-plain");
  auto plainRestored = std::dynamic_pointer_cast<const RankNode>(
      RankNode::create(plainNode->serialize(), nullptr));
  ASSERT_NE(plainRestored, nullptr);
  EXPECT_FALSE(plainRestored->firstN());

  // Legacy plan JSON: the "firstN" key absent means false.
  auto obj = node->serialize();
  ASSERT_TRUE(obj.count("firstN"));
  obj.erase("firstN");
  auto legacy =
      std::dynamic_pointer_cast<const RankNode>(RankNode::create(obj, nullptr));
  ASSERT_NE(legacy, nullptr);
  EXPECT_FALSE(legacy->firstN());
  EXPECT_EQ(3, legacy->rankEnd());
}

// perf-rank-opt design S7.1: the compact storage row byte budget on the
// q19 shape (7 columns: 3 BIGINT + 3 VARCHAR + TIMESTAMP, strings at
// nexmark-realistic lengths) - every stored row <= 300 bytes (SPEC
// acceptance, friend introspection of the stored compact rows).
TEST_F(RankOperatorTest, compactRowByteBudgetQ19Schema) {
  RankTestSpec spec;
  spec.rankEnd = 10;
  spec.keyChannels = {0};
  spec.sortChannels = {2};
  spec.inputTypeOverride =
      ROW({"bid", "bidder", "price", "dt", "extra", "channel", "auction"},
          {BIGINT(),
           VARCHAR(),
           BIGINT(),
           TIMESTAMP(),
           VARCHAR(),
           VARCHAR(),
           BIGINT()});
  auto chain = makeChain(spec);

  const std::string bidder = "Bidder 1234567890123";
  const std::string extra(76, 'e');
  const std::string channel = "Housewares_52";
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>({1001, 1002}),
           makeFlatVector<std::string>({bidder, bidder}),
           makeFlatVector<int64_t>({1500000, 1400000}),
           makeFlatVector<Timestamp>(
               {Timestamp(1765000000, 123456789),
                Timestamp(1765000001, 234567891)}),
           makeFlatVector<std::string>({extra, extra}),
           makeFlatVector<std::string>({channel, channel}),
           makeFlatVector<int64_t>({77777, 77778})}));
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>({1001, 1002}),
           makeFlatVector<std::string>({bidder, bidder}),
           makeFlatVector<int64_t>({1600000, 1300000}),
           makeFlatVector<Timestamp>(
               {Timestamp(1765000002, 345678912),
                Timestamp(1765000003, 456789123)}),
           makeFlatVector<std::string>({extra, extra}),
           makeFlatVector<std::string>({channel, channel}),
           makeFlatVector<int64_t>({77779, 77780})}));

  const auto sizes = storedRowBytes(*chain.rank, {1001, 1002});
  EXPECT_EQ(2, sizes.at(1001).size());
  EXPECT_EQ(2, sizes.at(1002).size());
  for (const auto& [key, rowSizes] : sizes) {
    for (const auto rowSize : rowSizes) {
      EXPECT_LE(rowSize, 300) << "key " << key << " row of " << rowSize
                              << " bytes breaks the 300B storage budget";
    }
  }
  // The displacement actually stored rows for both keys (rank 1+2 each).
  auto counts = storedRowCounts(*chain.rank, {1001, 1002});
  EXPECT_EQ(2, counts.at(1001));
  EXPECT_EQ(2, counts.at(1002));
}

// perf-rank-opt design S7.1 (alignment #59): every layout kind in one row
// type - fixed scalars, DECIMAL/DATE fixture kinds, TIMESTAMP, VARCHAR in
// the null / empty / short-inline / long-heap forms - encode->decode
// equivalence over the emission path, then the same rows across a
// checkpoint round trip (three keys, one row each, so the +I emissions
// read every column back with no displacement noise).
TEST_F(RankOperatorTest, compactRowRoundTripAllTypes) {
  RankTestSpec spec;
  spec.rankEnd = 3;
  spec.keyChannels = {0};
  spec.sortChannels = {1};
  spec.inputTypeOverride =
      ROW({"k", "d", "dt", "s", "r", "dbl", "ts"},
          {BIGINT(),
           DECIMAL(10, 2),
           DATE(),
           VARCHAR(),
           REAL(),
           DOUBLE(),
           TIMESTAMP()});
  auto chain = makeChain(spec);

  const std::string emptyString = "";
  const std::string shortString = "short str";
  const std::string heapString =
      "a-72-byte-string-that-escapes-the-inline-window-"
      "and-lands-in-the-heap-buffer-of-the-vector";
  // The string locals outlive the feed: VectorMaker hands inline strings to
  // the vector as views into the std::string buffers, and the compact row
  // copies the bytes into its own varlen area during this synchronous feed.
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>({1, 2, 3}),
           makeNullableFlatVector<int64_t>(
               {std::nullopt, 575, 1050}, DECIMAL(10, 2)),
           makeNullableFlatVector<int32_t>(
               {19450, std::nullopt, 19000}, DATE()),
           makeNullableFlatVector<std::string>(
               {std::optional<std::string>{emptyString},
                std::optional<std::string>{shortString},
                std::optional<std::string>{heapString}}),
           makeNullableFlatVector<float>({1.5f, std::nullopt, -2.25f}),
           makeNullableFlatVector<double>({std::nullopt, 3.25, -7.5}),
           makeFlatVector<Timestamp>(
               {Timestamp(1765000000, 123456789),
                Timestamp(0, 0),
                Timestamp(1765000003, 999999999)})}));

  // One +I per key with rank 1: the emission decoder rebuilt every column
  // - null bits, empty string, short inline string, long heap string, the
  // raw timestamp pairs - byte for byte (alignment #59's decode inverse).
  {
    const auto& collected = chain.collector->collected();
    ASSERT_EQ(1, collected.size());
    const auto& record = collected[0];
    ASSERT_NE(record->rowKind(), nullptr);
    ASSERT_EQ(3, record->rowKind()->size());
    auto kVector = record->record()->childAt(0)->as<SimpleVector<int64_t>>();
    auto dVector = record->record()->childAt(1)->as<SimpleVector<int64_t>>();
    auto dtVector = record->record()->childAt(2)->as<SimpleVector<int32_t>>();
    auto sVector = record->record()->childAt(3)->as<SimpleVector<StringView>>();
    auto rVector = record->record()->childAt(4)->as<SimpleVector<float>>();
    auto dblVector = record->record()->childAt(5)->as<SimpleVector<double>>();
    auto tsVector = record->record()->childAt(6)->as<SimpleVector<Timestamp>>();
    auto rankVector = record->record()->childAt(7)->as<SimpleVector<int64_t>>();
    for (vector_size_t i = 0; i < 3; ++i) {
      EXPECT_EQ(
          static_cast<int8_t>(RowKind::INSERT), record->rowKind()->valueAt(i));
      EXPECT_EQ(i + 1, kVector->valueAt(i));
      EXPECT_EQ(1, rankVector->valueAt(i));
    }
    EXPECT_TRUE(dVector->isNullAt(0));
    EXPECT_FALSE(dVector->isNullAt(1));
    EXPECT_EQ(575, dVector->valueAt(1));
    EXPECT_EQ(1050, dVector->valueAt(2));
    EXPECT_EQ(19450, dtVector->valueAt(0));
    EXPECT_TRUE(dtVector->isNullAt(1));
    EXPECT_EQ(19000, dtVector->valueAt(2));
    EXPECT_FALSE(sVector->isNullAt(0));
    EXPECT_EQ("", sVector->valueAt(0).str());
    EXPECT_EQ("short str", sVector->valueAt(1).str());
    EXPECT_EQ(heapString, sVector->valueAt(2).str());
    EXPECT_EQ(1.5f, rVector->valueAt(0));
    EXPECT_TRUE(rVector->isNullAt(1));
    EXPECT_EQ(-2.25f, rVector->valueAt(2));
    EXPECT_TRUE(dblVector->isNullAt(0));
    EXPECT_EQ(3.25, dblVector->valueAt(1));
    EXPECT_EQ(-7.5, dblVector->valueAt(2));
    EXPECT_EQ(Timestamp(1765000000, 123456789), tsVector->valueAt(0));
    EXPECT_EQ(Timestamp(0, 0), tsVector->valueAt(1));
    EXPECT_EQ(Timestamp(1765000003, 999999999), tsVector->valueAt(2));
  }

  // Checkpoint round trip: the compact-row serializer (schema id
  // "compact-row:v1:...") carries the rows; a restored chain continues
  // with changelogs identical to the uninterrupted run.
  auto records = chain.rank->snapshotRankState(1);
  ASSERT_EQ(1, records.size());
  auto restored = makeChain(spec);
  restored.rank->restoreRankState(records);

  auto feedSummary = [&](CollectingOperator& collector, size_t fromRecord) {
    std::vector<std::string> summary;
    for (size_t r = fromRecord; r < collector.collected().size(); ++r) {
      const auto& record = collector.collected()[r];
      auto kVector = record->record()->childAt(0)->as<SimpleVector<int64_t>>();
      auto sVector =
          record->record()->childAt(3)->as<SimpleVector<StringView>>();
      auto rankVector =
          record->record()->childAt(7)->as<SimpleVector<int64_t>>();
      for (auto i = 0; i < record->record()->size(); ++i) {
        summary.push_back(fmt::format(
            "kind={} k={} s={} rn={}",
            static_cast<int>(record->rowKind()->valueAt(i)),
            kVector->valueAt(i),
            sVector->isNullAt(i) ? "<null>" : sVector->valueAt(i).str(),
            rankVector->valueAt(i)));
      }
    }
    return summary;
  };
  auto tailBatch = [&](int64_t k, int64_t d, const std::string& s) {
    // Explicit std::vector for every child: a braced single scalar like {k}
    // would resolve to the makeFlatVector(size) overload and fabricate a
    // poisoned k-sized column (children.front() sets the row count).
    return makeRowVector(
        {makeFlatVector<int64_t>(std::vector<int64_t>{k}),
         makeNullableFlatVector<int64_t>({d}, DECIMAL(10, 2)),
         makeNullableFlatVector<int32_t>({19450}, DATE()),
         makeFlatVector<std::string>(std::vector<std::string>{s}),
         makeNullableFlatVector<float>(std::vector<std::optional<float>>{0.5f}),
         makeNullableFlatVector<double>(
             std::vector<std::optional<double>>{1.0}),
         makeFlatVector<Timestamp>(
             std::vector<Timestamp>{Timestamp(1765000009, 1)})});
  };
  feed(*chain.rank, tailBatch(2, 100, "tail string for the displacement"));
  feed(*restored.rank, tailBatch(2, 100, "tail string for the displacement"));
  EXPECT_EQ(
      feedSummary(*chain.collector, 1), feedSummary(*restored.collector, 0));
}

// perf-rank-opt design S7.1 (alignment #62): the LRU residency is a hard
// key bound - cacheSize=20, rankEnd=4 fold to capacity 5, eight keys under
// pressure keep the cache at or under 5 entries.
TEST_F(RankOperatorTest, lruBoundHoldsUnderKeyPressure) {
  RankTestSpec spec;
  spec.rankEnd = 4;
  spec.cacheSize = 20;
  auto chain = makeChain(spec);
  for (int round = 0; round < 4; ++round) {
    std::vector<std::pair<int64_t, int64_t>> rows;
    for (int64_t key = 1; key <= 8; ++key) {
      rows.push_back({key, 100 - 10 * round - key});
    }
    feed(*chain.rank, batch(rows));
  }
  EXPECT_LE(cacheEntriesOf(*chain.rank), 5);
  // The state kept every key's rows (write-through, alignment #63).
  auto counts = storedRowCounts(*chain.rank, {1, 2, 3, 4, 5, 6, 7, 8});
  for (const auto& [key, count] : counts) {
    EXPECT_EQ(4, count) << "key " << key;
  }
}

// perf-rank-opt design S7.1 (alignment #64, the native miss-replay
// precedent AppendOnlyTopNFunction#initHeapStates): a chain whose cache
// evicts and a chain that never evicts emit identical changelogs for the
// same input - eviction only loses a derived buffer, the replay rebuilds
// it from the state list.
TEST_F(RankOperatorTest, lruEvictionReplayEquivalence) {
  const std::vector<std::vector<std::pair<int64_t, int64_t>>> batches = {
      {{1, 40}, {2, 31}, {3, 22}, {4, 13}, {5, 44}, {6, 35}, {7, 26}, {8, 17}},
      {{1, 30}, {2, 21}, {3, 12}, {4, 43}, {5, 34}, {6, 25}, {7, 16}, {8, 47}},
      {{1, 20}, {2, 11}, {3, 42}, {4, 33}, {5, 24}, {6, 15}, {7, 46}, {8, 37}},
      {{1, 10}, {2, 51}, {3, 32}, {4, 23}, {5, 14}, {6, 45}, {7, 36}, {8, 27}},
  };
  RankTestSpec evictingSpec;
  evictingSpec.rankEnd = 2;
  // capacity 4: eight keys force eviction cycles on every batch.
  evictingSpec.cacheSize = 8;
  auto evicting = makeChain(evictingSpec);
  RankTestSpec residentSpec;
  residentSpec.rankEnd = 2;
  residentSpec.cacheSize = 100000;
  auto resident = makeChain(residentSpec);
  for (const auto& rows : batches) {
    feed(*evicting.rank, batch(rows));
    feed(*resident.rank, batch(rows));
  }
  const auto evictingOut = drain(*evicting.collector, true);
  const auto residentOut = drain(*resident.collector, true);
  expectOutRows(evictingOut, residentOut, "eviction replay equivalence");
  ASSERT_FALSE(evictingOut.empty());
  EXPECT_LE(cacheEntriesOf(*evicting.rank), 4);
}

// perf-rank-opt design S7.1 (alignment #63/#65): after eviction pressure
// the state lists hold exactly what the never-evicting chain holds.
TEST_F(RankOperatorTest, lruEvictionKeepsStateComplete) {
  RankTestSpec evictingSpec;
  evictingSpec.rankEnd = 3;
  evictingSpec.cacheSize = 8; // capacity 4 for 8 keys
  auto evicting = makeChain(evictingSpec);
  RankTestSpec residentSpec;
  residentSpec.rankEnd = 3;
  residentSpec.cacheSize = 100000;
  auto resident = makeChain(residentSpec);
  for (int round = 0; round < 3; ++round) {
    std::vector<std::pair<int64_t, int64_t>> rows;
    for (int64_t key = 1; key <= 8; ++key) {
      rows.push_back({key, 100 - 10 * round - key});
    }
    feed(*evicting.rank, batch(rows));
    feed(*resident.rank, batch(rows));
  }
  const auto evictingCounts =
      storedRowCounts(*evicting.rank, {1, 2, 3, 4, 5, 6, 7, 8});
  const auto residentCounts =
      storedRowCounts(*resident.rank, {1, 2, 3, 4, 5, 6, 7, 8});
  EXPECT_EQ(residentCounts, evictingCounts);
  for (const auto& [key, count] : evictingCounts) {
    EXPECT_EQ(3, count) << "key " << key;
  }
}

// perf-rank-opt design S7.1 (alignment #76): an evicted key replayed after
// a batch boundary (the backend reclaimed dead rows in advance) rebuilds
// its buffer and continues correctly - the cache key pointer stays a live
// key row because a non-empty state list pins it.
TEST_F(RankOperatorTest, lruEvictedKeyRowNotDangling) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.cacheSize = 8; // capacity 4
  auto chain = makeChain(spec);
  // Batch 1: five keys, two rows each - key 1's entry evicts when the
  // fifth key first touches the cache; the batch boundary advances and
  // key 1's row stays alive (pinned by its non-empty list).
  std::vector<std::pair<int64_t, int64_t>> rows1;
  for (int64_t key = 1; key <= 5; ++key) {
    rows1.push_back({key, 50 - key});
    rows1.push_back({key, 60 - key});
  }
  feed(*chain.rank, batch(rows1));
  // Batch 2: key 1 again - a cache miss replay through the comparator
  // while the previous boundary already ran advance(). Batch 1 stored
  // 49 and 59 for key 1 (rows 50-key and 60-key): the replay walks
  // [-U 49, +U 10, -U 59, +U 49] over ranks 1 and 2.
  feed(*chain.rank, batch({{1, 10}}));
  OutRows expectedBatch2{
      {1, 1, 49, false, 1},
      {2, 1, 10, false, 1},
      {1, 1, 59, false, 2},
      {2, 1, 49, false, 2}};
  expectOutRows(
      drain(*chain.collector, true, 1),
      expectedBatch2,
      "replay after eviction");
  EXPECT_EQ(2, storedRowCounts(*chain.rank, {1}).at(1));
}

// perf-rank-opt design S7.1 (SPEC verification plan, P=4 sub-range
// rescale): a full-range snapshot restores into four mutually exclusive
// quarter-range backends - the union of the shards is the original key
// set, each key lands in exactly the shard covering its key group.
TEST_F(RankOperatorTest, compactCheckpointFourWayShards) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  auto chain = makeChain(spec);
  std::vector<std::pair<int64_t, int64_t>> rows;
  for (int64_t key = 1; key <= 8; ++key) {
    rows.push_back({key, 80 - key});
  }
  feed(*chain.rank, batch(rows));
  auto records = chain.rank->snapshotRankState(1);
  ASSERT_EQ(1, records.size());

  auto reference = makeChain(spec);
  std::vector<decltype(reference)> shards;
  for (uint32_t shard = 0; shard < 4; ++shard) {
    shards.push_back(
        makeChain(spec, kMaxParallelism / 4 * shard, kMaxParallelism / 4));
    shards.back().rank->restoreRankState(records);
  }
  size_t restoredKeys = 0;
  for (int64_t key = 1; key <= 8; ++key) {
    const uint32_t keyGroup = keyGroupOf(*reference.rank, key);
    const uint32_t shard = keyGroup / (kMaxParallelism / 4);
    EXPECT_EQ(1, storedRowCounts(*shards[shard].rank, {key}).at(key))
        << "key " << key << " key group " << keyGroup;
    for (uint32_t other = 0; other < 4; ++other) {
      if (other != shard) {
        EXPECT_THROW(
            storedRowCounts(*shards[other].rank, {key}), VeloxRuntimeError)
            << "key " << key << " visible in shard " << other;
      }
    }
    ++restoredKeys;
  }
  EXPECT_EQ(8, restoredKeys);
}

// perf-rank-opt design S7.1 (SPEC verification plan, Top1 compact compare
// path): nulls never update (the null ordering result is final), equal
// sort keys never update, only a strictly better row emits -U/+U.
TEST_F(RankOperatorTest, top1CompactNullAndEqualKeys) {
  RankTestSpec spec;
  spec.rankStart = 1;
  spec.rankEnd = 1;
  spec.nullsFirst = {false};
  auto chain = makeChain(spec);

  feed(*chain.rank, batchWithNullV({std::nullopt}));
  OutRows expectedB1{{0, 1, 0, true, 1}};
  expectOutRows(drain(*chain.collector, true, 0), expectedB1, "first null +I");

  // An equal null is not strictly better: no output.
  feed(*chain.rank, batchWithNullV({std::nullopt}));
  EXPECT_TRUE(drain(*chain.collector, true, 1).empty())
      << "equal null no update";

  // A non-null row beats nulls-last: -U/+U. Batch 2 emitted no record, so
  // this batch's record is the collector's second one (index 1).
  feed(*chain.rank, batch({{1, 5}}));
  OutRows expectedB3{{1, 1, 0, true, 1}, {2, 1, 5, false, 1}};
  expectOutRows(
      drain(*chain.collector, true, 1), expectedB3, "better than null");

  // Equal value: no update (still two records so far).
  feed(*chain.rank, batch({{1, 5}}));
  EXPECT_TRUE(drain(*chain.collector, true, 2).empty())
      << "equal value no update";

  // Strictly better: -U/+U (the third record, index 2).
  feed(*chain.rank, batch({{1, 3}}));
  OutRows expectedB5{{1, 1, 5, false, 1}, {2, 1, 3, false, 1}};
  expectOutRows(
      drain(*chain.collector, true, 2), expectedB5, "strictly better");
}

// perf-rank-opt design S7.1 (design S3.2 step 6): the emission decoder
// rebuilds VARCHAR columns byte-exact across the displacement chain -
// long heap strings included.
TEST_F(RankOperatorTest, varcharEmissionDecodeContent) {
  RankTestSpec spec;
  spec.rankEnd = 2;
  spec.keyChannels = {0};
  spec.sortChannels = {2};
  spec.inputTypeOverride =
      ROW({"k", "s", "price"}, {BIGINT(), VARCHAR(), BIGINT()});
  auto chain = makeChain(spec);

  const std::string s30 = "thirty-bytes-of-string-content-aaa";
  const std::string s20 = "twenty-bytes-str-bbbb";
  const std::string s10 = "ten-bytes-cc";
  const std::string s5 = "tiny-dd";
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<std::string>({s30}),
           makeFlatVector<int64_t>(std::vector<int64_t>{30})}));
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<std::string>({s20}),
           makeFlatVector<int64_t>(std::vector<int64_t>{20})}));
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<std::string>({s10}),
           makeFlatVector<int64_t>(std::vector<int64_t>{10})}));
  feed(
      *chain.rank,
      makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>{1}),
           makeFlatVector<std::string>({s5}),
           makeFlatVector<int64_t>(std::vector<int64_t>{5})}));

  const auto& collected = chain.collector->collected();
  std::vector<std::pair<int8_t, std::string>> emitted;
  std::vector<int64_t> emittedRanks;
  for (const auto& record : collected) {
    auto sVector = record->record()->childAt(1)->as<SimpleVector<StringView>>();
    auto rankVector = record->record()->childAt(3)->as<SimpleVector<int64_t>>();
    for (auto i = 0; i < record->record()->size(); ++i) {
      emitted.emplace_back(
          record->rowKind()->valueAt(i),
          sVector->isNullAt(i) ? std::string() : sVector->valueAt(i).str());
      emittedRanks.push_back(rankVector->valueAt(i));
    }
  }
  // Displacement chains per batch (rankEnd 2, price ascending):
  // b1: +I s30@1.
  // b2: -U s30@1, +U s20@1, tail +I s30@2 (buffer not yet full).
  // b3: -U s20@1, +U s10@1, -U s30@2, +U s20@2 (full, the rank-3 row
  //     purges silently).
  // b4: -U s10@1, +U s5@1, -U s20@2, +U s10@2.
  const std::vector<std::pair<int8_t, std::string>> expected{
      {0, s30},
      {1, s30},
      {2, s20},
      {0, s30},
      {1, s20},
      {2, s10},
      {1, s30},
      {2, s20},
      {1, s10},
      {2, s5},
      {1, s20},
      {2, s10}};
  ASSERT_EQ(expected.size(), emitted.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(expected[i].first, emitted[i].first) << "row " << i;
    EXPECT_EQ(expected[i].second, emitted[i].second) << "row " << i;
  }
  const std::vector<int64_t> expectedRanks{1, 1, 1, 2, 1, 1, 2, 2, 1, 1, 2, 2};
  EXPECT_EQ(expectedRanks, emittedRanks);
}

// perf-rank-opt design S7.1 (SPEC verification plan, FirstN zero-copy
// paths over the workspace batch-level diff): lazy children load before
// the dictionary wrap, the offset window swallows below-window rows
// while still counting them, and a full counter rejects a whole batch
// with no output at all.
namespace {
class FixedVectorLoader : public VectorLoader {
 public:
  explicit FixedVectorLoader(VectorPtr vector) : vector_(std::move(vector)) {}

  void loadInternal(
      RowSet /*rows*/,
      ValueHook* /*hook*/,
      vector_size_t /*resultSize*/,
      VectorPtr* result) override {
    *result = vector_;
  }

 private:
  const VectorPtr vector_;
};
} // namespace

TEST_F(RankOperatorTest, firstNZeroCopyBatchPaths) {
  // Path 1: lazy children - the wrap happens after the load, the emitted
  // rows carry the input's own values.
  {
    RankTestSpec spec;
    spec.rankEnd = 2;
    spec.firstN = true;
    spec.outputRankNumber = true;
    auto chain = makeChain(spec);
    const auto kFlat = makeFlatVector<int64_t>({1, 1});
    const auto vFlat = makeFlatVector<int64_t>({30, 20});
    std::vector<VectorPtr> children = {
        std::make_shared<LazyVector>(
            pool(), BIGINT(), 2, std::make_unique<FixedVectorLoader>(kFlat)),
        std::make_shared<LazyVector>(
            pool(), BIGINT(), 2, std::make_unique<FixedVectorLoader>(vFlat))};
    feed(*chain.rank, makeRowVector(children));
    OutRows expected{{0, 1, 30, false, 1}, {0, 1, 20, false, 2}};
    expectOutRows(drain(*chain.collector, true, 0), expected, "lazy children");
  }

  // Path 2: the offset window swallows below-window rows but the counter
  // still counts them (state keeps counting, output is gated).
  {
    RankTestSpec spec;
    spec.rankStart = 2;
    spec.rankEnd = 3;
    spec.firstN = true;
    spec.outputRankNumber = true;
    auto chain = makeChain(spec);
    feed(*chain.rank, batch({{1, 30}, {1, 20}, {1, 10}}));
    OutRows expected{{0, 1, 20, false, 2}, {0, 1, 10, false, 3}};
    expectOutRows(drain(*chain.collector, true, 0), expected, "offset window");
    EXPECT_EQ((std::vector<int64_t>{3}), counterStateOf(*chain.rank, {1}));
  }

  // Path 3: a full counter rejects the whole batch - zero pushOutput.
  {
    RankTestSpec spec;
    spec.rankEnd = 2;
    spec.firstN = true;
    spec.outputRankNumber = true;
    auto chain = makeChain(spec);
    feed(*chain.rank, batch({{1, 30}, {1, 20}}));
    const auto recordsAfterFill = chain.collector->collected().size();
    EXPECT_EQ(1, recordsAfterFill);
    feed(*chain.rank, batch({{1, 10}, {1, 5}}));
    EXPECT_EQ(recordsAfterFill, chain.collector->collected().size())
        << "the full-counter batch emitted nothing";
    EXPECT_EQ((std::vector<int64_t>{2}), counterStateOf(*chain.rank, {1}));
  }
}

// perf-rank-opt design S7.1 (alignment #68): the cacheSize plan field
// round-trips exactly, and a plan JSON from before the field existed
// deserializes as the native default 10000.
TEST_F(RankOperatorTest, rankNodeSerdeCacheSizeDefaults) {
  StatefulPlanNode::registerSerDe();
  Type::registerSerDe();

  RankTestSpec spec;
  spec.rankEnd = 3;
  spec.cacheSize = 777;
  auto node = makeNode(spec, "rank-serde-cachesize");

  auto restored = std::dynamic_pointer_cast<const RankNode>(
      RankNode::create(node->serialize(), nullptr));
  ASSERT_NE(restored, nullptr);
  EXPECT_EQ(777, restored->cacheSize());

  // serialize() always writes the key (the same always-write shape as
  // firstN - the tolerant default only guards the read side).
  auto obj = node->serialize();
  ASSERT_TRUE(obj.count("cacheSize"));

  // Legacy plan JSON: the "cacheSize" key absent means 10000.
  obj.erase("cacheSize");
  auto legacy =
      std::dynamic_pointer_cast<const RankNode>(RankNode::create(obj, nullptr));
  ASSERT_NE(legacy, nullptr);
  EXPECT_EQ(10000, legacy->cacheSize());
  EXPECT_EQ(3, legacy->rankEnd());
}

} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
