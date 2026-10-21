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
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string_view>
#include <vector>

#include <folly/dynamic.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/KeyedStateSpec.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/HashMapStateBackend.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/KeySerializer.h"
#include "velox/experimental/stateful/state/KeyTable.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/experimental/stateful/state/StateMap.h"
#include "velox/experimental/stateful/state/StateTable.h"
#include "velox/experimental/stateful/state/StreamOperatorStateHandler.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::stateful {
namespace {

using namespace facebook::velox;

// 391 and 32728 are the collision pair that broke the old identity: both
// map to 250707955 under fmix64 % INT_MAX, so the old partition() silently
// merged them. The storage below must keep them apart.
constexpr int64_t kCollisionKeyA = 391;
constexpr int64_t kCollisionKeyB = 32728;
constexpr uint32_t kMaxParallelism = 128;

class StateBackendTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  RowVectorPtr makeInput(const std::vector<std::optional<int64_t>>& keys) {
    return makeRowVector({makeNullableFlatVector<int64_t>(keys)});
  }

  // Probes 'values' through the production grouping path and returns the
  // resulting per-row state keys.
  std::vector<RowContainerStateKey> probeKeys(
      KeySelector& selector,
      const std::vector<std::optional<int64_t>>& values) {
    selector.probe(makeInput(values));
    auto keys = selector.keys();
    return std::vector<RowContainerStateKey>(keys.begin(), keys.end());
  }

  // Hand-built key container for tests that control hashes and key groups
  // directly. The hash mirrors what the identity contract requires: equal
  // values get equal hashes.
  struct KeyLab {
    exec::RowContainer container;
    RowContainerKeySchema schema;

    explicit KeyLab(memory::MemoryPool* pool)
        : container({BIGINT()}, pool), schema(&container, {BIGINT()}) {}

    uint64_t hash(int64_t value) const {
      return static_cast<uint64_t>(std::hash<int64_t>{}(value));
    }

    RowContainerStateKey key(int64_t value) {
      char* row = container.newRow();
      const auto column = container.columnAt(0);
      *(row + column.nullByte()) &= ~column.nullMask();
      *reinterpret_cast<int64_t*>(row + column.offset()) = value;
      return RowContainerStateKey(&schema, row, hash(value), kMaxParallelism);
    }
  };

  // A real partial sum(BIGINT) aggregate wired the way StreamingAggregation
  // wires HashAggregation rows: the descriptor's accumulator array, the init
  // callback (initializeNewGroups) and the merge callback
  // (addSingleGroupIntermediateResults) all come from the aggregate, so the
  // tests exercise the production accumulator layout instead of hand-written
  // row offsets. The counters record callback runs for assertions. An
  // instance must be declared before the backend and outlive it: the
  // accumulator closures capture 'function' by raw pointer and the value
  // container destroys accumulators through them.
  struct SumAggregate {
    // Registration must precede the create call in the member initializer,
    // hence the static factory.
    static std::unique_ptr<exec::Aggregate> makeFunction() {
      aggregate::prestosql::registerAllAggregateFunctions();
      return exec::Aggregate::create(
          "sum",
          core::AggregationNode::Step::kPartial,
          {BIGINT()},
          BIGINT(),
          core::QueryConfig({}));
    }

    std::unique_ptr<exec::Aggregate> function = makeFunction();

    int initCount = 0;
    int mergeCount = 0;

    // Wires the aggregate to the first accumulator column of the state's
    // value container. Must run before the first rows() call (and before
    // restore, which also constructs rows) so the callbacks write the
    // offsets the container actually laid out.
    void wire(exec::RowContainer* valueRows) {
      function->setAllocator(&valueRows->stringAllocator());
      const auto column = valueRows->columnAt(0);
      function->setOffsets(
          column.offset(),
          column.nullByte(),
          column.nullMask(),
          column.initializedByte(),
          column.initializedMask(),
          valueRows->rowSizeOffset());
    }

    AggregatingStateDescriptor descriptor(memory::MemoryPool* pool) {
      return AggregatingStateDescriptor(
          "acc",
          {exec::Accumulator(function.get(), BIGINT())},
          [this](char* row) {
            ++initCount;
            char* groups[1] = {row};
            const std::vector<vector_size_t> indices{0};
            function->initializeNewGroups(
                groups,
                folly::Range<const vector_size_t*>(
                    indices.data(), indices.size()));
          },
          [this](char* row, const std::vector<VectorPtr>& columns) {
            ++mergeCount;
            function->addSingleGroupIntermediateResults(
                row, SelectivityVector(1), columns, false);
          },
          pool);
    }

    // Adds 'input' (a single-row BIGINT vector) to 'row' as the operator's
    // addRawInput would.
    void add(char* row, const VectorPtr& input) {
      function->addSingleGroupRawInput(
          row, SelectivityVector(1), {input}, false);
    }

    // The current intermediate value (running total) of 'row', the same
    // extraction the snapshot path performs. extractAccumulators requires a
    // pre-allocated flat result vector, like HashAggregation's extract.
    int64_t valueOf(char* row, memory::MemoryPool* pool) {
      char* groups[1] = {row};
      VectorPtr result = BaseVector::create(BIGINT(), 1, pool);
      function->extractAccumulators(groups, 1, &result);
      return result->asFlatVector<int64_t>()->valueAt(0);
    }
  };

  // sum(BIGINT) + avg(BIGINT) + min(BIGINT) in one descriptor: unlike
  // SumAggregate this exercises a multi-accumulator array (descriptor order
  // against column order, checkpoint frame order and merge dispatch), a
  // complex intermediate type (avg: row(double, bigint) through the presto
  // serde) and the null-accumulator lifecycle (aggregates of no non-null
  // input snapshot as null and restore as null). Same wiring pattern and
  // lifecycle rules as SumAggregate.
  struct SumAvgMin {
    static std::unique_ptr<exec::Aggregate> makeFunction(
        const std::string& name,
        const TypePtr& resultType) {
      aggregate::prestosql::registerAllAggregateFunctions();
      return exec::Aggregate::create(
          name,
          core::AggregationNode::Step::kPartial,
          {BIGINT()},
          resultType,
          core::QueryConfig({}));
    }

    std::unique_ptr<exec::Aggregate> sum = makeFunction("sum", BIGINT());
    std::unique_ptr<exec::Aggregate> avg = makeFunction("avg", DOUBLE());
    std::unique_ptr<exec::Aggregate> min = makeFunction("min", BIGINT());

    // The avg accumulator serializes as row(double, bigint): (total, count).
    static TypePtr avgIntermediateType() {
      return ROW({DOUBLE(), BIGINT()});
    }

    int initCount = 0;
    int mergeCount = 0;

    void wire(exec::RowContainer* valueRows) {
      exec::Aggregate* functions[] = {sum.get(), avg.get(), min.get()};
      for (size_t i = 0; i < std::size(functions); ++i) {
        functions[i]->setAllocator(&valueRows->stringAllocator());
        const auto column = valueRows->columnAt(static_cast<int32_t>(i));
        functions[i]->setOffsets(
            column.offset(),
            column.nullByte(),
            column.nullMask(),
            column.initializedByte(),
            column.initializedMask(),
            valueRows->rowSizeOffset());
      }
    }

    AggregatingStateDescriptor descriptor(memory::MemoryPool* pool) {
      std::vector<exec::Accumulator> accumulators;
      accumulators.emplace_back(sum.get(), BIGINT());
      accumulators.emplace_back(avg.get(), avgIntermediateType());
      accumulators.emplace_back(min.get(), BIGINT());
      return AggregatingStateDescriptor(
          "acc",
          std::move(accumulators),
          [this](char* row) {
            ++initCount;
            for (auto* function : {sum.get(), avg.get(), min.get()}) {
              char* groups[1] = {row};
              const std::vector<vector_size_t> indices{0};
              function->initializeNewGroups(
                  groups,
                  folly::Range<const vector_size_t*>(
                      indices.data(), indices.size()));
            }
          },
          [this](char* row, const std::vector<VectorPtr>& columns) {
            ++mergeCount;
            exec::Aggregate* functions[] = {sum.get(), avg.get(), min.get()};
            for (size_t i = 0; i < std::size(functions); ++i) {
              functions[i]->addSingleGroupIntermediateResults(
                  row, SelectivityVector(1), {columns[i]}, false);
            }
          },
          pool);
    }

    // Adds 'input' (a one-row nullable BIGINT vector) to every aggregate, as
    // the operator's addRawInput would.
    void add(char* row, const VectorPtr& input) {
      for (auto* function : {sum.get(), avg.get(), min.get()}) {
        function->addSingleGroupRawInput(
            row, SelectivityVector(1), {input}, false);
      }
    }

    // The current intermediate state of 'row', one extractor per aggregate —
    // the same extraction the snapshot path performs.
    int64_t sumOf(char* row, memory::MemoryPool* pool) {
      char* groups[1] = {row};
      VectorPtr result = BaseVector::create(BIGINT(), 1, pool);
      sum->extractAccumulators(groups, 1, &result);
      return result->asFlatVector<int64_t>()->valueAt(0);
    }

    std::pair<double, int64_t> avgOf(char* row, memory::MemoryPool* pool) {
      char* groups[1] = {row};
      VectorPtr result = BaseVector::create(avgIntermediateType(), 1, pool);
      avg->extractAccumulators(groups, 1, &result);
      const auto* intermediate = result->as<RowVector>();
      return {
          intermediate->childAt(0)->asFlatVector<double>()->valueAt(0),
          intermediate->childAt(1)->asFlatVector<int64_t>()->valueAt(0)};
    }

    bool minIsNull(char* row, memory::MemoryPool* pool) {
      char* groups[1] = {row};
      VectorPtr result = BaseVector::create(BIGINT(), 1, pool);
      min->extractAccumulators(groups, 1, &result);
      return result->asFlatVector<int64_t>()->isNullAt(0);
    }

    int64_t minOf(char* row, memory::MemoryPool* pool) {
      char* groups[1] = {row};
      VectorPtr result = BaseVector::create(BIGINT(), 1, pool);
      min->extractAccumulators(groups, 1, &result);
      return result->asFlatVector<int64_t>()->valueAt(0);
    }
  };

  // Descriptor serializers of the value types used across the tests.
  TypeSerializerPtr int64Serializer() {
    return std::make_shared<ValueSerializer<int64_t>>();
  }

  TypeSerializerPtr sharedInt64Serializer() {
    return std::make_shared<SharedPtrSerializer<int64_t>>(
        std::make_shared<ValueSerializer<int64_t>>());
  }
};

// Basics of the (K, N) -> S map: miss is nullptr, put / get / containsKey /
// remove / size, and the collision pair stays two entries.
TEST_F(StateBackendTest, stateMapBasics) {
  KeyLab lab(pool());
  auto keyA = lab.key(kCollisionKeyA);
  auto keyB = lab.key(kCollisionKeyB);
  const VoidNamespace ns;

  StateMap<RowContainerStateKey, VoidNamespace, char*> map;
  char valueA{'a'};
  char valueB{'b'};

  EXPECT_EQ(nullptr, map.get(keyA, ns));
  map.put(keyA, ns, &valueA);
  map.put(keyB, ns, &valueB);
  EXPECT_EQ(&valueA, map.get(keyA, ns));
  EXPECT_EQ(&valueB, map.get(keyB, ns));
  EXPECT_NE(map.get(keyA, ns), map.get(keyB, ns));
  EXPECT_TRUE(map.containsKey(keyA, ns));
  EXPECT_TRUE(map.containsKey(keyB, ns));
  EXPECT_EQ(2, map.size());

  map.remove(keyA, ns);
  EXPECT_EQ(nullptr, map.get(keyA, ns));
  EXPECT_FALSE(map.containsKey(keyA, ns));
  EXPECT_EQ(&valueB, map.get(keyB, ns));
  EXPECT_EQ(1, map.size());
}

// Key identity is the key value, not the row instance: a different
// StateKey over an equal value hits the same entry.
TEST_F(StateBackendTest, stateMapKeyEqualityByValue) {
  KeyLab lab(pool());
  auto keyA1 = lab.key(kCollisionKeyA);
  auto keyA2 = lab.key(kCollisionKeyA);
  ASSERT_TRUE(keyA1.equals(keyA2));
  const VoidNamespace ns;

  StateMap<RowContainerStateKey, VoidNamespace, char*> map;
  char value{'v'};
  map.put(keyA1, ns, &value);
  EXPECT_EQ(&value, map.get(keyA2, ns));
  EXPECT_EQ(1, map.size());
}

// Growth beyond the capacity threshold doubles the table and rehashes
// incrementally; every entry stays reachable across the process.
TEST_F(StateBackendTest, stateMapGrowsAndRehashes) {
  KeyLab lab(pool());
  const VoidNamespace ns;
  StateMap<RowContainerStateKey, VoidNamespace, char*> map;

  constexpr int kNumKeys = 300;
  std::vector<RowContainerStateKey> keys;
  keys.reserve(kNumKeys);
  std::vector<char> values(kNumKeys);
  for (int i = 0; i < kNumKeys; ++i) {
    keys.push_back(lab.key(i));
    values[i] = static_cast<char>('a' + (i % 26));
    map.put(keys[i], ns, &values[i]);
  }
  ASSERT_EQ(kNumKeys, map.size());
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_EQ(&values[i], map.get(keys[i], ns)) << "lost key " << i;
  }

  map.remove(keys[7], ns);
  map.remove(keys[173], ns);
  EXPECT_EQ(kNumKeys - 2, map.size());
  EXPECT_EQ(nullptr, map.get(keys[7], ns));
  EXPECT_EQ(&values[8], map.get(keys[8], ns));
}

// An open StateMap snapshot keeps seeing the entries as they were: later
// puts prepend entries the captured heads cannot reach, and removals and
// overwrites copy-on-write the touched entries. After release, the live
// view reflects the mutations.
TEST_F(StateBackendTest, stateMapSnapshotIsolation) {
  KeyLab lab(pool());
  auto key1 = lab.key(1);
  auto key2 = lab.key(2);
  auto key3 = lab.key(3);
  const VoidNamespace ns;
  StateMap<RowContainerStateKey, VoidNamespace, char*> map(8);
  char a{'a'}, b{'b'}, x{'x'}, c{'c'};
  map.put(key1, ns, &a);
  map.put(key2, ns, &b);

  auto snapshot = map.createSnapshot();
  map.put(key1, ns, &x);
  map.remove(key2, ns);
  map.put(key3, ns, &c);

  std::map<const char*, char*> observed;
  for (const auto& head : snapshot.heads) {
    for (auto entry = head; entry != nullptr; entry = entry->next_) {
      observed[entry->key_.row()] = entry->state_;
    }
  }
  EXPECT_EQ(2, observed.size());
  EXPECT_EQ(&a, observed[key1.row()]);
  EXPECT_EQ(&b, observed[key2.row()]);
  EXPECT_EQ(observed.end(), observed.find(key3.row()));

  map.releaseSnapshot(snapshot.version);
  EXPECT_EQ(&x, map.get(key1, ns));
  EXPECT_EQ(nullptr, map.get(key2, ns));
  EXPECT_EQ(&c, map.get(key3, ns));
  EXPECT_EQ(2, map.size());
}

// A snapshot taken while an incremental rehash is in flight captures the
// split layout (primary tail plus both incremental segments) and still
// observes every entry exactly once; the live map stays consistent.
TEST_F(StateBackendTest, stateMapSnapshotDuringIncrementalRehash) {
  KeyLab lab(pool());
  const VoidNamespace ns;
  StateMap<RowContainerStateKey, VoidNamespace, char*> map(8);

  std::vector<RowContainerStateKey> keys;
  std::vector<char> values;
  keys.reserve(64);
  values.reserve(64);
  std::set<const char*> rows;
  bool caughtMidRehash = false;
  for (int64_t value = 0; value < 64 && !caughtMidRehash; ++value) {
    keys.push_back(lab.key(value));
    values.push_back(static_cast<char>('a' + value % 26));
    map.put(keys.back(), ns, &values.back());
    rows.insert(keys.back().row());
    auto snapshot = map.createSnapshot();
    // Mid-rehash capture: the old capacity plus rehashIndex, never a
    // power of two (steady and fully-rehashed tables are).
    const size_t numHeads = snapshot.heads.size();
    if (numHeads > 8 && (numHeads & (numHeads - 1)) != 0) {
      caughtMidRehash = true;
      size_t entryCount = 0;
      std::set<const char*> observed;
      for (const auto& head : snapshot.heads) {
        for (auto entry = head; entry != nullptr; entry = entry->next_) {
          ++entryCount;
          observed.insert(entry->key_.row());
        }
      }
      EXPECT_EQ(map.size(), entryCount);
      EXPECT_EQ(rows, observed);
      for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_EQ(&values[i], map.get(keys[i], ns)) << "lost key " << i;
      }
    }
    map.releaseSnapshot(snapshot.version);
  }
  ASSERT_TRUE(caughtMidRehash);
}

// The table buckets by key group over its sub-range; the bucket index is
// keyGroup - startKeyGroup, keys outside the range are rejected.
TEST_F(StateBackendTest, stateTableBucketsBySubRange) {
  KeyLab lab(pool());
  std::vector<RowContainerStateKey> keys;
  for (int64_t value = 0; value < 8; ++value) {
    keys.push_back(lab.key(value));
  }
  uint32_t minGroup = kMaxParallelism;
  uint32_t maxGroup = 0;
  for (const auto& key : keys) {
    minGroup = std::min<uint32_t>(minGroup, key.keyGroup());
    maxGroup = std::max<uint32_t>(maxGroup, key.keyGroup());
  }

  StateTable<RowContainerStateKey, VoidNamespace, char*> table(
      minGroup, maxGroup - minGroup + 1);
  EXPECT_EQ(minGroup, table.startKeyGroup());
  EXPECT_EQ(maxGroup - minGroup + 1, table.numKeyGroups());

  const VoidNamespace ns;
  std::vector<char> values(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    values[i] = static_cast<char>('0' + i);
    table.put(keys[i], ns, &values[i]);
  }
  EXPECT_EQ(keys.size(), table.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    EXPECT_EQ(&values[i], table.get(keys[i], ns));
  }

  // A key outside the sub-range must be rejected, not silently mapped.
  RowContainerStateKey outside = [&] {
    for (int64_t value = 100; value < 1000; ++value) {
      auto key = lab.key(value);
      if (key.keyGroup() < minGroup || key.keyGroup() > maxGroup) {
        return key;
      }
    }
    // unreachable with 128 key groups and 900 candidates
    return lab.key(0);
  }();
  ASSERT_TRUE(outside.keyGroup() < minGroup || outside.keyGroup() > maxGroup);
  EXPECT_THROW(table.put(outside, ns, nullptr), VeloxException);

  // Two keys in the same key group stay separate entries; clearing drops
  // all buckets.
  table.clear();
  EXPECT_EQ(0, table.size());
}

// Storage-level regression for the collision pair through the state table.
TEST_F(StateBackendTest, stateTableCollisionPairStaysApart) {
  KeyTable keyTable({BIGINT()}, kMaxParallelism, pool());
  KeySelector selector(keyTable, {0});
  auto keys =
      probeKeys(selector, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  ASSERT_EQ(3, keys.size());

  const VoidNamespace ns;
  StateTable<RowContainerStateKey, VoidNamespace, char*> table(
      0, kMaxParallelism);
  char valueA{'a'};
  char valueB{'b'};
  table.put(keys[0], ns, &valueA);
  table.put(keys[1], ns, &valueB);
  EXPECT_EQ(&valueA, table.get(keys[0], ns));
  EXPECT_EQ(&valueB, table.get(keys[1], ns));
  EXPECT_EQ(&valueA, table.get(keys[2], ns));
}

// AggregatingState: misses materialize + initialize fresh value rows, duplicate
// keys share a row, the collision pair accumulates independently, and the
// handle is idempotent per descriptor name. Accumulation runs through a real
// partial sum(BIGINT) aggregate over the accumulator-layout value rows.
TEST_F(StateBackendTest, backendAggregatingState) {
  SumAggregate sum;
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys =
      probeKeys(selector, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  ASSERT_EQ(3, keys.size());

  auto state = backend.getOrCreateAggregatingState<VoidNamespace>(
      sum.descriptor(pool()));
  sum.wire(state->valueRows());
  EXPECT_EQ(
      state.get(),
      backend.getOrCreateAggregatingState<VoidNamespace>(sum.descriptor(pool()))
          .get());

  std::vector<char*> rows(keys.size(), nullptr);
  state->rows(
      folly::Range<const RowContainerStateKey*>(keys.data(), keys.size()),
      VoidNamespace::instance(),
      rows.data());
  EXPECT_EQ(2, sum.initCount);
  EXPECT_EQ(rows[0], rows[2]);
  EXPECT_NE(rows[0], rows[1]);

  // Accumulate through the aggregate, as the operator's addRawInput would.
  // Note the explicit one-element vectors: a braced {N} literal would resolve
  // to the makeFlatVector<T>(size_t size) overload and build an N-element
  // uninitialized vector.
  sum.add(rows[0], makeNullableFlatVector<int64_t>({10}));
  sum.add(rows[1], makeNullableFlatVector<int64_t>({20}));
  sum.add(rows[2], makeNullableFlatVector<int64_t>({11}));

  // Fresh key instances resolve to the same rows with the accumulated
  // values; nothing crossed between the collision pair.
  auto readKeys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  std::vector<char*> readRows(readKeys.size(), nullptr);
  state->rows(
      folly::Range<const RowContainerStateKey*>(
          readKeys.data(), readKeys.size()),
      VoidNamespace::instance(),
      readRows.data());
  EXPECT_EQ(2, sum.initCount);
  EXPECT_EQ(rows[0], readRows[0]);
  EXPECT_EQ(rows[1], readRows[1]);
  EXPECT_EQ(21, sum.valueOf(readRows[0], pool()));
  EXPECT_EQ(20, sum.valueOf(readRows[1], pool()));
}

// The accumulator rows live in the state's value container, outside the
// state table: remove() returns the row to the container instead of only
// dropping the entry. RowContainer never shrinks its row count, so the
// observable is address reuse - the next miss lands on the freed row - and
// removing a key nobody stored is a no-op.
TEST_F(StateBackendTest, aggregatingStateRemoveReclaimsValueRow) {
  SumAggregate sum;
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  const VoidNamespace ns;

  auto state = backend.getOrCreateAggregatingState<VoidNamespace>(
      sum.descriptor(pool()));
  sum.wire(state->valueRows());
  auto* rowA = state->row(keys[0], ns);
  state->row(keys[1], ns);
  const auto rowsBefore = state->valueRows()->numRows();
  ASSERT_EQ(2, rowsBefore);
  ASSERT_EQ(2, sum.initCount);

  state->remove(probeKeys(selector, {999})[0], ns);
  EXPECT_EQ(rowsBefore, state->valueRows()->numRows());

  state->remove(keys[0], ns);
  auto* rebuilt = state->row(keys[0], ns);
  EXPECT_EQ(rowA, rebuilt);
  EXPECT_EQ(rowsBefore, state->valueRows()->numRows());
  EXPECT_EQ(3, sum.initCount);
}

// clear() drops every entry and returns every accumulator row to the value
// container: the rows allocated after the clear are served from the freed
// addresses and the container does not grow.
TEST_F(StateBackendTest, aggregatingStateClearReclaimsValueRows) {
  SumAggregate sum;
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  const VoidNamespace ns;

  auto state = backend.getOrCreateAggregatingState<VoidNamespace>(
      sum.descriptor(pool()));
  sum.wire(state->valueRows());
  auto* rowA = state->row(keys[0], ns);
  auto* rowB = state->row(keys[1], ns);
  const auto rowsBefore = state->valueRows()->numRows();
  ASSERT_EQ(2, rowsBefore);

  state->clear();
  auto* freshA = state->row(keys[0], ns);
  auto* freshB = state->row(keys[1], ns);
  EXPECT_TRUE(
      (freshA == rowA && freshB == rowB) || (freshA == rowB && freshB == rowA));
  EXPECT_EQ(rowsBefore, state->valueRows()->numRows());
}

// Restoring the same stream twice overwrites the earlier entries (Flink's
// last-write-wins on duplicate restores); the overwritten accumulator rows
// return to the value container instead of leaking, so the row count after
// the second restore equals the count after the first.
TEST_F(StateBackendTest, aggregatingStateRestoreOverwriteReleasesOldRow) {
  SumAggregate sumA;
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorA(backendA.keyIndex(), {0});
  auto keysA = probeKeys(selectorA, {kCollisionKeyA});
  const VoidNamespace ns;
  auto stateA = backendA.getOrCreateAggregatingState<VoidNamespace>(
      sumA.descriptor(pool()));
  sumA.wire(stateA->valueRows());
  sumA.add(stateA->row(keysA[0], ns), makeNullableFlatVector<int64_t>({7}));
  const auto bytes = backendA.snapshot();

  SumAggregate sumB;
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorB(backendB.keyIndex(), {0});
  auto stateB = backendB.getOrCreateAggregatingState<VoidNamespace>(
      sumB.descriptor(pool()));
  sumB.wire(stateB->valueRows());
  backendB.restore(bytes);
  const auto rowsAfterFirstRestore = stateB->valueRows()->numRows();
  ASSERT_EQ(1, rowsAfterFirstRestore);

  backendB.restore(bytes);
  EXPECT_EQ(rowsAfterFirstRestore, stateB->valueRows()->numRows());

  auto keysB = probeKeys(selectorB, {kCollisionKeyA});
  EXPECT_EQ(7, sumB.valueOf(stateB->row(keysB[0], ns), pool()));
}

// Value / List / Map states on the same backend: Flink read semantics
// (miss is default), overwrite on map put, and the collision pair stays
// independent in each.
TEST_F(StateBackendTest, backendValueListMapStates) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB, 999});
  const auto& keyA = keys[0];
  const auto& keyB = keys[1];
  const auto& keyC = keys[2];
  const VoidNamespace ns;

  auto valueState = backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto stored = std::make_shared<int64_t>(7);
  EXPECT_EQ(nullptr, valueState->value(keyA, ns));
  valueState->update(keyA, ns, stored);
  EXPECT_EQ(stored, valueState->value(keyA, ns));
  EXPECT_EQ(nullptr, valueState->value(keyB, ns));
  valueState->remove(keyA, ns);
  EXPECT_EQ(nullptr, valueState->value(keyA, ns));

  auto listState = backend.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  listState->add(keyA, ns, 1);
  listState->add(keyA, ns, 2);
  listState->add(keyB, ns, 3);
  EXPECT_EQ((std::vector<int64_t>{1, 2}), listState->get(keyA, ns));
  EXPECT_EQ((std::vector<int64_t>{3}), listState->get(keyB, ns));
  EXPECT_TRUE(listState->get(keyC, ns).empty());
  listState->remove(keyA, ns);
  EXPECT_TRUE(listState->get(keyA, ns).empty());

  auto mapState = backend.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>(
          "map", int64Serializer(), int64Serializer(), pool()));
  mapState->put(keyA, ns, 10, 100);
  mapState->put(keyA, ns, 10, 111);
  mapState->put(keyA, ns, 20, 200);
  mapState->put(keyB, ns, 30, 300);
  EXPECT_EQ(111, mapState->get(keyA, ns, 10));
  EXPECT_EQ(200, mapState->get(keyA, ns, 20));
  EXPECT_EQ(0, mapState->get(keyA, ns, 99));
  EXPECT_EQ(0, mapState->get(keyC, ns, 10));
  EXPECT_EQ(2, mapState->entries(keyA, ns).size());
  EXPECT_EQ(1, mapState->entries(keyB, ns).size());
  mapState->remove(keyA, ns, 10);
  EXPECT_EQ(0, mapState->get(keyA, ns, 10));
  EXPECT_EQ(1, mapState->entries(keyA, ns).size());
}

// A full checkpoint round trip through all four state kinds: backend A
// populates, backend B restores the stream into freshly registered states
// and reads the same entries back. The collision pair stays independent,
// aggregating entries restore through the same construct-then-merge sequence
// used to reload spilled aggregation state, and snapshotting unchanged
// state twice yields identical bytes.
TEST_F(StateBackendTest, snapshotRestoreRoundTrip) {
  SumAggregate sumA;
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorA(backendA.keyIndex(), {0});
  auto keysA =
      probeKeys(selectorA, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  ASSERT_EQ(3, keysA.size());

  auto aggregatingState = backendA.getOrCreateAggregatingState<VoidNamespace>(
      sumA.descriptor(pool()));
  sumA.wire(aggregatingState->valueRows());
  auto valueState = backendA.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto listState = backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  auto mapState = backendA.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>(
          "map", int64Serializer(), int64Serializer(), pool()));

  // Accumulate 21 / 20 over the collision pair through the aggregate, as
  // addRawInput would.
  std::vector<char*> rows(keysA.size(), nullptr);
  aggregatingState->rows(
      folly::Range<const RowContainerStateKey*>(keysA.data(), keysA.size()),
      VoidNamespace::instance(),
      rows.data());
  sumA.add(rows[0], makeNullableFlatVector<int64_t>({10}));
  sumA.add(rows[1], makeNullableFlatVector<int64_t>({20}));
  sumA.add(rows[2], makeNullableFlatVector<int64_t>({11}));
  EXPECT_EQ(2, sumA.initCount);

  const VoidNamespace ns;
  valueState->update(keysA[0], ns, std::make_shared<int64_t>(7));
  listState->add(keysA[0], ns, 1);
  listState->add(keysA[0], ns, 2);
  listState->add(keysA[1], ns, 3);
  mapState->put(keysA[0], ns, 10, 111);
  mapState->put(keysA[1], ns, 30, 300);

  const auto bytes = backendA.snapshot();
  EXPECT_EQ(bytes, backendA.snapshot());

  // Side B: a fresh backend over its own key container. Each restored
  // aggregating entry runs the init callback on a fresh row and merges the
  // intermediate payload in through the merge callback; a later lookup hits
  // the restored rows without further initialization.
  SumAggregate sumB;
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorB(backendB.keyIndex(), {0});
  auto keysB =
      probeKeys(selectorB, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  auto aggregatingStateB = backendB.getOrCreateAggregatingState<VoidNamespace>(
      sumB.descriptor(pool()));
  sumB.wire(aggregatingStateB->valueRows());
  auto valueStateB = backendB.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto listStateB = backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  auto mapStateB = backendB.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>(
          "map", int64Serializer(), int64Serializer(), pool()));
  backendB.restore(bytes);
  EXPECT_EQ(2, sumB.initCount);
  EXPECT_EQ(2, sumB.mergeCount);

  std::vector<char*> rowsB(keysB.size(), nullptr);
  aggregatingStateB->rows(
      folly::Range<const RowContainerStateKey*>(keysB.data(), keysB.size()),
      VoidNamespace::instance(),
      rowsB.data());
  EXPECT_EQ(2, sumB.initCount);
  EXPECT_EQ(21, sumB.valueOf(rowsB[0], pool()));
  EXPECT_EQ(20, sumB.valueOf(rowsB[1], pool()));
  EXPECT_EQ(rowsB[0], rowsB[2]);
  EXPECT_NE(rowsB[0], rowsB[1]);

  auto restoredValue = valueStateB->value(keysB[0], ns);
  ASSERT_NE(nullptr, restoredValue);
  EXPECT_EQ(7, *restoredValue);
  EXPECT_EQ(nullptr, valueStateB->value(keysB[1], ns));
  EXPECT_EQ((std::vector<int64_t>{1, 2}), listStateB->get(keysB[0], ns));
  EXPECT_EQ((std::vector<int64_t>{3}), listStateB->get(keysB[1], ns));
  EXPECT_EQ(111, mapStateB->get(keysB[0], ns, 10));
  EXPECT_EQ(0, mapStateB->get(keysB[0], ns, 99));
  EXPECT_EQ(300, mapStateB->get(keysB[1], ns, 30));
}

// sum + avg + min in one descriptor across a snapshot/restore round trip:
// the three accumulator columns keep descriptor order through the checkpoint
// frames and the merge dispatch (a mixed-up order would corrupt values or
// crash the decode), avg's row(double, bigint) intermediate round-trips the
// presto serde as a complex type, and an aggregate that saw only null input
// (kCollisionKeyB) restores with a null accumulator.
TEST_F(StateBackendTest, aggregatingStateMultipleAggregatesRoundTrip) {
  SumAvgMin aggsA;
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorA(backendA.keyIndex(), {0});
  auto keysA = probeKeys(selectorA, {kCollisionKeyA, kCollisionKeyB});
  ASSERT_EQ(2, keysA.size());

  auto stateA = backendA.getOrCreateAggregatingState<VoidNamespace>(
      aggsA.descriptor(pool()));
  aggsA.wire(stateA->valueRows());
  std::vector<char*> rows(keysA.size(), nullptr);
  stateA->rows(
      folly::Range<const RowContainerStateKey*>(keysA.data(), keysA.size()),
      VoidNamespace::instance(),
      rows.data());
  EXPECT_EQ(2, aggsA.initCount);

  aggsA.add(rows[0], makeNullableFlatVector<int64_t>({10}));
  aggsA.add(rows[0], makeNullableFlatVector<int64_t>({20}));
  // Only null input: the min accumulator stays null.
  aggsA.add(rows[1], makeNullableFlatVector<int64_t>({std::nullopt}));

  const auto bytes = backendA.snapshot();

  SumAvgMin aggsB;
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorB(backendB.keyIndex(), {0});
  auto keysB = probeKeys(selectorB, {kCollisionKeyA, kCollisionKeyB});
  auto stateB = backendB.getOrCreateAggregatingState<VoidNamespace>(
      aggsB.descriptor(pool()));
  aggsB.wire(stateB->valueRows());
  backendB.restore(bytes);
  EXPECT_EQ(2, aggsB.initCount);
  EXPECT_EQ(2, aggsB.mergeCount);

  std::vector<char*> rowsB(keysB.size(), nullptr);
  stateB->rows(
      folly::Range<const RowContainerStateKey*>(keysB.data(), keysB.size()),
      VoidNamespace::instance(),
      rowsB.data());
  // Restored rows are found without re-initialization.
  EXPECT_EQ(2, aggsB.initCount);

  EXPECT_EQ(30, aggsB.sumOf(rowsB[0], pool()));
  const auto [total, count] = aggsB.avgOf(rowsB[0], pool());
  EXPECT_EQ(30.0, total);
  EXPECT_EQ(2, count);
  EXPECT_EQ(10, aggsB.minOf(rowsB[0], pool()));

  EXPECT_TRUE(aggsB.minIsNull(rowsB[1], pool()));
}

// Checkpointing registered but empty states round-trips as an empty
// stream: restore leaves every state at its miss semantics.
TEST_F(StateBackendTest, snapshotRestoreEmptyStates) {
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  backendA.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  const auto bytes = backendA.snapshot();

  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorB(backendB.keyIndex(), {0});
  auto valueState = backendB.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto listState = backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  backendB.restore(bytes);
  EXPECT_EQ(bytes, backendB.snapshot());

  auto key = probeKeys(selectorB, {42})[0];
  const VoidNamespace ns;
  EXPECT_EQ(nullptr, valueState->value(key, ns));
  EXPECT_TRUE(listState->get(key, ns).empty());
}

// One descriptor name is one state: registering a second type under a used
// name fails instead of returning a mistyped handle.
TEST_F(StateBackendTest, backendRejectsNameReuseAcrossTypes) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "dup", sharedInt64Serializer(), pool()));
  EXPECT_THROW(
      backend.getOrCreateListState<VoidNamespace>(
          ListStateDescriptor<int64_t>("dup", int64Serializer(), pool())),
      VeloxException);
}

// The pre-generic interface of the base stays abstract-compatible but is
// not implemented on the heap backend until the operators migrate.
TEST_F(StateBackendTest, backendPreGenericInterfaceIsNyi) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeyedStateBackend& raw = backend;
  StateDescriptor descriptor("x");
  EXPECT_THROW(raw.getOrCreateValueState(descriptor), VeloxException);
  EXPECT_THROW(raw.createTimerService(nullptr), VeloxException);
}

// Out-of-range key-group configurations are rejected at construction.
TEST_F(StateBackendTest, backendRejectsInvalidKeyGroupRange) {
  EXPECT_THROW(
      (HeapKeyedStateBackend<RowContainerStateKey>(
          {BIGINT()}, kMaxParallelism, 100, kMaxParallelism, pool())),
      VeloxException);
  EXPECT_THROW(
      (HeapKeyedStateBackend<RowContainerStateKey>(
          {BIGINT()}, kMaxParallelism, 0, 0, pool())),
      VeloxException);
}

// Restoring a checkpoint that contains a state the target backend has not
// registered fails: one name is one state, and states must be registered
// before restore.
TEST_F(StateBackendTest, restoreRejectsUnknownState) {
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  backendA.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  const auto bytes = backendA.snapshot();

  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  EXPECT_THROW(backendB.restore(bytes), VeloxException);
}

// A checkpoint whose key-group range does not overlap the target backend's
// range is rejected by the header cross-check instead of silently
// mis-bucketing.
TEST_F(StateBackendTest, restoreRejectsKeyGroupRangeMismatch) {
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      {BIGINT()}, kMaxParallelism, 4, 2, pool());
  backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  const auto bytes = backendA.snapshot();

  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      {BIGINT()}, kMaxParallelism, 100, 28, pool());
  backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  EXPECT_THROW(backendB.restore(bytes), VeloxException);
}

// A header with a zero key-group count is corruption, not an empty range:
// [64, 64) inside the local range would otherwise sneak past the overlap
// check (an empty range overlaps nothing) and restore nothing.
TEST_F(StateBackendTest, restoreRejectsZeroKeyGroupCount) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  backend.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));

  std::string bytes;
  CheckpointWriter writer(bytes);
  CheckpointHeader header;
  header.flags = kCheckpointFlagLittleEndian;
  header.maxParallelism = kMaxParallelism;
  header.startKeyGroup = kMaxParallelism / 2;
  header.numKeyGroups = 0;
  writeCheckpointHeader(writer, header);
  EXPECT_THROW(backend.restore(bytes), VeloxException);
}

// A sub-range backend picks its own key groups out of a wider checkpoint,
// the shape of a scale-up restore: the in-range entries restore, the rest
// of the stream is skipped, and a key of a foreign key group is rejected
// by the state table instead of mis-bucketing.
TEST_F(StateBackendTest, restorePicksSubRange) {
  const std::vector<std::optional<int64_t>> inputs = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorA(backendA.keyIndex(), {0});
  auto keysA = probeKeys(selectorA, inputs);
  // Split the probed values by the half of the key-group space their keys
  // hash to, through the production grouping path.
  std::vector<size_t> low;
  std::vector<size_t> high;
  for (size_t i = 0; i < keysA.size() && (low.size() < 2 || high.size() < 2);
       ++i) {
    auto& half = keysA[i].keyGroup() < kMaxParallelism / 2 ? low : high;
    if (half.size() < 2) {
      half.push_back(i);
    }
  }
  ASSERT_EQ(2, low.size());
  ASSERT_EQ(2, high.size());

  auto valueStateA = backendA.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  const VoidNamespace ns;
  for (const auto i : low) {
    valueStateA->update(keysA[i], ns, std::make_shared<int64_t>(*inputs[i]));
  }
  for (const auto i : high) {
    valueStateA->update(keysA[i], ns, std::make_shared<int64_t>(*inputs[i]));
  }
  const auto bytes = backendA.snapshot();

  // The upper half as its own backend, as a doubled parallelism would cut it.
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      {BIGINT()}, kMaxParallelism, 64, 64, pool());
  KeySelector selectorB(backendB.keyIndex(), {0});
  auto keysB = probeKeys(selectorB, inputs);
  auto valueStateB = backendB.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  backendB.restore(bytes);

  for (const auto i : high) {
    auto restored = valueStateB->value(keysB[i], ns);
    ASSERT_NE(nullptr, restored);
    EXPECT_EQ(*inputs[i], *restored);
  }
  for (const auto i : low) {
    EXPECT_THROW(valueStateB->value(keysB[i], ns), VeloxException);
  }
}

// A wider backend restores two disjoint-range streams in sequence, the
// shape of a scale-down restore; the halves accumulate into one backend.
TEST_F(StateBackendTest, restoreAccumulatesStreams) {
  const std::vector<std::optional<int64_t>> inputs = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  // Two backends over the two halves of the key-group space, as a halved
  // parallelism would cut it; each owns its key table.
  HeapKeyedStateBackend<RowContainerStateKey> backendLow(
      {BIGINT()}, kMaxParallelism, 0, 64, pool());
  KeySelector selectorLow(backendLow.keyIndex(), {0});
  auto keysLow = probeKeys(selectorLow, inputs);
  HeapKeyedStateBackend<RowContainerStateKey> backendHigh(
      {BIGINT()}, kMaxParallelism, 64, 64, pool());
  KeySelector selectorHigh(backendHigh.keyIndex(), {0});
  auto keysHigh = probeKeys(selectorHigh, inputs);
  // Split the probed values by the half of the key-group space their keys
  // hash to, through the production grouping path.
  std::vector<size_t> low;
  std::vector<size_t> high;
  for (size_t i = 0; i < keysLow.size() && (low.size() < 2 || high.size() < 2);
       ++i) {
    auto& half = keysLow[i].keyGroup() < kMaxParallelism / 2 ? low : high;
    if (half.size() < 2) {
      half.push_back(i);
    }
  }
  ASSERT_EQ(2, low.size());
  ASSERT_EQ(2, high.size());

  auto valueStateLow = backendLow.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto valueStateHigh = backendHigh.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  const VoidNamespace ns;
  for (const auto i : low) {
    valueStateLow->update(
        keysLow[i], ns, std::make_shared<int64_t>(*inputs[i]));
  }
  for (const auto i : high) {
    valueStateHigh->update(
        keysHigh[i], ns, std::make_shared<int64_t>(*inputs[i]));
  }
  const auto bytesLow = backendLow.snapshot();
  const auto bytesHigh = backendHigh.snapshot();

  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorB(backendB.keyIndex(), {0});
  auto keysB = probeKeys(selectorB, inputs);
  auto valueStateB = backendB.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  backendB.restore(bytesLow);
  backendB.restore(bytesHigh);

  for (const auto i : low) {
    auto restored = valueStateB->value(keysB[i], ns);
    ASSERT_NE(nullptr, restored);
    EXPECT_EQ(*inputs[i], *restored);
  }
  for (const auto i : high) {
    auto restored = valueStateB->value(keysB[i], ns);
    ASSERT_NE(nullptr, restored);
    EXPECT_EQ(*inputs[i], *restored);
  }
}

// Restore probes the backend's key table through its private door: a
// selector bound to the same table keeps its cached probe results and its
// newGroups clean, and a restored key resolves to the very row a runtime
// probe resolves to.
TEST_F(StateBackendTest, restoreDoesNotDisturbBoundSelectors) {
  const VoidNamespace ns;

  HeapKeyedStateBackend<RowContainerStateKey> backendX(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorX(backendX.keyIndex(), {0});
  auto keysX = probeKeys(selectorX, {42});
  auto listX = backendX.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  listX->add(keysX[0], ns, 7);
  const auto bytes = backendX.snapshot();

  HeapKeyedStateBackend<RowContainerStateKey> backendY(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorY(backendY.keyIndex(), {0});
  auto keysY = probeKeys(selectorY, {7});
  ASSERT_EQ(1, selectorY.newGroups().size());
  auto listY = backendY.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));

  // Restore lands key 42 in backendY's own table; selectorY's cached
  // results of its last probe were not touched by it.
  backendY.restore(bytes);
  EXPECT_EQ(1, selectorY.keys().size());
  EXPECT_EQ(1, selectorY.newGroups().size());
  EXPECT_EQ(1, selectorY.distinctKeys().size());
  EXPECT_EQ(keysY[0].row(), selectorY.keys()[0].row());

  // A later probe sees the restored key as an existing row, and the
  // restored entry is reachable through it.
  auto rekeys = probeKeys(selectorY, {42, 7});
  EXPECT_TRUE(selectorY.newGroups().empty());
  EXPECT_EQ((std::vector<int64_t>{7}), listY->get(rekeys[0], ns));
}

// The handler's checkpoint plane tolerates a backend that is not created
// (the factory returns null until the operators own typed backends).
TEST_F(StateBackendTest, handlerToleratesNullBackend) {
  StreamOperatorStateHandler handler(1, nullptr);
  handler.snapshotState(1);
  handler.notifyCheckpointComplete(1);
  handler.notifyCheckpointAborted(1);
}

// Backend parameters round-trip; plans written before the key-group fields
// existed deserialize with the defaults.
TEST_F(StateBackendTest, backendParametersRoundTrip) {
  auto parameters = std::make_shared<const KeyedStateBackendParameters>(
      StateBackendType::HEAP, "job", "op", 256, 10, 20);
  auto restored =
      KeyedStateBackendParameters::create(parameters->serialize(), nullptr);
  ASSERT_NE(nullptr, restored);
  EXPECT_EQ(StateBackendType::HEAP, restored->getBackendType());
  EXPECT_EQ("job", restored->getJobId());
  EXPECT_EQ("op", restored->getOperatorIdentifier());
  EXPECT_EQ(256, restored->getMaxParallelism());
  EXPECT_EQ(10, restored->getStartKeyGroup());
  EXPECT_EQ(20, restored->getNumKeyGroups());

  folly::dynamic legacy = folly::dynamic::object;
  legacy["jobId"] = "job";
  legacy["operatorId"] = "op";
  legacy["stateBackendType"] = static_cast<int32_t>(StateBackendType::HEAP);
  auto legacyRestored = KeyedStateBackendParameters::create(legacy, nullptr);
  ASSERT_NE(nullptr, legacyRestored);
  EXPECT_EQ(128, legacyRestored->getMaxParallelism());
  EXPECT_EQ(0, legacyRestored->getStartKeyGroup());
  EXPECT_EQ(128, legacyRestored->getNumKeyGroups());
}

// The keyed-state construction path an operator's initializeStateBackend
// runs: the plan contributes pure configuration (the KeyedStateSpec - key
// channels and key types), the task's state backend contributes the
// key-group numbers, and the keyed backend and its KeySelector are built
// at initialize time, in that order, from those two sources. The nine V1
// operators keep their plan-time partition() selectors until they migrate.
TEST_F(StateBackendTest, keyedStateBuiltAtInitializeFromSpecAndParameters) {
  const auto spec = std::make_shared<const KeyedStateSpec>(
      std::vector<column_index_t>{0}, std::vector<TypePtr>{BIGINT()});

  // The task-level backend, as StatefulTask::initStateBackend builds it.
  // The key-group configuration lives here and nowhere else.
  HashMapStateBackend taskBackend(
      std::make_shared<const KeyedStateBackendParameters>(
          StateBackendType::HEAP,
          "job",
          "op",
          kMaxParallelism,
          0,
          kMaxParallelism));
  const auto& parameters = taskBackend.parameters();

  // The operator side, in construction order: the keyed backend first
  // (the spec's key types, the parameters' key-group range), then a
  // selector bound to the backend's key table over the spec's channels.
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      spec->keyTypes(),
      parameters->getMaxParallelism(),
      parameters->getStartKeyGroup(),
      parameters->getNumKeyGroups(),
      pool());
  KeySelector selector(backend.keyIndex(), spec->keyChannels());

  // One source: maxParallelism exists only in the parameters; the
  // backend and its key table carry that number, and the selector keeps
  // no copy of its own - every key it hands out is built from the bound
  // table's value.
  EXPECT_EQ(parameters->getMaxParallelism(), backend.maxParallelism());
  EXPECT_EQ(
      parameters->getMaxParallelism(), backend.keyIndex().maxParallelism());
  // The selector probes the backend's own key table.
  EXPECT_EQ(backend.keyIndex().keyRows(), selector.keyRowContainer());

  auto keys =
      probeKeys(selector, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  ASSERT_EQ(3, keys.size());
  EXPECT_EQ(keys[0].row(), keys[2].row());
  EXPECT_NE(keys[0].row(), keys[1].row());
}

// The keyed-state spec is plan data: it round-trips through the serde
// with its key channels and key column types intact.
TEST_F(StateBackendTest, keyedStateSpecRoundTrip) {
  const KeyedStateSpec spec(
      std::vector<column_index_t>{0, 2},
      std::vector<TypePtr>{BIGINT(), VARCHAR()});
  const auto restored = KeyedStateSpec::create(spec.serialize(), nullptr);
  ASSERT_EQ(2, restored->keyChannels().size());
  EXPECT_EQ(0, restored->keyChannels()[0]);
  EXPECT_EQ(2, restored->keyChannels()[1]);
  ASSERT_EQ(2, restored->keyTypes().size());
  EXPECT_TRUE(spec.keyTypes()[0]->equivalent(*restored->keyTypes()[0]));
  EXPECT_TRUE(spec.keyTypes()[1]->equivalent(*restored->keyTypes()[1]));

  // A spec whose channels and types disagree is rejected at construction,
  // not midway through a probe.
  EXPECT_THROW(
      (KeyedStateSpec(
          std::vector<column_index_t>{0, 1}, std::vector<TypePtr>{BIGINT()})),
      VeloxException);
}

// Removing the last user entry of a map drops the stored map itself: the
// checkpoint bytes equal those of a backend that never stored a map for
// that key, so empty maps never reach a checkpoint.
TEST_F(StateBackendTest, mapRemoveLastUserEntryDropsStoredMap) {
  const VoidNamespace ns;

  HeapKeyedStateBackend<RowContainerStateKey> backendX(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorX(backendX.keyIndex(), {0});
  auto keysX = probeKeys(selectorX, {kCollisionKeyA, kCollisionKeyB});
  auto mapX = backendX.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>(
          "map", int64Serializer(), int64Serializer(), pool()));
  mapX->put(keysX[0], ns, 10, 100);
  mapX->put(keysX[1], ns, 30, 300);
  mapX->remove(keysX[0], ns, 10);

  HeapKeyedStateBackend<RowContainerStateKey> backendY(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorY(backendY.keyIndex(), {0});
  auto keysY = probeKeys(selectorY, {kCollisionKeyA, kCollisionKeyB});
  auto mapY = backendY.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>(
          "map", int64Serializer(), int64Serializer(), pool()));
  mapY->put(keysY[1], ns, 30, 300);

  EXPECT_EQ(backendY.snapshot(), backendX.snapshot());
}

// A key whose entries are all removed is physically reclaimed at the batch
// boundary: the next probe reports it as a new group again, while a key that
// still holds an entry survives the boundary. (RowContainer never shrinks
// its row count; the observable for the physical erase is the new group on
// re-probe, and the address reuse in the probeBorn test below.)
TEST_F(StateBackendTest, advanceReclaimsRemovedKeyRows) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  const VoidNamespace ns;
  auto valueState = backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto stored = std::make_shared<int64_t>(7);
  valueState->update(keys[0], ns, stored);
  valueState->update(keys[1], ns, stored);
  valueState->remove(keys[0], ns);

  backend.advance();

  // Key A (input row 0) lost its only entry and was erased; key B kept its
  // entry. The state of B survives, A reads back empty over its fresh row.
  auto rekeys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  ASSERT_EQ(1, selector.newGroups().size());
  EXPECT_EQ(0, selector.newGroups()[0]);
  EXPECT_EQ(stored, valueState->value(rekeys[1], ns));
  EXPECT_EQ(nullptr, valueState->value(rekeys[0], ns));
}

// A key row born at a probe but never written to any state is reclaimable at
// the next boundary, exactly like a key whose entries were all removed. The
// freed row is recycled through the container free list: the next distinct
// key lands at the same address and the allocated row count does not grow.
TEST_F(StateBackendTest, probeBornKeyNeverWrittenIsReclaimed) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA});
  const auto* erasedRow = keys[0].row();
  const auto numRowsBefore = backend.keyIndex().keyRows()->numRows();

  backend.advance();

  auto rekeys = probeKeys(selector, {kCollisionKeyA});
  EXPECT_EQ(1, selector.newGroups().size());
  EXPECT_EQ(erasedRow, rekeys[0].row());
  EXPECT_EQ(numRowsBefore, backend.keyIndex().keyRows()->numRows());
}

// A key that goes to zero and back within one batch (entry removed, then
// re-added before the boundary) survives advance(): the dirty list is
// re-verified, not blindly erased.
TEST_F(StateBackendTest, removeAndReAddWithinBatchKeepsKey) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA});
  const VoidNamespace ns;
  auto valueState = backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  valueState->update(keys[0], ns, std::make_shared<int64_t>(7));
  valueState->remove(keys[0], ns);
  auto rewritten = std::make_shared<int64_t>(9);
  valueState->update(keys[0], ns, rewritten);

  backend.advance();

  auto rekeys = probeKeys(selector, {kCollisionKeyA});
  EXPECT_TRUE(selector.newGroups().empty());
  EXPECT_EQ(rewritten, valueState->value(rekeys[0], ns));
}

// A key removed in one batch and re-added in the next gets a fresh row with
// a fresh refcount: after the re-add the row again survives boundaries.
TEST_F(StateBackendTest, removedKeyReAddedNextBatchRebuildsRow) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA});
  const VoidNamespace ns;
  auto valueState = backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  valueState->update(keys[0], ns, std::make_shared<int64_t>(7));
  valueState->remove(keys[0], ns);
  backend.advance();

  auto rekeys = probeKeys(selector, {kCollisionKeyA});
  ASSERT_EQ(1, selector.newGroups().size());
  auto rewritten = std::make_shared<int64_t>(9);
  valueState->update(rekeys[0], ns, rewritten);
  backend.advance();

  auto rekeys2 = probeKeys(selector, {kCollisionKeyA});
  EXPECT_TRUE(selector.newGroups().empty());
  EXPECT_EQ(rewritten, valueState->value(rekeys2[0], ns));
}

// The refcount spans all states of the backend: a key with entries in two
// states is reclaimed only after both are gone.
TEST_F(StateBackendTest, keyWithEntriesInTwoStatesReclaimsWhenAllRemoved) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA});
  const VoidNamespace ns;
  auto valueState = backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto mapState = backend.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>(
          "map", int64Serializer(), int64Serializer(), pool()));
  valueState->update(keys[0], ns, std::make_shared<int64_t>(7));
  mapState->put(keys[0], ns, 10, 100);

  valueState->remove(keys[0], ns);
  backend.advance();
  auto rekeys = probeKeys(selector, {kCollisionKeyA});
  EXPECT_TRUE(selector.newGroups().empty());

  // Removing the last user entry drops the map's table entry (the commit-2
  // behavior); that was the key's last reference.
  mapState->remove(rekeys[0], ns, 10);
  backend.advance();
  auto rekeys2 = probeKeys(selector, {kCollisionKeyA});
  EXPECT_EQ(1, selector.newGroups().size());
}

// snapshot() runs the reclamation first: dead key rows never sit across a
// checkpoint barrier. The key whose entry survives the snapshot keeps its
// row; the key whose entry was removed before the snapshot is gone.
TEST_F(StateBackendTest, snapshotReclaimsDeadKeyRowsFirst) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  const VoidNamespace ns;
  auto valueState = backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto stored = std::make_shared<int64_t>(7);
  valueState->update(keys[0], ns, stored);
  valueState->update(keys[1], ns, stored);
  valueState->remove(keys[0], ns);

  backend.snapshot();

  auto rekeys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  ASSERT_EQ(1, selector.newGroups().size());
  EXPECT_EQ(0, selector.newGroups()[0]);
  EXPECT_EQ(stored, valueState->value(rekeys[1], ns));
}

// Entries restored from a checkpoint count toward the key refcount like any
// other entry: the restore-born row survives advance() with its entry, and
// only removing the restored entry retires the row.
TEST_F(StateBackendTest, restoredEntriesCountTowardKeyRefcount) {
  HeapKeyedStateBackend<RowContainerStateKey> backendX(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selectorX(backendX.keyIndex(), {0});
  auto keysX = probeKeys(selectorX, {kCollisionKeyA});
  const VoidNamespace ns;
  auto valueStateX = backendX.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto stored = std::make_shared<int64_t>(7);
  valueStateX->update(keysX[0], ns, stored);
  const auto bytes = backendX.snapshot();

  // Side B never probes before restore; the restored key row is born through
  // the restore door and its refcount comes from the restored entry alone.
  HeapKeyedStateBackend<RowContainerStateKey> backendY(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  auto valueStateY = backendY.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  backendY.restore(bytes);
  backendY.advance();

  KeySelector selectorY(backendY.keyIndex(), {0});
  auto keysY = probeKeys(selectorY, {kCollisionKeyA});
  EXPECT_TRUE(selectorY.newGroups().empty());
  auto restored = valueStateY->value(keysY[0], ns);
  ASSERT_NE(nullptr, restored);
  EXPECT_EQ(*stored, *restored);

  valueStateY->remove(keysY[0], ns);
  backendY.advance();
  auto rekeysY = probeKeys(selectorY, {kCollisionKeyA});
  EXPECT_EQ(1, selectorY.newGroups().size());
}

// Many add/remove cycles pile the same row up in the dirty list (its birth
// plus every zero transition); one advance() must deduplicate before
// erasing - erasing twice would be a double free - and a second advance()
// over the emptied list is a no-op.
TEST_F(StateBackendTest, repeatedRemoveAddCyclesAdvanceOnceSafely) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      {BIGINT()}, kMaxParallelism, 0, kMaxParallelism, pool());
  KeySelector selector(backend.keyIndex(), {0});
  auto keys = probeKeys(selector, {kCollisionKeyA});
  const VoidNamespace ns;
  auto valueState = backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  for (int i = 0; i < 5; ++i) {
    valueState->update(keys[0], ns, std::make_shared<int64_t>(i));
    valueState->remove(keys[0], ns);
  }

  backend.advance();
  auto rekeys = probeKeys(selector, {kCollisionKeyA});
  EXPECT_EQ(1, selector.newGroups().size());

  backend.advance();
  auto rekeys2 = probeKeys(selector, {kCollisionKeyA});
  EXPECT_EQ(1, selector.newGroups().size());
}

// The refcount is guarded: removing an entry that was never added underflows
// and fails loudly instead of silently corrupting the count.
TEST_F(StateBackendTest, keyRefcountUnderflowFailsLoud) {
  KeyTable table({BIGINT()}, kMaxParallelism, pool());
  auto key = table.probeKey({makeNullableFlatVector<int64_t>({5})});
  EXPECT_THROW(table.onEntryRemoved(key), VeloxException);
}

// The key serializer rejects unsupported key column types at construction
// instead of midway through a serialization.
TEST_F(StateBackendTest, serializerRejectsUnsupportedKeyType) {
  exec::RowContainer container({ARRAY(BIGINT())}, pool());
  RowContainerKeySchema schema(&container, {ARRAY(BIGINT())});
  EXPECT_THROW(
      (RowContainerStateKeySerializer(&schema, pool(), nullptr)),
      VeloxException);
}

// A corrupted shared-value null flag fails loudly instead of silently
// deserializing to null.
TEST_F(StateBackendTest, sharedValueCorruptNullFlagFailsLoud) {
  SharedPtrSerializer<int64_t> serializer(
      std::make_shared<ValueSerializer<int64_t>>());

  EXPECT_EQ(nullptr, serializer.deserialize(std::string(1, '\x00')));

  std::string payload(1, '\x01');
  const int64_t value = 7;
  payload.append(reinterpret_cast<const char*>(&value), sizeof(value));
  auto restored = serializer.deserialize(payload);
  ASSERT_NE(nullptr, restored);
  EXPECT_EQ(7, *restored);

  EXPECT_THROW(serializer.deserialize(std::string(1, '\x02')), VeloxException);
}

} // namespace
} // namespace facebook::velox::stateful
