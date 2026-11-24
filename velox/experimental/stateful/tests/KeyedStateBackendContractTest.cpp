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
// The contract every keyed state backend honors, run against each backend:
// probing and the row handles of AggregatingState, commit() and reclaim(),
// the checkpoint stream through any CheckpointSink / CheckpointSource, and
// key values read back through extractKeyColumn(). The tests use the
// backend only through this surface - no backend type appears outside the
// type list - so a new backend plugs in by being added to the list.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <folly/Range.h>

#include "velox/common/memory/MemoryPool.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/Aggregate.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/state/CheckpointStream.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/KeyedStateBackend.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/experimental/stateful/tests/ByteBufferKeyedStateBackend.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace facebook::velox::stateful {

// The backends the contract runs against: the heap storage, which commits
// in place, and the test-only byte-buffer storage, which commits through a
// write-back step and so makes the commit() rules observable.
using BackendTypes = ::testing::Types<
    HeapKeyedStateBackend<RowContainerStateKey>,
    ByteBufferKeyedStateBackend>;

// The memory manager is set once per process: the typed suite below is
// instantiated once per backend type and each instantiation runs
// SetUpTestSuite, but replacing the manager would pull it from under the
// pools of the other instantiation.
void ensureMemoryManager() {
  static const bool initialized = [] {
    memory::MemoryManager::testingSetInstance({});
    return true;
  }();
  (void)initialized;
}

constexpr int64_t kCollisionKeyA = 391;
constexpr int64_t kCollisionKeyB = 32728;
constexpr uint32_t kMaxParallelism = 128;

// A real partial sum(BIGINT) aggregate: the descriptor carries the function
// next to its accumulator, the state wires the function to its rows at
// creation, and the counter records init callback runs. Declared before the
// backend it feeds and outliving it: the closures capture 'function' by raw
// pointer.
struct SumAggregate {
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

  AggregatingStateDescriptor descriptor(memory::MemoryPool* pool) {
    return AggregatingStateDescriptor(
        "acc",
        {function.get()},
        {exec::Accumulator(function.get(), BIGINT())},
        [this](char** rows, size_t numRows) {
          initCount += numRows;
          std::vector<vector_size_t> indices(numRows);
          for (size_t i = 0; i < numRows; ++i) {
            indices[i] = static_cast<vector_size_t>(i);
          }
          function->initializeNewGroups(
              rows,
              folly::Range<const vector_size_t*>(
                  indices.data(), indices.size()));
        },
        [this](char* row, const std::vector<VectorPtr>& columns) {
          function->addSingleGroupIntermediateResults(
              row, SelectivityVector(1), columns, false);
        },
        pool);
  }

  // Adds 'input' (a one-row BIGINT vector) to 'row' as the operator's
  // addRawInput would, through the handle alone.
  void add(char* row, const VectorPtr& input) {
    function->addSingleGroupRawInput(row, SelectivityVector(1), {input}, false);
  }

  // The running total of 'row'.
  int64_t valueOf(char* row, memory::MemoryPool* pool) {
    char* groups[1] = {row};
    VectorPtr result = BaseVector::create(BIGINT(), 1, pool);
    function->extractAccumulators(groups, 1, &result);
    return result->asFlatVector<int64_t>()->valueAt(0);
  }
};

// A CheckpointSink that keeps every append as its own piece: the test sees
// how the backend handed the stream over.
class RecordingSink : public CheckpointSink {
 public:
  void append(std::string_view bytes) override {
    appends_.emplace_back(bytes);
  }

  const std::vector<std::string>& appends() const {
    return appends_;
  }

  std::string bytes() const {
    std::string all;
    for (const auto& piece : appends_) {
      all += piece;
    }
    return all;
  }

 private:
  std::vector<std::string> appends_;
};

// A CheckpointSource that delivers its bytes in pieces no larger than
// 'chunkSize', whatever the backend asks for: restore must take the stream
// piece by piece.
class ChunkingSource : public CheckpointSource {
 public:
  ChunkingSource(std::string_view bytes, size_t chunkSize)
      : bytes_(bytes), chunkSize_(chunkSize) {}

  std::string_view next(size_t maxSize) override {
    const size_t length =
        std::min({maxSize, chunkSize_, bytes_.size() - offset_});
    const std::string_view chunk(bytes_.data() + offset_, length);
    offset_ += length;
    ++reads_;
    return chunk;
  }

  size_t offset() const {
    return offset_;
  }

  size_t reads() const {
    return reads_;
  }

 private:
  const std::string_view bytes_;
  const size_t chunkSize_;
  size_t offset_{0};
  size_t reads_{0};
};

template <typename B>
class KeyedStateBackendContractTest : public ::testing::TestWithParam<B>,
                                      public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    ensureMemoryManager();
  }

  // A backend owning the whole key-group range.
  std::unique_ptr<B> makeBackend() {
    return makeBackend(0, kMaxParallelism);
  }

  // A backend owning the key groups [startKeyGroup, startKeyGroup +
  // numKeyGroups) only, as a subtask after a rescale.
  std::unique_ptr<B> makeBackend(
      uint32_t startKeyGroup,
      uint32_t numKeyGroups) {
    return std::make_unique<B>(
        std::vector<TypePtr>{BIGINT()},
        kMaxParallelism,
        startKeyGroup,
        numKeyGroups,
        pool());
  }

  // Probes 'values' through the production grouping path and returns the
  // resulting per-row state keys.
  std::vector<RowContainerStateKey> probeKeys(
      KeySelector& selector,
      const std::vector<int64_t>& values) {
    selector.probe(
        this->makeRowVector({this->template makeFlatVector<int64_t>(values)}));
    const auto keys = selector.keys();
    return std::vector<RowContainerStateKey>(keys.begin(), keys.end());
  }

  static folly::Range<const RowContainerStateKey*> range(
      const std::vector<RowContainerStateKey>& keys) {
    return folly::Range<const RowContainerStateKey*>(keys.data(), keys.size());
  }

  // A one-row BIGINT vector holding 'value'. The explicit vector keeps the
  // braced literal off the makeFlatVector<T>(size_t size) overload, which
  // would build an N-element uninitialized vector instead.
  VectorPtr one(int64_t value) {
    return this->template makeFlatVector<int64_t>(std::vector<int64_t>{value});
  }

  FlatVectorPtr<int64_t> int64s(std::vector<int64_t> values) {
    return this->template makeFlatVector<int64_t>(std::move(values));
  }

  // The handles of 'keys' from 'state', in key order.
  std::vector<char*> handles(
      const std::shared_ptr<AggregatingState<RowContainerStateKey>>& state,
      const std::vector<RowContainerStateKey>& keys) {
    std::vector<char*> rows(keys.size(), nullptr);
    state->rows(range(keys), VoidNamespace::instance(), rows.data());
    return rows;
  }

  const VoidNamespace ns = VoidNamespace::instance();
};

TYPED_TEST_SUITE(KeyedStateBackendContractTest, BackendTypes);

// rows() hands out one handle per key - equal keys share it, the collision
// pair does not - initialized through the descriptor's callback, with the
// aggregate functions already wired by the state: the test adds through
// the handles alone and reads the running totals back.
TYPED_TEST(KeyedStateBackendContractTest, rowsHandsOutWiredHandles) {
  SumAggregate sum;
  auto backend = this->makeBackend();
  KeySelector selector(backend->keyIndex(), {0});
  const auto keys = this->probeKeys(
      selector, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  auto state = backend->getOrCreateAggregatingState(
      sum.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());
  const auto rows = this->handles(state, keys);

  ASSERT_EQ(3, rows.size());
  EXPECT_EQ(rows[0], rows[2]);
  EXPECT_NE(rows[0], rows[1]);
  EXPECT_EQ(2, sum.initCount);

  sum.add(rows[0], this->one(7));
  sum.add(rows[0], this->one(5));
  sum.add(rows[1], this->one(9));
  EXPECT_EQ(12, sum.valueOf(rows[0], this->pool()));
  EXPECT_EQ(9, sum.valueOf(rows[1], this->pool()));

  // The single-key entry point lands on the same handle.
  EXPECT_EQ(rows[0], state->row(keys[2], this->ns));
  // A second call with the same descriptor returns the same state.
  EXPECT_EQ(
      state,
      backend->getOrCreateAggregatingState(
          sum.descriptor(this->pool()),
          NamespaceSerializerTraits<VoidNamespace>::create()));
}

// commit() hands the state to storage, and the operator re-fetches its
// handles afterwards instead of holding the old ones across it (a
// write-back backend drops them). reclaim() then keeps every key an entry
// refers to.
TYPED_TEST(
    KeyedStateBackendContractTest,
    commitKeepsEntriesAndReclaimKeepsTheirKeys) {
  SumAggregate sum;
  auto backend = this->makeBackend();
  KeySelector selector(backend->keyIndex(), {0});
  const auto keys = this->probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  auto state = backend->getOrCreateAggregatingState(
      sum.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());
  const auto rows = this->handles(state, keys);
  sum.add(rows[0], this->one(7));
  sum.add(rows[1], this->one(9));

  backend->commit();

  // Fetched anew after the commit: the committed values.
  const auto again = this->handles(state, keys);
  EXPECT_EQ(7, sum.valueOf(again[0], this->pool()));
  EXPECT_EQ(9, sum.valueOf(again[1], this->pool()));

  backend->reclaim();

  // Both keys still have their entries: a re-probe finds them in the key
  // table and reports no new group.
  const auto rekeys =
      this->probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  EXPECT_TRUE(selector.newGroups().empty());
  EXPECT_TRUE(rekeys[0].equals(keys[0]));
  EXPECT_TRUE(rekeys[1].equals(keys[1]));
  // Removing an entry and reclaiming retires its key; the other stays.
  state->remove(rekeys[0], this->ns);
  backend->reclaim();
  const auto last = this->probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  ASSERT_EQ(1, selector.newGroups().size());
  EXPECT_EQ(0, selector.newGroups()[0]);
  EXPECT_TRUE(last[1].equals(keys[1]));
}

// reclaim() drops a key no entry refers to: a probed key that was never
// written is gone after it, and comes back as a new group when probed
// again.
TYPED_TEST(KeyedStateBackendContractTest, reclaimDropsUnwrittenKeys) {
  auto backend = this->makeBackend();
  KeySelector selector(backend->keyIndex(), {0});
  this->probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  ASSERT_EQ(2, selector.newGroups().size());

  backend->reclaim();

  // Both keys are gone: probed again, each is a new group.
  this->probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  ASSERT_EQ(2, selector.newGroups().size());
  EXPECT_EQ(0, selector.newGroups()[0]);
  EXPECT_EQ(1, selector.newGroups()[1]);

  // Still unwritten, they are dropped again at the next boundary: probing
  // alone never pins a key past a reclaim().
  backend->reclaim();
  this->probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  EXPECT_EQ(2, selector.newGroups().size());
}

// snapshot() goes through any CheckpointSink and commits first (the test
// never calls commit() itself); restore() reads the stream back through a
// CheckpointSource that delivers it in small pieces, into a fresh backend
// of the same kind, and the entries arrive with their values.
TYPED_TEST(
    KeyedStateBackendContractTest,
    checkpointStreamsThroughSinkAndSource) {
  SumAggregate sumA;
  auto backendA = this->makeBackend();
  KeySelector selectorA(backendA->keyIndex(), {0});
  const auto keysA =
      this->probeKeys(selectorA, {kCollisionKeyA, kCollisionKeyB});
  auto stateA = backendA->getOrCreateAggregatingState(
      sumA.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());
  const auto rowsA = this->handles(stateA, keysA);
  sumA.add(rowsA[0], this->one(7));
  sumA.add(rowsA[1], this->one(9));

  RecordingSink sink;
  backendA->snapshot(sink);
  const auto bytes = sink.bytes();
  EXPECT_FALSE(bytes.empty());
  EXPECT_GE(sink.appends().size(), 1);
  // The in-memory convenience writes the same stream: same entries, same
  // order, so the bytes are identical.
  EXPECT_EQ(bytes, backendA->snapshotBytes());

  SumAggregate sumB;
  auto backendB = this->makeBackend();
  KeySelector selectorB(backendB->keyIndex(), {0});
  auto stateB = backendB->getOrCreateAggregatingState(
      sumB.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());

  ChunkingSource source(bytes, 7);
  backendB->restore(source);
  EXPECT_EQ(bytes.size(), source.offset());
  EXPECT_GT(source.reads(), 1);

  // The restored keys sit in the key table (no new group on probe), and
  // the handles carry the committed values.
  const auto keysB =
      this->probeKeys(selectorB, {kCollisionKeyA, kCollisionKeyB});
  EXPECT_TRUE(selectorB.newGroups().empty());
  const auto rowsB = this->handles(stateB, keysB);
  EXPECT_EQ(7, sumB.valueOf(rowsB[0], this->pool()));
  EXPECT_EQ(9, sumB.valueOf(rowsB[1], this->pool()));

  // restoreBytes() takes the same stream through the convenience.
  SumAggregate sumC;
  auto backendC = this->makeBackend();
  KeySelector selectorC(backendC->keyIndex(), {0});
  auto stateC = backendC->getOrCreateAggregatingState(
      sumC.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());
  backendC->restoreBytes(bytes);
  const auto keysC =
      this->probeKeys(selectorC, {kCollisionKeyA, kCollisionKeyB});
  const auto rowsC = this->handles(stateC, keysC);
  EXPECT_EQ(7, sumC.valueOf(rowsC[0], this->pool()));
  EXPECT_EQ(9, sumC.valueOf(rowsC[1], this->pool()));
}

// restore() merges the stream into the states registered beforehand, as in
// Flink: an entry of the stream overwrites a live entry of the same key, a
// live entry the stream does not carry is left alone, and a key the stream
// adds lands in the key table (no new group on probe).
TYPED_TEST(KeyedStateBackendContractTest, restoreMergesIntoDirtyBackend) {
  constexpr int64_t kLiveOnlyKey = 5;
  SumAggregate sumA;
  auto backendA = this->makeBackend();
  KeySelector selectorA(backendA->keyIndex(), {0});
  const auto keysA =
      this->probeKeys(selectorA, {kCollisionKeyA, kCollisionKeyB});
  auto stateA = backendA->getOrCreateAggregatingState(
      sumA.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());
  const auto rowsA = this->handles(stateA, keysA);
  sumA.add(rowsA[0], this->one(7));
  sumA.add(rowsA[1], this->one(9));
  const auto bytes = backendA->snapshotBytes();

  // A backend that already holds A with another value, and a key the
  // stream does not carry.
  SumAggregate sumB;
  auto backendB = this->makeBackend();
  KeySelector selectorB(backendB->keyIndex(), {0});
  const auto keysDirty =
      this->probeKeys(selectorB, {kCollisionKeyA, kLiveOnlyKey});
  auto stateB = backendB->getOrCreateAggregatingState(
      sumB.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());
  const auto rowsDirty = this->handles(stateB, keysDirty);
  sumB.add(rowsDirty[0], this->one(100));
  sumB.add(rowsDirty[1], this->one(5));
  backendB->commit();

  backendB->restoreBytes(bytes);

  // A took the stream's value, B arrived with the stream, the live-only
  // key kept its own; nothing is a new group.
  const auto keysB = this->probeKeys(
      selectorB, {kCollisionKeyA, kCollisionKeyB, kLiveOnlyKey});
  EXPECT_TRUE(selectorB.newGroups().empty());
  const auto rowsB = this->handles(stateB, keysB);
  EXPECT_EQ(7, sumB.valueOf(rowsB[0], this->pool()));
  EXPECT_EQ(9, sumB.valueOf(rowsB[1], this->pool()));
  EXPECT_EQ(5, sumB.valueOf(rowsB[2], this->pool()));
}

// A rescale restore feeds a wider stream to a backend that owns part of
// its key-group range: the backend takes the entries of its own key groups
// and reads the others past, on both backends alike.
TYPED_TEST(
    KeyedStateBackendContractTest,
    restoreSkipsKeyGroupsOutsideTheBackendRange) {
  SumAggregate sumA;
  auto backendA = this->makeBackend();
  KeySelector selectorA(backendA->keyIndex(), {0});
  const auto keysA =
      this->probeKeys(selectorA, {kCollisionKeyA, kCollisionKeyB});
  ASSERT_NE(keysA[0].keyGroup(), keysA[1].keyGroup());
  auto stateA = backendA->getOrCreateAggregatingState(
      sumA.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());
  const auto rowsA = this->handles(stateA, keysA);
  sumA.add(rowsA[0], this->one(7));
  sumA.add(rowsA[1], this->one(9));
  const auto bytes = backendA->snapshotBytes();

  // The backend owns A's key group alone.
  SumAggregate sumB;
  auto backendB = this->makeBackend(keysA[0].keyGroup(), 1);
  KeySelector selectorB(backendB->keyIndex(), {0});
  auto stateB = backendB->getOrCreateAggregatingState(
      sumB.descriptor(this->pool()),
      NamespaceSerializerTraits<VoidNamespace>::create());
  backendB->restoreBytes(bytes);

  // A came back with its value; B's row is a new group, its entry stayed
  // out (B's key group is not this backend's, so the state is not asked
  // for it).
  const auto keysB =
      this->probeKeys(selectorB, {kCollisionKeyA, kCollisionKeyB});
  ASSERT_EQ(1, selectorB.newGroups().size());
  EXPECT_EQ(1, selectorB.newGroups()[0]);
  const auto rowsB = this->handles(stateB, {keysB[0]});
  EXPECT_EQ(7, sumB.valueOf(rowsB[0], this->pool()));
}

// Key values come back through extractKeyColumn() - the RowContainer::
// extractColumn shape over key views, at the given result offset - both
// through the selector and through the index; the key rows themselves stay
// behind the index.
TYPED_TEST(KeyedStateBackendContractTest, extractKeyColumnReadsKeyValues) {
  auto backend = this->makeBackend();
  KeySelector selector(backend->keyIndex(), {0});
  this->probeKeys(selector, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  const auto distinct = selector.distinctKeys();
  ASSERT_EQ(2, distinct.size());
  const std::vector<vector_size_t> positions{0, 1};

  auto values = this->int64s({0, 0});
  selector.extractKeyColumn(distinct, positions, 0, 0, values);
  EXPECT_EQ(kCollisionKeyA, values->valueAt(0));
  EXPECT_EQ(kCollisionKeyB, values->valueAt(1));

  // Through the index, landing at offset 1 of a three-row result, with
  // the positions in reverse order.
  auto shifted = this->int64s({0, 0, 0});
  const std::vector<vector_size_t> reversed{1, 0};
  backend->keyIndex().extractKeyColumn(distinct, reversed, 0, 1, shifted);
  EXPECT_EQ(0, shifted->valueAt(0));
  EXPECT_EQ(kCollisionKeyB, shifted->valueAt(1));
  EXPECT_EQ(kCollisionKeyA, shifted->valueAt(2));
}

} // namespace facebook::velox::stateful
