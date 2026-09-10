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
#include <vector>

#include <folly/dynamic.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/KeySerializer.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/experimental/stateful/state/StateMap.h"
#include "velox/experimental/stateful/state/StateTable.h"
#include "velox/experimental/stateful/state/StreamOperatorStateHandler.h"
#include "velox/type/Type.h"
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
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
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
// handle is idempotent per descriptor name.
TEST_F(StateBackendTest, backendAggregatingState) {
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keys =
      probeKeys(selector, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  ASSERT_EQ(3, keys.size());
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      selector.keySerializer(), kMaxParallelism, 0, kMaxParallelism);

  int initCount = 0;
  AggregatingStateDescriptor descriptor(
      "acc",
      {BIGINT()},
      [&](char* row) {
        ++initCount;
        // Single fixed-width accumulator column: value sits at offset 0.
        *reinterpret_cast<int64_t*>(row) = 0;
      },
      pool());
  auto state = backend.getOrCreateAggregatingState<VoidNamespace>(descriptor);
  EXPECT_EQ(
      state.get(),
      backend.getOrCreateAggregatingState<VoidNamespace>(descriptor).get());

  std::vector<char*> rows(keys.size(), nullptr);
  state->rows(
      folly::Range<const RowContainerStateKey*>(keys.data(), keys.size()),
      VoidNamespace::instance(),
      rows.data());
  EXPECT_EQ(2, initCount);
  EXPECT_EQ(rows[0], rows[2]);
  EXPECT_NE(rows[0], rows[1]);

  // Accumulate through the raw row pointers, as addRawInput would.
  const auto offsetOf = [&] {
    const auto column = state->valueRows()->columnAt(0);
    return column.offset();
  };
  *reinterpret_cast<int64_t*>(rows[0] + offsetOf()) = 10;
  *reinterpret_cast<int64_t*>(rows[1] + offsetOf()) = 20;
  *reinterpret_cast<int64_t*>(rows[2] + offsetOf()) += 11;

  // Fresh key instances resolve to the same rows with the accumulated
  // values; nothing crossed between the collision pair.
  auto readKeys = probeKeys(selector, {kCollisionKeyA, kCollisionKeyB});
  std::vector<char*> readRows(readKeys.size(), nullptr);
  state->rows(
      folly::Range<const RowContainerStateKey*>(
          readKeys.data(), readKeys.size()),
      VoidNamespace::instance(),
      readRows.data());
  EXPECT_EQ(2, initCount);
  EXPECT_EQ(rows[0], readRows[0]);
  EXPECT_EQ(rows[1], readRows[1]);
  EXPECT_EQ(21, *reinterpret_cast<int64_t*>(readRows[0] + offsetOf()));
  EXPECT_EQ(20, *reinterpret_cast<int64_t*>(readRows[1] + offsetOf()));
}

// Value / List / Map states on the same backend: Flink read semantics
// (miss is default), overwrite on map put, and the collision pair stays
// independent in each.
TEST_F(StateBackendTest, backendValueListMapStates) {
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      selector.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
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
// restore does not re-initialize acc rows, and snapshotting unchanged
// state twice yields identical bytes.
TEST_F(StateBackendTest, snapshotRestoreRoundTrip) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysA =
      probeKeys(selectorA, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  ASSERT_EQ(3, keysA.size());
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);

  int initCountA = 0;
  AggregatingStateDescriptor aggregatingDescriptor(
      "acc",
      {BIGINT()},
      [&](char* row) {
        ++initCountA;
        // Single fixed-width accumulator column: value sits at offset 0.
        *reinterpret_cast<int64_t*>(row) = 0;
      },
      pool());
  auto aggregatingState =
      backendA.getOrCreateAggregatingState<VoidNamespace>(aggregatingDescriptor);
  auto valueState = backendA.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto listState = backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  auto mapState = backendA.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>(
          "map", int64Serializer(), int64Serializer(), pool()));

  // Accumulate 21 / 20 over the collision pair, as addRawInput would.
  std::vector<char*> rows(keysA.size(), nullptr);
  aggregatingState->rows(
      folly::Range<const RowContainerStateKey*>(keysA.data(), keysA.size()),
      VoidNamespace::instance(),
      rows.data());
  const auto accumulatorOffset = aggregatingState->valueRows()->columnAt(0).offset();
  *reinterpret_cast<int64_t*>(rows[0] + accumulatorOffset) = 10;
  *reinterpret_cast<int64_t*>(rows[1] + accumulatorOffset) = 20;
  *reinterpret_cast<int64_t*>(rows[2] + accumulatorOffset) += 11;
  EXPECT_EQ(2, initCountA);

  const VoidNamespace ns;
  valueState->update(keysA[0], ns, std::make_shared<int64_t>(7));
  listState->add(keysA[0], ns, 1);
  listState->add(keysA[0], ns, 2);
  listState->add(keysA[1], ns, 3);
  mapState->put(keysA[0], ns, 10, 111);
  mapState->put(keysA[1], ns, 30, 300);

  const auto bytes = backendA.snapshot();
  EXPECT_EQ(bytes, backendA.snapshot());

  // Side B: a fresh backend over its own key container; restoring writes
  // the entries without touching the init callback.
  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysB =
      probeKeys(selectorB, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  int initCountB = 0;
  AggregatingStateDescriptor aggregatingDescriptorB(
      "acc",
      {BIGINT()},
      [&](char* row) {
        ++initCountB;
        *reinterpret_cast<int64_t*>(row) = 0;
      },
      pool());
  auto aggregatingStateB =
      backendB.getOrCreateAggregatingState<VoidNamespace>(aggregatingDescriptorB);
  auto valueStateB = backendB.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  auto listStateB = backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  auto mapStateB = backendB.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>(
          "map", int64Serializer(), int64Serializer(), pool()));
  backendB.restore(bytes);

  std::vector<char*> rowsB(keysB.size(), nullptr);
  aggregatingStateB->rows(
      folly::Range<const RowContainerStateKey*>(keysB.data(), keysB.size()),
      VoidNamespace::instance(),
      rowsB.data());
  EXPECT_EQ(0, initCountB);
  const auto accumulatorOffsetB = aggregatingStateB->valueRows()->columnAt(0).offset();
  EXPECT_EQ(21, *reinterpret_cast<int64_t*>(rowsB[0] + accumulatorOffsetB));
  EXPECT_EQ(20, *reinterpret_cast<int64_t*>(rowsB[1] + accumulatorOffsetB));
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

// Checkpointing registered but empty states round-trips as an empty
// stream: restore leaves every state at its miss semantics.
TEST_F(StateBackendTest, snapshotRestoreEmptyStates) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backendA.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  const auto bytes = backendA.snapshot();

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
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
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      selector.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
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
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      selector.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  KeyedStateBackend& raw = backend;
  StateDescriptor descriptor("x");
  EXPECT_THROW(raw.getOrCreateValueState(descriptor), VeloxException);
  EXPECT_THROW(raw.createTimerService(nullptr), VeloxException);
}

// Out-of-range key-group configurations are rejected at construction.
TEST_F(StateBackendTest, backendRejectsInvalidKeyGroupRange) {
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keySerializer = selector.keySerializer();
  EXPECT_THROW(
      (HeapKeyedStateBackend<RowContainerStateKey>(
          keySerializer, kMaxParallelism, 100, kMaxParallelism)),
      VeloxException);
  EXPECT_THROW(
      (HeapKeyedStateBackend<RowContainerStateKey>(
          keySerializer, kMaxParallelism, 0, 0)),
      VeloxException);
}

// Restoring a checkpoint that contains a state the target backend has not
// registered fails: one name is one state, and states must be registered
// before restore.
TEST_F(StateBackendTest, restoreRejectsUnknownState) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  backendA.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  const auto bytes = backendA.snapshot();

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  EXPECT_THROW(backendB.restore(bytes), VeloxException);
}

// A checkpoint whose key-group range does not overlap the target backend's
// range is rejected by the header cross-check instead of silently
// mis-bucketing.
TEST_F(StateBackendTest, restoreRejectsKeyGroupRangeMismatch) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 4, 2);
  backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  const auto bytes = backendA.snapshot();

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 100, 28);
  backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  EXPECT_THROW(backendB.restore(bytes), VeloxException);
}

// A states-bearing header with a zero key-group count is corruption, not an
// empty range: [64, 64) inside the local range would otherwise sneak past
// the overlap check (an empty range overlaps nothing) and restore nothing.
TEST_F(StateBackendTest, restoreRejectsZeroKeyGroupCount) {
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      selector.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto list = backend.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));

  std::string bytes;
  CheckpointWriter writer(bytes);
  writer.writeInt32(kCheckpointFormatVersion);
  writer.writeBytes(selector.keySerializer()->schema());
  writer.writeInt32(1);
  writer.writeBytes("list");
  writer.writeBytes(list->namespaceSchema());
  writer.writeBytes(list->valueSchema());
  writer.writeInt32(kMaxParallelism / 2);
  writer.writeInt32(0);
  EXPECT_THROW(backend.restore(bytes), VeloxException);
}

// A sub-range backend picks its own key groups out of a wider checkpoint,
// the shape of a scale-up restore: the in-range entries restore, the rest
// of the stream is skipped, and a key of a foreign key group is rejected
// by the state table instead of mis-bucketing.
TEST_F(StateBackendTest, restorePicksSubRange) {
  const std::vector<std::optional<int64_t>> inputs = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
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

  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
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
  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysB = probeKeys(selectorB, inputs);
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 64, 64);
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
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysA = probeKeys(selectorA, inputs);
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

  // Two backends over the two halves of the key-group space, as a halved
  // parallelism would cut it.
  HeapKeyedStateBackend<RowContainerStateKey> backendLow(
      selectorA.keySerializer(), kMaxParallelism, 0, 64);
  auto valueStateLow = backendLow.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  HeapKeyedStateBackend<RowContainerStateKey> backendHigh(
      selectorA.keySerializer(), kMaxParallelism, 64, 64);
  auto valueStateHigh = backendHigh.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "value", sharedInt64Serializer(), pool()));
  const VoidNamespace ns;
  for (const auto i : low) {
    valueStateLow->update(keysA[i], ns, std::make_shared<int64_t>(*inputs[i]));
  }
  for (const auto i : high) {
    valueStateHigh->update(keysA[i], ns, std::make_shared<int64_t>(*inputs[i]));
  }
  const auto bytesLow = backendLow.snapshot();
  const auto bytesHigh = backendHigh.snapshot();

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysB = probeKeys(selectorB, inputs);
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
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

} // namespace
} // namespace facebook::velox::stateful
