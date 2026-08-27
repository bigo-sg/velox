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
#include <memory>
#include <optional>
#include <vector>

#include <folly/dynamic.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
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
      return RowContainerStateKey(
          &schema, row, hash(value), kMaxParallelism);
    }
  };
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
  ASSERT_TRUE(
      outside.keyGroup() < minGroup || outside.keyGroup() > maxGroup);
  EXPECT_THROW(table.put(outside, ns, nullptr), VeloxException);

  // Two keys in the same key group stay separate entries; clearing drops
  // all buckets.
  table.clear();
  EXPECT_EQ(0, table.size());
}

// Storage-level regression for the collision pair through the state table.
TEST_F(StateBackendTest, stateTableCollisionPairStaysApart) {
  KeySelector selector({0}, kMaxParallelism, pool());
  auto keys =
      probeKeys(selector, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
  ASSERT_EQ(3, keys.size());

  const VoidNamespace ns;
  StateTable<RowContainerStateKey, VoidNamespace, char*> table(0, kMaxParallelism);
  char valueA{'a'};
  char valueB{'b'};
  table.put(keys[0], ns, &valueA);
  table.put(keys[1], ns, &valueB);
  EXPECT_EQ(&valueA, table.get(keys[0], ns));
  EXPECT_EQ(&valueB, table.get(keys[1], ns));
  EXPECT_EQ(&valueA, table.get(keys[2], ns));
}

// AccState: misses materialize + initialize fresh value rows, duplicate
// keys share a row, the collision pair accumulates independently, and the
// handle is idempotent per descriptor name.
TEST_F(StateBackendTest, backendAccState) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      kMaxParallelism, 0, kMaxParallelism);

  int initCount = 0;
  AccStateDescriptor descriptor(
      "acc",
      {BIGINT()},
      [&](char* row) {
        ++initCount;
        // Single fixed-width acc column: value sits at offset 0.
        *reinterpret_cast<int64_t*>(row) = 0;
      },
      pool());
  auto state = backend.getOrCreateAccState<VoidNamespace>(descriptor);
  EXPECT_EQ(state.get(), backend.getOrCreateAccState<VoidNamespace>(descriptor).get());

  KeySelector selector({0}, kMaxParallelism, pool());
  auto keys =
      probeKeys(selector, {kCollisionKeyA, kCollisionKeyB, kCollisionKeyA});
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
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      kMaxParallelism, 0, kMaxParallelism);
  KeyLab lab(pool());
  auto keyA = lab.key(kCollisionKeyA);
  auto keyB = lab.key(kCollisionKeyB);
  auto keyC = lab.key(999);
  const VoidNamespace ns;

  auto valueState = backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>("value", nullptr, pool()));
  auto stored = std::make_shared<int64_t>(7);
  EXPECT_EQ(nullptr, valueState->value(keyA, ns));
  valueState->update(keyA, ns, stored);
  EXPECT_EQ(stored, valueState->value(keyA, ns));
  EXPECT_EQ(nullptr, valueState->value(keyB, ns));
  valueState->remove(keyA, ns);
  EXPECT_EQ(nullptr, valueState->value(keyA, ns));

  auto listState = backend.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", nullptr, pool()));
  listState->add(keyA, ns, 1);
  listState->add(keyA, ns, 2);
  listState->add(keyB, ns, 3);
  EXPECT_EQ((std::vector<int64_t>{1, 2}), listState->get(keyA, ns));
  EXPECT_EQ((std::vector<int64_t>{3}), listState->get(keyB, ns));
  EXPECT_TRUE(listState->get(keyC, ns).empty());
  listState->remove(keyA, ns);
  EXPECT_TRUE(listState->get(keyA, ns).empty());

  auto mapState = backend.getOrCreateMapState<VoidNamespace>(
      MapStateDescriptor<int64_t, int64_t>("map", nullptr, nullptr, pool()));
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

// One descriptor name is one state: registering a second type under a used
// name fails instead of returning a mistyped handle.
TEST_F(StateBackendTest, backendRejectsNameReuseAcrossTypes) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      kMaxParallelism, 0, kMaxParallelism);
  backend.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>("dup", nullptr, pool()));
  EXPECT_THROW(
      backend.getOrCreateListState<VoidNamespace>(
          ListStateDescriptor<int64_t>("dup", nullptr, pool())),
      VeloxException);
}

// The pre-generic interface of the base stays abstract-compatible but is
// not implemented on the heap backend until the operators migrate.
TEST_F(StateBackendTest, backendPreGenericInterfaceIsNyi) {
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      kMaxParallelism, 0, kMaxParallelism);
  KeyedStateBackend& raw = backend;
  StateDescriptor descriptor("x");
  EXPECT_THROW(raw.getOrCreateValueState(descriptor), VeloxException);
  EXPECT_THROW(raw.createTimerService(nullptr), VeloxException);
}

// Out-of-range key-group configurations are rejected at construction.
TEST_F(StateBackendTest, backendRejectsInvalidKeyGroupRange) {
  EXPECT_THROW(
      (HeapKeyedStateBackend<RowContainerStateKey>(
          kMaxParallelism, 100, kMaxParallelism)),
      VeloxException);
  EXPECT_THROW(
      (HeapKeyedStateBackend<RowContainerStateKey>(kMaxParallelism, 0, 0)),
      VeloxException);
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
  auto restored = KeyedStateBackendParameters::create(
      parameters->serialize(), nullptr);
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
  auto legacyRestored =
      KeyedStateBackendParameters::create(legacy, nullptr);
  ASSERT_NE(nullptr, legacyRestored);
  EXPECT_EQ(128, legacyRestored->getMaxParallelism());
  EXPECT_EQ(0, legacyRestored->getStartKeyGroup());
  EXPECT_EQ(128, legacyRestored->getNumKeyGroups());
}

} // namespace
} // namespace facebook::velox::stateful
