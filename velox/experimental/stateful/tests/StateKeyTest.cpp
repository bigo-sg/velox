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
#include "velox/experimental/stateful/state/StateKey.h"
#include <gtest/gtest.h>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "velox/exec/RowContainer.h"
#include "velox/exec/VectorHasher.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/state/KeySerializer.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::stateful::test {
namespace {

using exec::RowContainer;

class StateKeyTest : public testing::Test, public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::unique_ptr<RowContainer> newContainer(
      const std::vector<TypePtr>& types) {
    return std::make_unique<RowContainer>(types, pool());
  }

  VectorPtr bigintValue(int64_t value) {
    auto vector = BaseVector::create(BIGINT(), 1, pool());
    vector->as<FlatVector<int64_t>>()->set(0, value);
    return vector;
  }

  VectorPtr varcharValue(const std::string& value) {
    auto vector = BaseVector::create(VARCHAR(), 1, pool());
    vector->as<FlatVector<StringView>>()->set(0, StringView(value));
    return vector;
  }

  VectorPtr nullValue(const TypePtr& type) {
    auto vector = BaseVector::create(type, 1, pool());
    vector->setNull(0, true);
    return vector;
  }

  // Creates a new row in 'container' and stores the single value of each
  // column vector into the matching column.
  char* storeRow(
      RowContainer* container,
      const std::vector<VectorPtr>& columns) {
    char* row = container->newRow();
    for (vector_size_t i = 0; i < columns.size(); ++i) {
      DecodedVector decoded;
      decoded.decode(*columns[i]);
      container->store(decoded, 0, row, i);
    }
    return row;
  }

  // Computes the expected 64-bit hash of a key row with the same
  // VectorHasher chain used by HashPartitionFunction: first column with
  // mix=false, following columns with mix=true. Independently re-implemented
  // here so that key.hash() is pinned to this idiom.
  uint64_t expectedHash(const RowContainerKeySchema& schema, const char* row) {
    const SelectivityVector rows(1);
    raw_vector<uint64_t> hashes(1);
    for (vector_size_t i = 0; i < schema.keyTypes().size(); ++i) {
      auto hasher = exec::VectorHasher::create(schema.keyTypes()[i], i);
      auto column = BaseVector::create(schema.keyTypes()[i], 1, pool());
      const char* rowPtr = row;
      schema.container()->extractColumn(&rowPtr, 1, i, column);
      hasher->decode(*column, rows);
      hasher->hash(rows, i > 0, hashes);
    }
    return hashes[0];
  }
};

TEST_F(StateKeyTest, bigintKeys) {
  auto container = newContainer({BIGINT()});
  RowContainerKeySchema schema(container.get(), {BIGINT()});
  constexpr uint32_t kMaxParallelism = 128;

  auto* row391 = storeRow(container.get(), {bigintValue(391)});
  auto* row392 = storeRow(container.get(), {bigintValue(392)});
  RowContainerStateKey key391(
      &schema, row391, expectedHash(schema, row391), kMaxParallelism);
  RowContainerStateKey key392(
      &schema, row392, expectedHash(schema, row392), kMaxParallelism);

  EXPECT_TRUE(key391.equals(key391));
  EXPECT_FALSE(key391.equals(key392));
  EXPECT_FALSE(key392.equals(key391));
  EXPECT_NE(key391.hash(), key392.hash());
}

// Regression for the hash collision bug: BIGINT keys 391 and 32728 collided
// under the old identity scheme (fmix64 % INT_MAX is equal for both). Under
// the new scheme the 64-bit hashes differ and equals() compares the actual
// key values, so the two keys address distinct state.
TEST_F(StateKeyTest, collisionPair391And32728) {
  auto container = newContainer({BIGINT()});
  RowContainerKeySchema schema(container.get(), {BIGINT()});
  constexpr uint32_t kMaxParallelism = 128;

  auto* row391 = storeRow(container.get(), {bigintValue(391)});
  auto* row32728 = storeRow(container.get(), {bigintValue(32728)});
  const uint64_t hash391 = expectedHash(schema, row391);
  const uint64_t hash32728 = expectedHash(schema, row32728);

  RowContainerStateKey key391(&schema, row391, hash391, kMaxParallelism);
  RowContainerStateKey key32728(&schema, row32728, hash32728, kMaxParallelism);

  // The old scheme collapsed these two keys onto the same identity; the new
  // 64-bit VectorHasher chain must keep them apart.
  EXPECT_NE(hash391, hash32728);
  EXPECT_NE(key391.hash(), key32728.hash());
  EXPECT_FALSE(key391.equals(key32728));
  EXPECT_FALSE(key32728.equals(key391));
}

TEST_F(StateKeyTest, equalValuesInDifferentRowsAreEqual) {
  auto container = newContainer({BIGINT()});
  RowContainerKeySchema schema(container.get(), {BIGINT()});
  constexpr uint32_t kMaxParallelism = 128;

  auto* rowA = storeRow(container.get(), {bigintValue(42)});
  auto* rowB = storeRow(container.get(), {bigintValue(42)});
  RowContainerStateKey keyA(
      &schema, rowA, expectedHash(schema, rowA), kMaxParallelism);
  RowContainerStateKey keyB(
      &schema, rowB, expectedHash(schema, rowB), kMaxParallelism);

  EXPECT_NE(rowA, rowB);
  EXPECT_TRUE(keyA.equals(keyB));
  EXPECT_TRUE(keyB.equals(keyA));
  EXPECT_EQ(keyA.hash(), keyB.hash());
  EXPECT_EQ(keyA.keyGroup(), keyB.keyGroup());
}

TEST_F(StateKeyTest, keyGroupDerivationAndStability) {
  auto container = newContainer({BIGINT()});
  RowContainerKeySchema schema(container.get(), {BIGINT()});
  auto* row = storeRow(container.get(), {bigintValue(7)});
  const uint64_t hash = expectedHash(schema, row);

  constexpr uint32_t kMaxParallelism = 10;
  RowContainerStateKey key(&schema, row, hash, kMaxParallelism);

  // Derived once from the hash and stable across reads (cached).
  EXPECT_EQ(key.keyGroup(), static_cast<uint32_t>(hash % kMaxParallelism));
  EXPECT_EQ(key.keyGroup(), key.keyGroup());
  EXPECT_EQ(key.hash(), hash);
  EXPECT_EQ(key.hash(), key.hash());

  // Same key value under a different maxParallelism derives a different
  // keyGroup but keeps the same hash.
  RowContainerStateKey rescaled(&schema, row, hash, 16);
  EXPECT_EQ(rescaled.hash(), hash);
  EXPECT_EQ(rescaled.keyGroup(), static_cast<uint32_t>(hash % 16));
}

TEST_F(StateKeyTest, nullKeys) {
  auto container = newContainer({BIGINT()});
  RowContainerKeySchema schema(container.get(), {BIGINT()});
  constexpr uint32_t kMaxParallelism = 128;

  auto* nullRowA = storeRow(container.get(), {nullValue(BIGINT())});
  auto* nullRowB = storeRow(container.get(), {nullValue(BIGINT())});
  auto* valueRow = storeRow(container.get(), {bigintValue(1)});
  RowContainerStateKey nullA(
      &schema, nullRowA, expectedHash(schema, nullRowA), kMaxParallelism);
  RowContainerStateKey nullB(
      &schema, nullRowB, expectedHash(schema, nullRowB), kMaxParallelism);
  RowContainerStateKey value(
      &schema, valueRow, expectedHash(schema, valueRow), kMaxParallelism);

  EXPECT_TRUE(nullA.equals(nullB));
  EXPECT_FALSE(nullA.equals(value));
  EXPECT_FALSE(value.equals(nullA));
}

TEST_F(StateKeyTest, varcharKeys) {
  auto container = newContainer({VARCHAR()});
  RowContainerKeySchema schema(container.get(), {VARCHAR()});
  constexpr uint32_t kMaxParallelism = 128;

  auto* rowFoo = storeRow(container.get(), {varcharValue("foo")});
  auto* rowFoo2 = storeRow(container.get(), {varcharValue("foo")});
  auto* rowFood = storeRow(container.get(), {varcharValue("food")});
  auto* rowBar = storeRow(container.get(), {varcharValue("bar")});
  RowContainerStateKey foo(
      &schema, rowFoo, expectedHash(schema, rowFoo), kMaxParallelism);
  RowContainerStateKey foo2(
      &schema, rowFoo2, expectedHash(schema, rowFoo2), kMaxParallelism);
  RowContainerStateKey food(
      &schema, rowFood, expectedHash(schema, rowFood), kMaxParallelism);
  RowContainerStateKey bar(
      &schema, rowBar, expectedHash(schema, rowBar), kMaxParallelism);

  EXPECT_TRUE(foo.equals(foo2));
  EXPECT_TRUE(foo2.equals(foo));
  // Prefix strings must not compare equal.
  EXPECT_FALSE(foo.equals(food));
  EXPECT_FALSE(food.equals(foo));
  EXPECT_FALSE(foo.equals(bar));
}

TEST_F(StateKeyTest, multiColumnKeys) {
  auto container = newContainer({BIGINT(), VARCHAR()});
  RowContainerKeySchema schema(container.get(), {BIGINT(), VARCHAR()});
  constexpr uint32_t kMaxParallelism = 64;

  auto* rowA = storeRow(container.get(), {bigintValue(1), varcharValue("a")});
  auto* rowA2 = storeRow(container.get(), {bigintValue(1), varcharValue("a")});
  auto* rowB = storeRow(container.get(), {bigintValue(1), varcharValue("b")});
  auto* rowC = storeRow(container.get(), {bigintValue(2), varcharValue("a")});
  RowContainerStateKey keyA(
      &schema, rowA, expectedHash(schema, rowA), kMaxParallelism);
  RowContainerStateKey keyA2(
      &schema, rowA2, expectedHash(schema, rowA2), kMaxParallelism);
  RowContainerStateKey keyB(
      &schema, rowB, expectedHash(schema, rowB), kMaxParallelism);
  RowContainerStateKey keyC(
      &schema, rowC, expectedHash(schema, rowC), kMaxParallelism);

  EXPECT_TRUE(keyA.equals(keyA2));
  EXPECT_FALSE(keyA.equals(keyB));
  EXPECT_FALSE(keyA.equals(keyC));
  EXPECT_FALSE(keyB.equals(keyC));
}

TEST_F(StateKeyTest, crossSchemaKeysNeverEqual) {
  auto container1 = newContainer({BIGINT()});
  auto container2 = newContainer({BIGINT()});
  RowContainerKeySchema schema1(container1.get(), {BIGINT()});
  RowContainerKeySchema schema2(container2.get(), {BIGINT()});

  auto* row1 = storeRow(container1.get(), {bigintValue(5)});
  auto* row2 = storeRow(container2.get(), {bigintValue(5)});
  RowContainerStateKey key1(&schema1, row1, expectedHash(schema1, row1), 128);
  RowContainerStateKey key2(&schema2, row2, expectedHash(schema2, row2), 128);

  // Same value, different operators: keys of different schemas must not
  // compare equal (rows come from different containers).
  EXPECT_FALSE(key1.equals(key2));
}

TEST(VoidNamespaceTest, singleton) {
  EXPECT_EQ(&VoidNamespace::instance(), &VoidNamespace::instance());
  EXPECT_EQ(VoidNamespace::instance().hash(), 0);
  EXPECT_TRUE(VoidNamespace::instance().equals(VoidNamespace::instance()));
}

class RowContainerStateKeySerializerTest : public StateKeyTest {};

// Every round trip goes through the owning selector: serialize() writes the
// column values, deserialize() reassembles them and probes them back through
// the selector, so a restored key must land on the very row it had at probe
// time, with the same hash and key group.
TEST_F(RowContainerStateKeySerializerTest, bigintRoundtrip) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());
  selector.probe(
      makeRowVector({makeNullableFlatVector<int64_t>({391, 32728, -1, 0})}));
  auto serializer = selector.keySerializer();
  for (const auto& key : selector.keys()) {
    auto restored = serializer->deserialize(serializer->serialize(key));
    EXPECT_EQ(key.row(), restored.row());
    EXPECT_TRUE(restored.equals(key));
    EXPECT_EQ(restored.hash(), key.hash());
    EXPECT_EQ(restored.keyGroup(), key.keyGroup());
  }
}

TEST_F(RowContainerStateKeySerializerTest, collisionPairRoundtrip) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());
  selector.probe(
      makeRowVector({makeNullableFlatVector<int64_t>({391, 32728})}));
  auto keys = selector.keys();
  auto serializer = selector.keySerializer();

  auto restored391 = serializer->deserialize(serializer->serialize(keys[0]));
  auto restored32728 = serializer->deserialize(serializer->serialize(keys[1]));

  // The restored keys stay distinct: the collision pair must not collapse
  // through the snapshot / restore path either.
  EXPECT_FALSE(restored391.equals(restored32728));
  EXPECT_NE(restored391.hash(), restored32728.hash());
  EXPECT_NE(restored391.row(), restored32728.row());
}

TEST_F(RowContainerStateKeySerializerTest, varcharRoundtrip) {
  KeySelector selector({0}, {VARCHAR()}, 128, pool());
  selector.probe(makeRowVector({makeNullableFlatVector<std::string>(
      {"", "a", "hello world", std::string(500, 'x')})}));
  auto serializer = selector.keySerializer();
  for (const auto& key : selector.keys()) {
    auto restored = serializer->deserialize(serializer->serialize(key));
    EXPECT_EQ(key.row(), restored.row());
    EXPECT_TRUE(restored.equals(key));
    EXPECT_EQ(restored.hash(), key.hash());
    EXPECT_EQ(restored.keyGroup(), key.keyGroup());
  }
}

TEST_F(RowContainerStateKeySerializerTest, nullColumnRoundtrip) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());
  selector.probe(
      makeRowVector({makeNullableFlatVector<int64_t>({std::nullopt, 5})}));
  auto keys = selector.keys();
  ASSERT_EQ(2, keys.size());
  auto serializer = selector.keySerializer();

  // The null column round-trips through its null flag alone; null equals
  // null at probe time, so the restored key lands on the null group's row.
  auto restoredNull = serializer->deserialize(serializer->serialize(keys[0]));
  EXPECT_EQ(keys[0].row(), restoredNull.row());
  EXPECT_TRUE(restoredNull.equals(keys[0]));
  EXPECT_EQ(restoredNull.hash(), keys[0].hash());
  EXPECT_FALSE(restoredNull.equals(keys[1]));
}

TEST_F(RowContainerStateKeySerializerTest, multiColumnRoundtrip) {
  KeySelector selector({0, 1}, {BIGINT(), VARCHAR()}, 64, pool());
  selector.probe(makeRowVector(
      {makeNullableFlatVector<int64_t>({3}),
       makeNullableFlatVector<std::string>({"k"})}));
  auto keys = selector.keys();
  ASSERT_EQ(1, keys.size());
  auto serializer = selector.keySerializer();

  auto restored = serializer->deserialize(serializer->serialize(keys[0]));

  EXPECT_EQ(keys[0].row(), restored.row());
  EXPECT_TRUE(restored.equals(keys[0]));
  EXPECT_EQ(restored.hash(), keys[0].hash());
  EXPECT_EQ(restored.keyGroup(), keys[0].keyGroup());
  EXPECT_EQ(restored.schema(), keys[0].schema());
}

class NamespaceSerializerTest : public StateKeyTest {};

TEST_F(NamespaceSerializerTest, voidRoundtrip) {
  VoidNamespaceSerializer serializer;
  EXPECT_TRUE(serializer.serialize(VoidNamespace::instance()).empty());
  EXPECT_TRUE(serializer.deserialize("").equals(VoidNamespace::instance()));
}

// Minimal in-memory implementations exercising the explicit (key, namespace,
// element) API of the state interfaces: state must be addressable per (K, N)
// pair without the namespace leaking into the key.
template <typename K, typename N, typename V>
class FakeValueState : public ValueState<K, N, V> {
 public:
  V value(const K& key, const N& ns) override {
    return map_[{key, ns}];
  }

  void update(const K& key, const N& ns, const V& value) override {
    map_[{key, ns}] = value;
  }

  void remove(const K& key, const N& ns) override {
    map_.erase({key, ns});
  }

  void clear() override {
    map_.clear();
  }

 private:
  std::map<std::pair<K, N>, V> map_;
};

template <typename K, typename N, typename S>
class FakeListState : public ListState<K, N, S> {
 public:
  std::vector<S> get(const K& key, const N& ns) override {
    return map_[{key, ns}];
  }

  void add(const K& key, const N& ns, const S& value) override {
    map_[{key, ns}].push_back(value);
  }

  void remove(const K& key, const N& ns) override {
    map_.erase({key, ns});
  }

  void clear() override {
    map_.clear();
  }

 private:
  std::map<std::pair<K, N>, std::vector<S>> map_;
};

template <typename K, typename N, typename UK, typename UV>
class FakeMapState : public MapState<K, N, UK, UV> {
 public:
  UV get(const K& key, const N& ns, const UK& userKey) override {
    return map_[{key, ns}][userKey];
  }

  void put(const K& key, const N& ns, const UK& userKey, const UV& value)
      override {
    map_[{key, ns}][userKey] = value;
  }

  std::map<UK, UV> entries(const K& key, const N& ns) override {
    return map_[{key, ns}];
  }

  void remove(const K& key, const N& ns, const UK& userKey) override {
    map_[{key, ns}].erase(userKey);
  }

  void clear() override {
    map_.clear();
  }

 private:
  std::map<std::pair<K, N>, std::map<UK, UV>> map_;
};

TEST(StateApiTest, valueStateExplicitKeyAndNamespace) {
  FakeValueState<int64_t, int64_t, std::string> state;
  state.update(1, 100, "one-ns100");
  state.update(1, 200, "one-ns200");
  state.update(2, 100, "two-ns100");

  EXPECT_EQ(state.value(1, 100), "one-ns100");
  EXPECT_EQ(state.value(1, 200), "one-ns200");
  EXPECT_EQ(state.value(2, 100), "two-ns100");

  state.remove(1, 100);
  EXPECT_EQ(state.value(1, 100), "");
  EXPECT_EQ(state.value(1, 200), "one-ns200");
  state.clear();
  EXPECT_EQ(state.value(1, 200), "");
}

TEST(StateApiTest, listStateExplicitKeyAndNamespace) {
  FakeListState<int64_t, int64_t, int32_t> state;
  state.add(1, 100, 1);
  state.add(1, 100, 2);
  state.add(1, 200, 3);

  EXPECT_EQ(state.get(1, 100), (std::vector<int32_t>{1, 2}));
  EXPECT_EQ(state.get(1, 200), (std::vector<int32_t>{3}));
  EXPECT_TRUE(state.get(2, 100).empty());

  state.remove(1, 100);
  EXPECT_TRUE(state.get(1, 100).empty());
  EXPECT_EQ(state.get(1, 200), (std::vector<int32_t>{3}));
}

TEST(StateApiTest, mapStateExplicitKeyNamespaceAndUserKey) {
  FakeMapState<int64_t, int64_t, std::string, int64_t> state;
  state.put(1, 100, "a", 1);
  state.put(1, 100, "b", 2);
  state.put(1, 200, "a", 3);

  EXPECT_EQ(state.get(1, 100, "a"), 1);
  EXPECT_EQ(state.get(1, 100, "b"), 2);
  EXPECT_EQ(state.get(1, 200, "a"), 3);

  auto entries = state.entries(1, 100);
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries.at("a"), 1);
  EXPECT_EQ(entries.at("b"), 2);

  state.remove(1, 100, "a");
  EXPECT_EQ(state.get(1, 100, "a"), 0);
  EXPECT_EQ(state.get(1, 100, "b"), 2);
  EXPECT_EQ(state.get(1, 200, "a"), 3);
}

} // namespace
} // namespace facebook::velox::stateful::test
