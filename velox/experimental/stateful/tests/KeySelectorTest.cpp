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

#include <cstdint>
#include <memory>
#include <vector>

#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/state/KeySerializer.h"
#include "velox/experimental/stateful/state/StateKey.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::stateful {
namespace {

using namespace facebook::velox;

// 391 and 32728 are the collision pair that broke the old identity: both
// map to 250707955 under fmix64 % INT_MAX, so the old partition() silently
// merged them. Every test below probes them through the groupProbe path.
constexpr int64_t kCollisionKeyA = 391;
constexpr int64_t kCollisionKeyB = 32728;

class KeySelectorTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  RowVectorPtr makeInput(const std::vector<std::optional<int64_t>>& keys) {
    return makeRowVector({makeNullableFlatVector<int64_t>(keys)});
  }
};

// The collision pair lands on two different rows; nothing merges.
TEST_F(KeySelectorTest, collisionPairLandsOnDifferentRows) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());
  selector.probe(makeInput({kCollisionKeyA, kCollisionKeyB}));

  auto keys = selector.keys();
  ASSERT_EQ(2, keys.size());
  EXPECT_NE(keys[0].row(), keys[1].row());
  EXPECT_FALSE(keys[0].equals(keys[1]));

  auto distinct = selector.distinctKeys();
  ASSERT_EQ(2, distinct.size());
  EXPECT_NE(distinct[0].row(), distinct[1].row());
  EXPECT_FALSE(distinct[0].equals(distinct[1]));

  const auto& groupRows = selector.groupRows();
  ASSERT_EQ(2, groupRows.size());
  EXPECT_TRUE(groupRows[0].isValid(0));
  EXPECT_FALSE(groupRows[0].isValid(1));
  EXPECT_FALSE(groupRows[1].isValid(0));
  EXPECT_TRUE(groupRows[1].isValid(1));

  EXPECT_NE(nullptr, selector.keyRowContainer());
}

// Usage 1 (per-row): keys are 1:1 with input rows, equal keys share the row
// pointer, and repeated probes are idempotent.
TEST_F(KeySelectorTest, perRowKeys) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());
  selector.probe(makeInput(
      {kCollisionKeyA,
       kCollisionKeyB,
       kCollisionKeyA,
       kCollisionKeyA,
       kCollisionKeyB}));

  auto keys = selector.keys();
  ASSERT_EQ(5, keys.size());
  EXPECT_EQ(keys[0].row(), keys[2].row());
  EXPECT_EQ(keys[0].row(), keys[3].row());
  EXPECT_EQ(keys[1].row(), keys[4].row());
  EXPECT_NE(keys[0].row(), keys[1].row());
  EXPECT_TRUE(keys[0].equals(keys[2]));
  EXPECT_EQ(keys[0].hash(), keys[2].hash());
  EXPECT_EQ(keys[0].keyGroup(), keys[2].keyGroup());
  EXPECT_LT(keys[0].keyGroup(), 128);

  auto distinct = selector.distinctKeys();
  ASSERT_EQ(2, distinct.size());

  const auto& groupRows = selector.groupRows();
  ASSERT_EQ(2, groupRows.size());
  EXPECT_EQ(3, groupRows[0].countSelected());
  EXPECT_EQ(2, groupRows[1].countSelected());
  EXPECT_TRUE(groupRows[0].isValid(0));
  EXPECT_TRUE(groupRows[0].isValid(2));
  EXPECT_TRUE(groupRows[0].isValid(3));
  EXPECT_TRUE(groupRows[1].isValid(1));
  EXPECT_TRUE(groupRows[1].isValid(4));

  auto newGroups = selector.newGroups();
  ASSERT_EQ(2, newGroups.size());
  EXPECT_EQ(0, newGroups[0]);
  EXPECT_EQ(1, newGroups[1]);
}

// Row pointers stay stable and newGroups only reports first occurrences
// across probes.
TEST_F(KeySelectorTest, probesAreIncremental) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());

  selector.probe(makeInput({kCollisionKeyA}));
  auto firstRows = selector.keys();
  ASSERT_EQ(1, firstRows.size());
  ASSERT_EQ(1, selector.newGroups().size());

  selector.probe(makeInput({kCollisionKeyA, kCollisionKeyB}));
  auto keys = selector.keys();
  ASSERT_EQ(2, keys.size());
  EXPECT_EQ(firstRows[0].row(), keys[0].row());
  auto newGroups = selector.newGroups();
  ASSERT_EQ(1, newGroups.size());
  EXPECT_EQ(1, newGroups[0]);

  // Everything exists now: no new groups, pointers unchanged.
  selector.probe(makeInput({kCollisionKeyB, kCollisionKeyA}));
  keys = selector.keys();
  EXPECT_EQ(firstRows[0].row(), keys[1].row());
  EXPECT_TRUE(selector.newGroups().empty());
  EXPECT_EQ(2, selector.distinctKeys().size());
}

// Null keys form their own group: null equals null, one distinct key.
TEST_F(KeySelectorTest, nullKeysFormOneGroup) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());
  selector.probe(makeInput({std::nullopt, kCollisionKeyA, std::nullopt}));

  auto keys = selector.keys();
  ASSERT_EQ(3, keys.size());
  EXPECT_EQ(keys[0].row(), keys[2].row());
  EXPECT_NE(keys[0].row(), keys[1].row());
  EXPECT_TRUE(keys[0].equals(keys[2]));
  EXPECT_FALSE(keys[0].equals(keys[1]));

  auto distinct = selector.distinctKeys();
  ASSERT_EQ(2, distinct.size());
  EXPECT_EQ(2, selector.groupRows()[0].countSelected());
  EXPECT_EQ(1, selector.groupRows()[1].countSelected());
}

// Composite keys (varchar, bigint) group on the full column tuple.
TEST_F(KeySelectorTest, compositeKey) {
  auto input = makeRowVector(
      {makeNullableFlatVector<std::string>({"a", "b", "a", "a"}),
       makeNullableFlatVector<int64_t>({1, 1, 1, 2})});

  KeySelector selector({0, 1}, {VARCHAR(), BIGINT()}, 128, pool());
  selector.probe(input);

  auto keys = selector.keys();
  ASSERT_EQ(4, keys.size());
  EXPECT_EQ(keys[0].row(), keys[2].row());
  EXPECT_NE(keys[0].row(), keys[1].row());
  EXPECT_NE(keys[0].row(), keys[3].row());

  EXPECT_EQ(3, selector.distinctKeys().size());
  const auto& groupRows = selector.groupRows();
  ASSERT_EQ(3, groupRows.size());
  EXPECT_EQ(2, groupRows[0].countSelected());
  EXPECT_EQ(1, groupRows[1].countSelected());
  EXPECT_EQ(1, groupRows[2].countSelected());
}

// The table may switch hash modes at runtime (kArray -> kNormalizedKey /
// kHash on rehash). Previously returned row pointers must survive such
// transitions and the collision pair must stay distinct afterwards.
TEST_F(KeySelectorTest, rowPointersSurviveHashModeTransitions) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());

  // Small non-negative values keep the table in kArray mode.
  selector.probe(makeInput({kCollisionKeyA, kCollisionKeyB}));
  const char* rowA = selector.keys()[0].row();
  const char* rowB = selector.keys()[1].row();

  // Large and negative values overflow the value-ID ranges and force the
  // table to rehash into a generic mode.
  selector.probe(makeInput({1LL << 40, -(1LL << 40), -5}));
  EXPECT_EQ(3, selector.newGroups().size());

  selector.probe(makeInput({kCollisionKeyA, kCollisionKeyB}));
  auto keys = selector.keys();
  ASSERT_EQ(2, keys.size());
  EXPECT_EQ(rowA, keys[0].row());
  EXPECT_EQ(rowB, keys[1].row());
  EXPECT_TRUE(selector.newGroups().empty());
  // distinctKeys() reflects the last probe() input only; the five keys seen
  // so far live on in the key RowContainer / the table.
  EXPECT_EQ(2, selector.distinctKeys().size());
}

// Empty input is a no-op probe with empty results.
TEST_F(KeySelectorTest, emptyInput) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());
  selector.probe(makeInput({}));

  EXPECT_TRUE(selector.keys().empty());
  EXPECT_TRUE(selector.distinctKeys().empty());
  EXPECT_TRUE(selector.groupRows().empty());
  EXPECT_TRUE(selector.newGroups().empty());

  selector.probe(makeInput({kCollisionKeyA}));
  EXPECT_EQ(1, selector.keys().size());
}

// The stable hash chain of probe() must match the chain the serializer
// probes a restored key through (serialize -> deserialize round trip), so
// hash and keyGroup are identical at probe time and after a restart, and a
// restored key lands on the very row it had at probe time.
TEST_F(KeySelectorTest, probeHashMatchesSerializerChain) {
  KeySelector selector({0}, {BIGINT()}, 128, pool());
  selector.probe(makeInput({kCollisionKeyA, kCollisionKeyB, kCollisionKeyA}));

  auto distinct = selector.distinctKeys();
  ASSERT_EQ(2, distinct.size());
  auto serializer = selector.keySerializer();
  for (const auto& key : distinct) {
    auto restored = serializer->deserialize(serializer->serialize(key));
    EXPECT_EQ(key.row(), restored.row());
    EXPECT_EQ(key.hash(), restored.hash());
    EXPECT_EQ(key.keyGroup(), restored.keyGroup());
    EXPECT_TRUE(key.equals(restored));
    EXPECT_TRUE(restored.equals(key));
  }
}

} // namespace
} // namespace facebook::velox::stateful
