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

#include "velox/common/memory/Memory.h"
#include "velox/connectors/nexmark/Event.h"
#include "velox/connectors/nexmark/GeneratorConfig.h"
#include "velox/connectors/nexmark/NexmarkGenerator.h"

namespace facebook::velox::connector::nexmark::test {

namespace {

struct MemoryGuard {
  MemoryGuard() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManagerOptions{});
  }
};
static MemoryGuard kMemoryGuard;

std::unique_ptr<NexmarkGenerator> makeGenerator(int64_t maxEvents) {
  static auto root = memory::memoryManager()->addRootPool();
  static auto leaf = root->addLeafChild("test");
  NexmarkConfiguration cfg;
  cfg.bidProportion = 46;
  GeneratorConfig config(
      std::move(cfg),
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count(),
      1,
      maxEvents,
      1);
  return std::make_unique<NexmarkGenerator>(config, 0, -1, leaf.get());
}

int64_t getInt64(RowVector* rv, int child, vector_size_t idx) {
  return rv->childAt(child)->asFlatVector<int64_t>()->valueAt(idx);
}

std::string getString(RowVector* rv, int child, vector_size_t idx) {
  return rv->childAt(child)->asFlatVector<StringView>()->valueAt(idx).str();
}

Timestamp getTimestamp(RowVector* rv, int child, vector_size_t idx) {
  return rv->childAt(child)->asFlatVector<Timestamp>()->valueAt(idx);
}

void assertTimestampNanos(const Timestamp& ts) {
  EXPECT_EQ(ts.getNanos(), (ts.toMillis() % 1000) * 1'000'000);
}

void assertPersonFields(RowVector* person, vector_size_t i) {
  ASSERT_FALSE(person->isNullAt(i));
  EXPECT_GE(getInt64(person, 0, i), GeneratorConfig::FIRST_PERSON_ID);
  EXPECT_FALSE(getString(person, 1, i).empty());
  EXPECT_FALSE(getString(person, 2, i).empty());
  EXPECT_FALSE(getString(person, 3, i).empty());
  EXPECT_FALSE(getString(person, 4, i).empty());
  EXPECT_FALSE(getString(person, 5, i).empty());
  auto dt = getTimestamp(person, 6, i);
  EXPECT_GT(dt.getSeconds(), 0);
  assertTimestampNanos(dt);
}

void assertAuctionFields(RowVector* auction, vector_size_t i) {
  ASSERT_FALSE(auction->isNullAt(i));
  EXPECT_GT(getInt64(auction, 0, i), 0);
  EXPECT_FALSE(getString(auction, 1, i).empty());
  EXPECT_FALSE(getString(auction, 2, i).empty());
  EXPECT_GT(getInt64(auction, 3, i), 0);
  EXPECT_GT(getInt64(auction, 4, i), 0);
  EXPECT_GE(getInt64(auction, 4, i), getInt64(auction, 3, i));
  auto dt = getTimestamp(auction, 5, i);
  EXPECT_GT(dt.getSeconds(), 0);
  assertTimestampNanos(dt);
  auto exp = getTimestamp(auction, 6, i);
  EXPECT_GT(exp.toMillis(), dt.toMillis());
  assertTimestampNanos(exp);
  EXPECT_GE(getInt64(auction, 7, i), GeneratorConfig::FIRST_PERSON_ID);
  EXPECT_GE(getInt64(auction, 8, i), GeneratorConfig::FIRST_CATEGORY_ID);
}

void assertBidFields(RowVector* bid, vector_size_t i) {
  ASSERT_FALSE(bid->isNullAt(i));
  EXPECT_GE(getInt64(bid, 0, i), GeneratorConfig::FIRST_AUCTION_ID);
  EXPECT_GE(getInt64(bid, 1, i), GeneratorConfig::FIRST_PERSON_ID);
  EXPECT_GT(getInt64(bid, 2, i), 0);
  EXPECT_FALSE(getString(bid, 3, i).empty());
  EXPECT_FALSE(getString(bid, 4, i).empty());
  auto dt = getTimestamp(bid, 5, i);
  EXPECT_GT(dt.getSeconds(), 0);
  assertTimestampNanos(dt);
}

} // namespace

TEST(NexmarkGeneratorTest, NextBatchCountsAndFields) {
  const int64_t kMaxEvents = 1000;
  auto generator = makeGenerator(kMaxEvents);
  int totalCount = 0;
  int personCount = 0;
  int auctionCount = 0;
  int bidCount = 0;

  while (generator->hasNext()) {
    auto [eventVec, maxTs] = generator->nextBatch(512);
    auto types = eventVec->childAt(0)->asFlatVector<int32_t>();

    for (size_t i = 0; i < eventVec->size(); ++i) {
      totalCount++;
      auto type = static_cast<Event::Type>(types->valueAt(i));
      switch (type) {
        case Event::Type::PERSON:
          personCount++;
          assertPersonFields(eventVec->childAt(1)->as<RowVector>(), i);
          break;
        case Event::Type::AUCTION:
          auctionCount++;
          assertAuctionFields(eventVec->childAt(2)->as<RowVector>(), i);
          break;
        case Event::Type::BID:
          bidCount++;
          assertBidFields(eventVec->childAt(3)->as<RowVector>(), i);
          break;
      }
    }
  }

  EXPECT_EQ(totalCount, kMaxEvents);
  EXPECT_EQ(personCount, kMaxEvents / 50);
  EXPECT_EQ(auctionCount, kMaxEvents * 3 / 50);
  EXPECT_EQ(bidCount, kMaxEvents * 46 / 50);
  EXPECT_EQ(personCount + auctionCount + bidCount, totalCount);
}

} // namespace facebook::velox::connector::nexmark::test
