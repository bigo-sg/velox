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

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <gtest/gtest.h>

#include "NexmarkConnectorTestBase.h"

namespace facebook::velox::connector::nexmark::test {

class NexmarkConnectorTest : public NexmarkConnectorTestBase {};

using facebook::velox::exec::test::PlanBuilder;

DEFINE_int64(numRows, 100, "Number of rows to process in the test");

TEST_F(NexmarkConnectorTest, testRows) {
  const int64_t numRows =
      FLAGS_numRows; // Use the value from the command line flag
  auto type = Event::createType();

  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(std::dynamic_pointer_cast<const RowType>(type))
                  .tableHandle(makeNexmarkTableHandle())
                  .endTableScan()
                  .planNode();

  exec::test::AssertQueryBuilder(plan)
      .split(makeNexmarkSplit(numRows))
      .assertTypeAndNumRows(type, numRows);
}

TEST_F(NexmarkConnectorTest, testProportions) {
  const int64_t numRows =
      FLAGS_numRows; // Use the value from the command line flag
  auto type = Event::createType();

  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(std::dynamic_pointer_cast<const RowType>(type))
                  .tableHandle(makeNexmarkTableHandle())
                  .endTableScan()
                  .partialAggregation(
                      {"event_type"}, // Group by "event_type"
                      {"count(*)"}) // Count rows for each "event_type"
                  .orderBy({"event_type"}, false) // Order by "event_type"
                  .planNode();

  auto result = exec::test::AssertQueryBuilder(plan)
                    .split(makeNexmarkSplit(numRows))
                    .copyResults(pool()); // Execute and fetch results

  // Assert the number of rows in the result is 3
  ASSERT_EQ(result->size(), 3);

  // Assert that the counts for each "event_type" are proportional
  std::vector<Event::Type> expectedTypes = {
      Event::Type::PERSON, Event::Type::AUCTION, Event::Type::BID};
  std::vector<int64_t> expectedCounts = {
      numRows / 50, numRows * 3 / 50, numRows * 46 / 50};

  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(
        result->childAt(0)->asFlatVector<int32_t>()->valueAt(i),
        static_cast<int32_t>(expectedTypes[i]));
    ASSERT_EQ(
        result->childAt(1)->asFlatVector<int64_t>()->valueAt(i),
        expectedCounts[i]);
  }
}

TEST_F(NexmarkConnectorTest, testNext) {
  auto generator = makeNexmarkGenerator(FLAGS_numRows);
  int count = 0;
  int personCount = 0;
  int auctionCount = 0;
  int bidCount = 0;

  while (generator->hasNext()) {
    auto nextEvent = generator->next();
    const auto& event = nextEvent.getEvent();

    count++;
    switch (event.getType()) {
      case Event::Type::PERSON:
        personCount++;
        break;
      case Event::Type::AUCTION:
        auctionCount++;
        break;
      case Event::Type::BID:
        bidCount++;
        break;
    }
  }
  ASSERT_EQ(count, FLAGS_numRows);

  const std::vector<int64_t> expectedCounts = {
      FLAGS_numRows / 50, FLAGS_numRows * 3 / 50, FLAGS_numRows * 46 / 50};
  ASSERT_EQ(personCount, expectedCounts[0]);
  ASSERT_EQ(auctionCount, expectedCounts[1]);
  ASSERT_EQ(bidCount, expectedCounts[2]);
}

TEST_F(NexmarkConnectorTest, testNextBatch) {
  auto generator = makeNexmarkGenerator(FLAGS_numRows);
  int count = 0;
  int personCount = 0;
  int auctionCount = 0;
  int bidCount = 0;

  size_t batchSize = 8192;
  while (generator->hasNext()) {
    auto pair = generator->nextBatch(batchSize);
    auto [eventVector, maxWallclockTimestamp] = pair;

    count += eventVector->size();
    auto eventTypeVector = eventVector->childAt(0)->asFlatVector<int32_t>();
    for (size_t i = 0; i < eventVector->size(); ++i) {
      auto eventType = static_cast<Event::Type>(eventTypeVector->valueAt(i));
      switch (eventType) {
        case Event::Type::PERSON:
          personCount++;
          break;
        case Event::Type::AUCTION:
          auctionCount++;
          break;
        case Event::Type::BID:
          bidCount++;
          break;
      }
    }
  }

  ASSERT_EQ(count, FLAGS_numRows);

  const std::vector<int64_t> expectedCounts = {
      FLAGS_numRows / 50, FLAGS_numRows * 3 / 50, FLAGS_numRows * 46 / 50};
  ASSERT_EQ(personCount, expectedCounts[0]);
  ASSERT_EQ(auctionCount, expectedCounts[1]);
  ASSERT_EQ(bidCount, expectedCounts[2]);
}

} // namespace facebook::velox::connector::nexmark::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}
