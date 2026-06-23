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

#include "velox/connectors/nexmark/AuctionGenerator.h"

namespace facebook::velox::connector::nexmark {

RowVectorPtr AuctionGenerator::nextAuctionBatch(
    size_t rows,
    int64_t eventsCountSoFar,
    const FlatVector<int32_t>& eventTypeVector,
    const FlatVector<int64_t>& eventIdVector,
    pcg32_fast& random,
    const FlatVector<int64_t>& timestampVector,
    const NexmarkGeneratorConfig& config,
    memory::MemoryPool* pool) {

  auto auctionVector = Auction::createVector(rows, pool);
  auto idVector = auctionVector->childAt(0)->asFlatVector<int64_t>();
  auto itemNameVector = auctionVector->childAt(1)->asFlatVector<StringView>();
  auto descriptionVector = auctionVector->childAt(2)->asFlatVector<StringView>();
  auto initialBidVector = auctionVector->childAt(3)->asFlatVector<int64_t>();
  auto reserveVector = auctionVector->childAt(4)->asFlatVector<int64_t>();
  auto dateTimeVector = auctionVector->childAt(5)->asFlatVector<Timestamp>();
  auto expiresVector = auctionVector->childAt(6)->asFlatVector<Timestamp>();
  auto sellerVector = auctionVector->childAt(7)->asFlatVector<int64_t>();
  auto categoryVector = auctionVector->childAt(8)->asFlatVector<int64_t>();
  auto extraVector = auctionVector->childAt(9)->asFlatVector<StringView>();

  for (size_t i = 0; i < rows; ++i) {
    auto eventType = static_cast<Event::Type>(eventTypeVector.valueAt(i));
    if (Event::Type::AUCTION != eventType) {
      auctionVector->setNull(i, true);
      continue;
    }

    auto eventId = eventIdVector.valueAt(i);
    auto timestamp = timestampVector.valueAt(i);
    auto auction =
        nextAuction(eventsCountSoFar + i, eventId, random, timestamp, config);

    idVector->set(i, auction.id);
    itemNameVector->set(i, StringView(auction.itemName));
    descriptionVector->set(i, StringView(auction.description));
    initialBidVector->set(i, auction.initialBid);
    reserveVector->set(i, auction.reserve);
    dateTimeVector->set(i, Timestamp::fromMillis(auction.dateTime));
    expiresVector->set(i, Timestamp::fromMillis(auction.expires));
    sellerVector->set(i, auction.seller);
    categoryVector->set(i, auction.category);
    extraVector->set(i, StringView(auction.extra));
  }

  return auctionVector;
}

int64_t AuctionGenerator::nextAuctionLengthMs(
    int64_t eventsCountSoFar,
    pcg32_fast& random,
    int64_t timestamp,
    const NexmarkGeneratorConfig& config) {

  // What's our current event number?
  int64_t currentEventNumber = config.nextAdjustedEventNumber(eventsCountSoFar);
  // How many events till we've generated numInFlightAuctions?
  int64_t numEventsForAuctions =
      ((int64_t)config.getNumInFlightAuctions() * config.totalProportion)
          / config.auctionProportion;
  // When will the auction numInFlightAuctions beyond now be generated?
  int64_t futureAuction = config.timestampForEvent(currentEventNumber + numEventsForAuctions);
  // Choose a length with average horizonMs.
  int64_t horizonMs = futureAuction - timestamp;
  return 1L + LongGenerator::nextLong(random, std::max<int64_t>(horizonMs * 2, 1L));
}

} // namespace facebook::velox::connector::nexmark
