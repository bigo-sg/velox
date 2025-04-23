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
#include "velox/connectors/nexmark/StringsGenerator.h"
#include "velox/connectors/nexmark/PersonGenerator.h"
#include "velox/connectors/nexmark/PriceGenerator.h"

namespace facebook::velox::connector::nexmark {

RowVectorPtr AuctionGenerator::nextAuctionBatch(
    size_t rows,
    int64_t eventsCountSoFar,
    const FlatVector<int32_t>& eventTypeVector,
    const FlatVector<int64_t>& eventIdVector,
    pcg32_fast& random,
    const FlatVector<int64_t>& timestampVector,
    const GeneratorConfig& config,
    memory::MemoryPool* pool) {

  auto auctionVector = Auction::createVector(rows, pool);
  auto idVector = auctionVector->childAt(0)->asFlatVector<int64_t>();
  auto nameVector = auctionVector->childAt(1)->asFlatVector<StringView>();
  auto descVector = auctionVector->childAt(2)->asFlatVector<StringView>();
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
    int64_t id =
        lastBase0AuctionId(config, eventId) + GeneratorConfig::FIRST_AUCTION_ID;

    int64_t seller;
    // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
    if (getNextInt(random, config.getHotSellersRatio()) > 0) {
      // Choose the first person in the batch of last HOT_SELLER_RATIO people.
      seller = (PersonGenerator::lastBase0PersonId(config, eventId) /
                HOT_SELLER_RATIO) *
          HOT_SELLER_RATIO;
    } else {
      seller = PersonGenerator::nextBase0PersonId(eventId, random, config);
    }
    seller += GeneratorConfig::FIRST_PERSON_ID;

    int64_t category =
        GeneratorConfig::FIRST_CATEGORY_ID + getNextInt(random, NUM_CATEGORIES);
    int64_t initialBid = PriceGenerator::nextPrice(random);
    int64_t expires = timestamp +
        nextAuctionLengthMs(eventsCountSoFar + i, random, timestamp, config);
    std::string name = StringsGenerator::nextString(random, 20);
    std::string desc = StringsGenerator::nextString(random, 100);
    int64_t reserve = initialBid + PriceGenerator::nextPrice(random);
    int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
    std::string_view extra = StringsGenerator::nextExtra(
        random, currentSize, config.getAvgAuctionByteSize());

    idVector->set(i, id);
    nameVector->set(i, StringView(name));
    descVector->set(i, StringView(desc));
    initialBidVector->set(i, initialBid);
    reserveVector->set(i, reserve);
    dateTimeVector->set(
        i, Timestamp(timestamp / 1000, (timestamp % 1000) * 1000));
    expiresVector->set(i, Timestamp(expires / 1000, (expires % 1000) * 1000));
    sellerVector->set(i, seller);
    categoryVector->set(i, category);
    extraVector->set(i, StringView(std::move(extra)));
  }

  return auctionVector;
}

Auction AuctionGenerator::nextAuction(
    int64_t eventsCountSoFar,
    int64_t eventId,
    pcg32_fast& random,
    int64_t timestamp,
    const GeneratorConfig& config) {
  int64_t id = lastBase0AuctionId(config, eventId) + GeneratorConfig::FIRST_AUCTION_ID;

  int64_t seller;
  // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
  if (getNextInt(random, config.getHotSellersRatio()) > 0) {
    // Choose the first person in the batch of last HOT_SELLER_RATIO people.
    seller = (PersonGenerator::lastBase0PersonId(config, eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
  } else {
    seller = PersonGenerator::nextBase0PersonId(eventId, random, config);
  }
  seller += GeneratorConfig::FIRST_PERSON_ID;

  int64_t category = GeneratorConfig::FIRST_CATEGORY_ID + getNextInt(random, NUM_CATEGORIES);
  int64_t initialBid = PriceGenerator::nextPrice(random);
  int64_t expires = timestamp + nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
  std::string name = StringsGenerator::nextString(random, 20);
  std::string desc = StringsGenerator::nextString(random, 100);
  int64_t reserve = initialBid + PriceGenerator::nextPrice(random);
  int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
  std::string_view extra = StringsGenerator::nextExtra(random, currentSize, config.getAvgAuctionByteSize());

  return Auction(
      id,
      std::move(name),
      std::move(desc),
      initialBid,
      reserve,
      timestamp,
      expires,
      seller,
      category,
      std::move(extra));
}

int64_t AuctionGenerator::nextAuctionLengthMs(
    int64_t eventsCountSoFar,
    pcg32_fast& random,
    int64_t timestamp,
    const GeneratorConfig& config) {

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
