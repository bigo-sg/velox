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

#pragma once

#include "velox/connectors/nexmark/Event.h"
#include "velox/connectors/nexmark/GeneratorConfig.h"
#include "velox/connectors/nexmark/LongGenerator.h"
#include "velox/connectors/nexmark/NexmarkUtils.h"
#include "velox/connectors/nexmark/PersonGenerator.h"
#include "velox/connectors/nexmark/PriceGenerator.h"
#include "velox/connectors/nexmark/StringsGenerator.h"
#include "velox/connectors/nexmark/pcg_random.hpp"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <string>

namespace facebook::velox::connector::nexmark {

class GeneratorConfig;

/** AuctionGenerator */
class AuctionGenerator {
 public:
  /**
   * Keep the number of categories small so the example queries will find
   * results even with a small batch of events.
   */
  static constexpr int NUM_CATEGORIES = 5;

  /** Number of yet-to-be-created people and auction ids allowed. */
  static constexpr int AUCTION_ID_LEAD = 10;

  /**
   * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions
   * are 1 over these values.
   */
  static constexpr int HOT_SELLER_RATIO = 100;

  /** Generate and return a random auction with next available id. */
  FOLLY_ALWAYS_INLINE static Auction nextAuction(
      int64_t eventsCountSoFar,
      int64_t eventId,
      pcg32_fast& random,
      int64_t timestamp,
      const GeneratorConfig& config) {
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
        nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
    std::string name = StringsGenerator::nextString(random, 20);
    std::string desc = StringsGenerator::nextString(random, 100);
    int64_t reserve = initialBid + PriceGenerator::nextPrice(random);
    int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
    std::string_view extra = StringsGenerator::nextExtra(
        random, currentSize, config.getAvgAuctionByteSize());

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

  static RowVectorPtr nextAuctionBatch(
      size_t rows,
      int64_t eventsCountSoFar,
      const FlatVector<int32_t>& eventTypeVector,
      const FlatVector<int64_t>& eventIdVector,
      pcg32_fast& random,
      const FlatVector<int64_t>& timestampVector,
      const GeneratorConfig& config,
      memory::MemoryPool* pool);

  /**
   * Return the last valid auction id (ignoring FIRST_AUCTION_ID).
   * Will be the current auction id if due to generate an auction.
   */
  FOLLY_ALWAYS_INLINE static int64_t lastBase0AuctionId(
      const GeneratorConfig& config,
      int64_t eventId) {
    int64_t epoch = eventId / config.totalProportion;
    int64_t offset = eventId % config.totalProportion;
    if (offset < config.personProportion) {
      // About to generate a person.
      // Go back to the last auction in the last epoch.
      epoch--;
      offset = config.auctionProportion - 1;
    } else if (offset >= config.personProportion + config.auctionProportion) {
      // About to generate a bid.
      // Go back to the last auction generated in this epoch.
      offset = config.auctionProportion - 1;
    } else {
      // About to generate an auction.
      offset -= config.personProportion;
    }
    return epoch * config.auctionProportion + offset;
  }

  /** Return a random auction id (base 0). */
  FOLLY_ALWAYS_INLINE static int64_t nextBase0AuctionId(
      int64_t nextEventId,
      pcg32_fast& random,
      const GeneratorConfig& config) {
    // Choose a random auction for any of those which are likely to still be in
    // flight, plus a few 'leads'. Note that ideally we'd track non-expired
    // auctions exactly, but that state is difficult to split.
    int64_t lastAuctionId = lastBase0AuctionId(config, nextEventId);
    int64_t minAuction =
        std::max<int64_t>(lastAuctionId - config.getNumInFlightAuctions(), 0);
    int64_t maxAuction = lastAuctionId;
    return minAuction +
        LongGenerator::nextLong(
               random, maxAuction - minAuction + 1 + AUCTION_ID_LEAD);
  }

 private:
  /** Return a random time delay, in milliseconds, for length of auctions. */
  static int64_t nextAuctionLengthMs(
      int64_t eventsCountSoFar,
      pcg32_fast& random,
      int64_t timestamp,
      const GeneratorConfig& config);
};

} // namespace facebook::velox::connector::nexmark
