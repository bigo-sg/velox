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

#include "velox/connectors/nexmark/NexmarkUtils.h"
#include "velox/connectors/nexmark/pcg_random.hpp"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <string>

namespace facebook::velox::connector::nexmark {

/** An auction submitted by a person. */
struct Auction {
  /** Id of auction. */
  int64_t id; // primary key

  /** Extra auction properties. */
  std::string itemName;

  std::string description;

  /** Initial bid price, in cents. */
  int64_t initialBid;

  /** Reserve price, in cents. */
  int64_t reserve;

  int64_t dateTime;

  /** When does auction expire? (ms since epoch). Bids at or after this time are
   * ignored. */
  int64_t expires;

  /** Id of person who instigated auction. */
  int64_t seller; // foreign key: Person.id

  /** Id of category auction is listed under. */
  int64_t category; // foreign key: Category.id

  /** Additional arbitrary payload for performance testing. */
  std::string_view extra;

  /** Constructor with all fields */
  Auction(
      int64_t id,
      std::string itemName,
      std::string description,
      int64_t initialBid,
      int64_t reserve,
      int64_t dateTime,
      int64_t expires,
      int64_t seller,
      int64_t category,
      std::string_view extra)
      : id(id),
        itemName(std::move(itemName)),
        description(std::move(description)),
        initialBid(initialBid),
        reserve(reserve),
        dateTime(dateTime),
        expires(expires),
        seller(seller),
        category(category),
        extra(std::move(extra)) {}

        std::string toString() const {
          return "Auction{id=" + std::to_string(id) + ", itemName='" +
              itemName + '\'' + ", description='" + description + '\'' +
              ", initialBid=" + std::to_string(initialBid) +
              ", reserve=" + std::to_string(reserve) +
              ", dateTime=" + formatDateTime(dateTime) +
              ", expires=" + formatDateTime(expires) +
              ", seller=" + std::to_string(seller) +
              ", category=" + std::to_string(category) + ", extra='" +
              std::string(extra) + '\'' + '}';
        }

  // Auction RowType
  static TypePtr createType() {
    return ROW(
        {
            "id", // Auction ID
            "name", // Auction name
            "description", // Description
            "initialBid", // Initial bid
            "reserve", // Reserve price
            "dateTime", // Timestamp
            "expires", // Expiration time
            "seller", // Seller ID
            "category", // Category
            "extra" // Additional information
        },
        {
            BIGINT(), // id
            VARCHAR(), // name
            VARCHAR(), // description
            BIGINT(), // initialBid
            BIGINT(), // reserve
            TIMESTAMP(), // timestamp
            TIMESTAMP (), // expires
            BIGINT(), // seller
            BIGINT(), // category
            VARCHAR() // extra
        });
  }

  static RowVectorPtr createVector(int rows, memory::MemoryPool* pool) {
    auto idVector = BaseVector::create(BIGINT(), rows, pool);
    auto nameVector = BaseVector::create(VARCHAR(), rows, pool);
    auto descVector = BaseVector::create(VARCHAR(), rows, pool);
    auto initialBidVector = BaseVector::create(BIGINT(), rows, pool);
    auto reserveVector = BaseVector::create(BIGINT(), rows, pool);
    auto dateTimeVector = BaseVector::create(TIMESTAMP(), rows, pool);
    auto expiresVector = BaseVector::create(TIMESTAMP(), rows, pool);
    auto sellerVector = BaseVector::create(BIGINT(), rows, pool);
    auto categoryVector = BaseVector::create(BIGINT(), rows, pool);
    auto extraVector = BaseVector::create(VARCHAR(), rows, pool);

    return std::make_shared<RowVector>(
        pool,
        createType(),
        nullptr,
        rows,
        std::vector<VectorPtr>{
            idVector,
            nameVector,
            descVector,
            initialBidVector,
            reserveVector,
            dateTimeVector,
            expiresVector,
            sellerVector,
            categoryVector,
            extraVector});
  }

  static void fillVector(
      RowVector* auctionVector,
      int index,
      const Auction* auction) {
    if (!auction) {
      auctionVector->setNull(index, true);
      return;
    }

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

    idVector->set(index, auction->id);
    nameVector->set(index, StringView(auction->itemName));
    descVector->set(index, StringView(auction->description));
    initialBidVector->set(index, auction->initialBid);
    reserveVector->set(index, auction->reserve);
    dateTimeVector->set(
        index,
        Timestamp(auction->dateTime / 1000, (auction->dateTime) % 1000 * 1000));
    expiresVector->set(
        index,
        Timestamp(auction->expires / 1000, (auction->expires) % 1000 * 1000));
    sellerVector->set(index, auction->seller);
    categoryVector->set(index, auction->category);
    extraVector->set(index, StringView(auction->extra));
  }
};

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
  static Auction nextAuction(
      int64_t eventsCountSoFar,
      int64_t eventId,
      pcg32_fast& random,
      int64_t timestamp,
      const GeneratorConfig& config);

  /**
   * Return the last valid auction id (ignoring FIRST_AUCTION_ID).
   * Will be the current auction id if due to generate an auction.
   */
  static int64_t lastBase0AuctionId(
      const GeneratorConfig& config,
      int64_t eventId);

  /** Return a random auction id (base 0). */
  static int64_t nextBase0AuctionId(
      int64_t nextEventId,
      pcg32_fast& random,
      const GeneratorConfig& config);

 private:
  /** Return a random time delay, in milliseconds, for length of auctions. */
  static int64_t nextAuctionLengthMs(
      int64_t eventsCountSoFar,
      pcg32_fast& random,
      int64_t timestamp,
      const GeneratorConfig& config);
};

} // namespace facebook::velox::connector::nexmark
