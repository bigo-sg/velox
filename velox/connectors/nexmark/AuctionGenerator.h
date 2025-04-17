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

#include <random>
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
  std::string extra;

  /** Constructor with all fields */
  Auction(
      int64_t id,
      const std::string& itemName,
      const std::string& description,
      int64_t initialBid,
      int64_t reserve,
      int64_t dateTime,
      int64_t expires,
      int64_t seller,
      int64_t category,
      const std::string& extra)
      : id(id),
        itemName(itemName),
        description(description),
        initialBid(initialBid),
        reserve(reserve),
        dateTime(dateTime),
        expires(expires),
        seller(seller),
        category(category),
        extra(extra) {}
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
      std::mt19937& random,
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
      std::mt19937& random,
      const GeneratorConfig& config);

 private:
  /** Return a random time delay, in milliseconds, for length of auctions. */
  static int64_t nextAuctionLengthMs(
      int64_t eventsCountSoFar,
      std::mt19937& random,
      int64_t timestamp,
      const GeneratorConfig& config);
};

} // namespace facebook::velox::connector::nexmark
