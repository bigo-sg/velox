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
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <type/Type.h>
#include <string>
#include <vector>
#include <random>

namespace facebook::velox::connector::nexmark {

struct Bid {
  /** Id of auction this bid is for. */
  int64_t auction; // foreign key: Auction.id

  /** Id of person bidding in auction. */
  int64_t bidder; // foreign key: Person.id

  /** Price of bid, in cents. */
  int64_t price;

  /** The channel introduced this bidding. */
  std::string channel;

  /** The url of this bid. */
  std::string url;

  /**
   * Instant at which bid was made (ms since epoch). NOTE: This may be earlier
   * than the system's event time.
   */
  int64_t dateTime;


  /** Additional arbitrary payload for performance testing. */
  std::string extra;

  std::string toString() const {
    return "Bid{auction=" + std::to_string(auction) +
        ", bidder=" + std::to_string(bidder) +
        ", price=" + std::to_string(price) + ", channel=" + channel +
        ", url=" + url + ", dateTime=" + formatDateTime(dateTime) +
        ", extra='" + extra + '\'' + '}';
  }

  // Bid RowType
  static TypePtr createType() {
    return ROW(
        {
            "auction", // Auction ID
            "bidder", // Bidder ID
            "price", // Price
            "channel", // Channel
            "url", // Url
            "dateTime", // Timestamp
            "extra" // Additional information
        },
        {
            BIGINT(), // auction
            BIGINT(), // bidder
            BIGINT(), // price
            VARCHAR(), // channel
            VARCHAR(), // url
            TIMESTAMP(), // timestamp
            VARCHAR() // extra
        });
  }

  static RowVectorPtr createVector(int rows, memory::MemoryPool* pool) {
    auto auctionVector = BaseVector::create(BIGINT(), rows, pool);
    auto bidderVector = BaseVector::create(BIGINT(), rows, pool);
    auto priceVector = BaseVector::create(BIGINT(), rows, pool);
    auto channelVector = BaseVector::create(VARCHAR(), rows, pool);
    auto urlVector = BaseVector::create(VARCHAR(), rows, pool);
    auto dateTimeVector = BaseVector::create(TIMESTAMP(), rows, pool);
    auto extraVector = BaseVector::create(VARCHAR(), rows, pool);

    return std::make_shared<RowVector>(
        pool,
        createType(),
        nullptr,
        rows,
        std::vector<VectorPtr>{
            auctionVector,
            bidderVector,
            priceVector,
            channelVector,
            urlVector,
            dateTimeVector,
            extraVector});
  }

  static void fillVector(RowVector* bidVector, int index, const Bid* bid) {
    if (!bid) {
      bidVector->setNull(index, true);
      return;
    }

    auto auctionVector = bidVector->childAt(0)->asFlatVector<int64_t>();
    auto bidderVector = bidVector->childAt(1)->asFlatVector<int64_t>();
    auto priceVector = bidVector->childAt(2)->asFlatVector<int64_t>();
    auto channelVector = bidVector->childAt(3)->asFlatVector<StringView>();
    auto urlVector = bidVector->childAt(4)->asFlatVector<StringView>();
    auto dateTimeVector = bidVector->childAt(5)->asFlatVector<Timestamp>();
    auto extraVector = bidVector->childAt(6)->asFlatVector<StringView>();

    auctionVector->set(index, bid->auction);
    bidderVector->set(index, bid->bidder);
    priceVector->set(index, bid->price);
    channelVector->set(index, StringView(bid->channel));
    urlVector->set(index, StringView(bid->url));
    dateTimeVector->set(
        index, Timestamp(bid->dateTime / 1000, (bid->dateTime) % 1000 * 1000));
    extraVector->set(index, StringView(bid->extra));
  }
};

class GeneratorConfig;

class BidGenerator {
 public:
  static Bid nextBid(
      int64_t eventId,
      std::mt19937& random,
      int64_t timestamp,
      const GeneratorConfig& config);

 private:
  static std::string getBaseUrl(std::mt19937& random);
  static std::pair<std::string, std::string> getNextChannelAndUrl(
      std::mt19937& random);
  static std::vector<std::pair<std::string, std::string>> createChannelUrlCache();

  static constexpr int HOT_AUCTION_RATIO = 100;
  static constexpr int HOT_BIDDER_RATIO = 100;
  static constexpr int HOT_CHANNELS_RATIO = 2;
  static constexpr int CHANNELS_NUMBER = 10000;

  static inline const std::vector<std::string> HOT_CHANNELS = {
      "Google",
      "Facebook",
      "Baidu",
      "Apple"};
  static inline std::vector<std::pair<std::string, std::string>>
      CHANNEL_URL_CACHE = createChannelUrlCache();
};

} // namespace facebook::velox::connector::nexmark
