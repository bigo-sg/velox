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

#include "velox/connectors/nexmark/AuctionGenerator.h"
#include "velox/connectors/nexmark/Event.h"
#include "velox/connectors/nexmark/NexmarkGeneratorConfig.h"
#include "velox/connectors/nexmark/NexmarkUtils.h"
#include "velox/connectors/nexmark/PersonGenerator.h"
#include "velox/connectors/nexmark/PriceGenerator.h"
#include "velox/connectors/nexmark/StringsGenerator.h"
#include "velox/connectors/nexmark/pcg_random.hpp"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <string>
#include <vector>

namespace facebook::velox::connector::nexmark {

class NexmarkGeneratorConfig;

class BidGenerator {
 public:
  static RowVectorPtr nextBidBatch(
      size_t rows,
      const FlatVector<int32_t>& eventTypeVector,
      const FlatVector<int64_t>& eventIdVector,
      pcg32_fast& random,
      const FlatVector<int64_t>& timestampVector,
      const NexmarkGeneratorConfig& config,
      memory::MemoryPool* pool);

  FOLLY_ALWAYS_INLINE static Bid nextBid(
      int64_t eventId,
      pcg32_fast& random,
      int64_t timestamp,
      const NexmarkGeneratorConfig& config) {
    int64_t auction;
    if (random() % config.getHotAuctionRatio() > 0) {
      auction = (AuctionGenerator::lastBase0AuctionId(config, eventId) /
                 HOT_AUCTION_RATIO) *
          HOT_AUCTION_RATIO;
    } else {
      auction = AuctionGenerator::nextBase0AuctionId(eventId, random, config);
    }
    auction += NexmarkGeneratorConfig::FIRST_AUCTION_ID;

    int64_t bidder;
    if (random() % config.getHotBiddersRatio() > 0) {
      bidder = (PersonGenerator::lastBase0PersonId(config, eventId) /
                HOT_BIDDER_RATIO) *
              HOT_BIDDER_RATIO +
          1;
    } else {
      bidder = PersonGenerator::nextBase0PersonId(eventId, random, config);
    }
    bidder += NexmarkGeneratorConfig::FIRST_PERSON_ID;

    int64_t price = PriceGenerator::nextPrice(random);

    std::string channel;
    std::string url;
    if (random() % HOT_CHANNELS_RATIO > 0) {
      int i = random() % HOT_CHANNELS.size();
      channel = HOT_CHANNELS[i];
      url = getBaseUrl(random);
    } else {
      const auto& channelAndUrl = getNextChannelAndUrl(random);
      channel = channelAndUrl.first;
      url = channelAndUrl.second;
    }

    bidder += NexmarkGeneratorConfig::FIRST_PERSON_ID;

    std::string_view extra =
        StringsGenerator::nextExtra(random, 32, config.getAvgBidByteSize());

    return Bid(
        auction,
        bidder,
        price,
        std::move(channel),
        std::move(url),
        timestamp,
        std::move(extra));
  }

 private:
  FOLLY_ALWAYS_INLINE static std::string getBaseUrl(pcg32_fast& random) {
    return "https://www.nexmark.com/" +
        StringsGenerator::nextString(random, 5, '_') + '/' +
        StringsGenerator::nextString(random, 5, '_') + '/' +
        StringsGenerator::nextString(random, 5, '_') + '/' + "item.htm?query=1";
  }

  static const std::pair<std::string, std::string>& getNextChannelAndUrl(
      pcg32_fast& random);
  static std::vector<std::pair<std::string, std::string>>
  createChannelUrlCache();

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
