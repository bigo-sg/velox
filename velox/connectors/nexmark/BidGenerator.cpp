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
#include "velox/connectors/nexmark/BidGenerator.h"
#include "velox/connectors/nexmark/AuctionGenerator.h"
#include "velox/connectors/nexmark/GeneratorConfig.h"
#include "velox/connectors/nexmark/PersonGenerator.h"
#include "velox/connectors/nexmark/PriceGenerator.h"
#include "velox/connectors/nexmark/StringsGenerator.h"


namespace facebook::velox::connector::nexmark {

Bid BidGenerator::nextBid(
    int64_t eventId,
    pcg32_fast& random,
    int64_t timestamp,
    const GeneratorConfig& config) {
  int64_t auction;
  if (random() % config.getHotAuctionRatio() > 0) {
    auction = (AuctionGenerator::lastBase0AuctionId(config, eventId) /
               HOT_AUCTION_RATIO) *
        HOT_AUCTION_RATIO;
  } else {
    auction = AuctionGenerator::nextBase0AuctionId(eventId, random, config);
  }
  auction += GeneratorConfig::FIRST_AUCTION_ID;

  int64_t bidder;
  if (random() % config.getHotBiddersRatio() > 0) {
    bidder = (PersonGenerator::lastBase0PersonId(config, eventId) /
              HOT_BIDDER_RATIO) *
            HOT_BIDDER_RATIO +
        1;
  } else {
    bidder = PersonGenerator::nextBase0PersonId(eventId, random, config);
  }
  bidder += GeneratorConfig::FIRST_PERSON_ID;

  int64_t price = PriceGenerator::nextPrice(random);

  std::string channel;
  std::string url;
  if (random() % HOT_CHANNELS_RATIO > 0) {
    int i = random() % HOT_CHANNELS.size();
    channel = HOT_CHANNELS[i];
    url = getBaseUrl(random);
  } else {
    const auto & channelAndUrl = getNextChannelAndUrl(random);
    channel = channelAndUrl.first;
    url = channelAndUrl.second;
  }

  bidder += GeneratorConfig::FIRST_PERSON_ID;

  int currentSize = 8 + 8 + 8 + 8;
  std::string_view extra = StringsGenerator::nextExtra(
      random, currentSize, config.getAvgBidByteSize());

  return Bid(
      auction,
      bidder,
      price,
      std::move(channel),
      std::move(url),
      timestamp,
      std::move(extra));
}

std::string BidGenerator::getBaseUrl(pcg32_fast& random) {
  return "https://www.nexmark.com/" +
      StringsGenerator::nextString(random, 5, '_') + '/' +
      StringsGenerator::nextString(random, 5, '_') + '/' +
      StringsGenerator::nextString(random, 5, '_') + '/' + "item.htm?query=1";
}

std::vector<std::pair<std::string, std::string>>
BidGenerator::createChannelUrlCache() {
  pcg32_fast random;
  std::vector<std::pair<std::string, std::string>> cache;
  cache.resize(CHANNELS_NUMBER);
  for (int i = 0; i < CHANNELS_NUMBER; ++i) {
    std::string url = getBaseUrl(random);
    if (random() % 10 > 0) {
      url += "&channel_id=" + std::to_string(std::abs(~i));
    }
    cache[i] = {"channel-" + std::to_string(i), url};
  }
  return cache;
}

const std::pair<std::string, std::string>& BidGenerator::getNextChannelAndUrl(
    pcg32_fast& random) {
  int channelNumber = random() % CHANNELS_NUMBER;
  return CHANNEL_URL_CACHE[channelNumber];
}

} // namespace facebook::velox::connector::nexmark
