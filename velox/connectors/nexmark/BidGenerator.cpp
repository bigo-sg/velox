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

RowVectorPtr BidGenerator::nextBidBatch(
    size_t rows,
    const FlatVector<int32_t>& eventTypeVector,
    const FlatVector<int64_t>& eventIdVector,
    pcg32_fast& random,
    const FlatVector<int64_t>& timestampVector,
    const GeneratorConfig& config,
    memory::MemoryPool* pool) {
  auto bidVector = Bid::createVector(rows, pool);
  auto auctionVector = bidVector->childAt(0)->asFlatVector<int64_t>();
  auto bidderVector = bidVector->childAt(1)->asFlatVector<int64_t>();
  auto priceVector = bidVector->childAt(2)->asFlatVector<int64_t>();
  auto channelVector = bidVector->childAt(3)->asFlatVector<StringView>();
  auto urlVector = bidVector->childAt(4)->asFlatVector<StringView>();
  auto dateTimeVector = bidVector->childAt(5)->asFlatVector<Timestamp>();
  auto extraVector = bidVector->childAt(6)->asFlatVector<StringView>();

  for (size_t i = 0; i < rows; ++i) {
    auto eventType = static_cast<Event::Type>(eventTypeVector.valueAt(i));
    if (Event::Type::BID != eventType) {
      bidVector->setNull(i, true);
      continue;
    }

    auto eventId = eventIdVector.valueAt(i);
    auto timestamp = timestampVector.valueAt(i);
    auto bid = nextBid(eventId, random, timestamp, config);
    auctionVector->set(i, bid.auction);
    bidderVector->set(i, bid.bidder);
    priceVector->set(i, bid.price);
    channelVector->set(i, StringView(bid.channel));
    urlVector->set(i, StringView(bid.url));
    dateTimeVector->set(
        i, Timestamp(bid.dateTime / 1000, bid.dateTime % 1000 * 1000));
    extraVector->set(i, StringView(bid.extra));

    /*

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
      const auto& channelAndUrl = getNextChannelAndUrl(random);
      channel = channelAndUrl.first;
      url = channelAndUrl.second;
    }

    bidder += GeneratorConfig::FIRST_PERSON_ID;

    std::string_view extra = StringsGenerator::nextExtra(
        random, 32, config.getAvgBidByteSize());
    */
  }

  return bidVector;
}

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
    const auto& channelAndUrl = getNextChannelAndUrl(random);
    channel = channelAndUrl.first;
    url = channelAndUrl.second;
  }

  bidder += GeneratorConfig::FIRST_PERSON_ID;

  std::string_view extra = StringsGenerator::nextExtra(
      random, 32, config.getAvgBidByteSize());

  return Bid(
      auction,
      bidder,
      price,
      std::move(channel),
      std::move(url),
      timestamp,
      std::move(extra));
}

FOLLY_ALWAYS_INLINE std::string BidGenerator::getBaseUrl(pcg32_fast& random) {
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
