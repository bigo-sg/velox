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
};

class NexmarkGeneratorConfig;

class BidGenerator {
 public:
  static Bid nextBid(
      int64_t eventId,
      std::mt19937& random,
      int64_t timestamp,
      const NexmarkGeneratorConfig& config);

 private:
  static std::string getBaseUrl(std::mt19937& random);
  static std::pair<std::string, std::string> getNextChannelAndUrl(
      std::mt19937& random);
  static void createChannelUrlCache(std::mt19937& random);

  static constexpr int HOT_AUCTION_RATIO = 100;
  static constexpr int HOT_BIDDER_RATIO = 100;
  static constexpr int HOT_CHANNELS_RATIO = 2;
  static constexpr int CHANNELS_NUMBER = 10000;

  static inline const std::vector<std::string> HOT_CHANNELS = {"Google", "Facebook", "Baidu", "Apple"};
  static inline std::vector<std::pair<std::string, std::string>> CHANNEL_URL_CACHE;
};

} // namespace facebook::velox::connector::nexmark
