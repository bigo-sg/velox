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
#include "velox/connectors/nexmark/Event.h"
#include "velox/connectors/nexmark/pcg_random.hpp"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <string>
#include <vector>

namespace facebook::velox::connector::nexmark {


class GeneratorConfig;

class BidGenerator {
 public:
  static Bid nextBid(
      int64_t eventId,
      pcg32_fast& random,
      int64_t timestamp,
      const GeneratorConfig& config);

  static RowVectorPtr nextBidBatch(
      size_t rows,
      const FlatVector<int32_t>& eventTypeVector,
      const FlatVector<int64_t>& eventIdVector,
      pcg32_fast& random,
      const FlatVector<int64_t>& timestampVector,
      const GeneratorConfig& config,
      memory::MemoryPool* pool);

 private:
  static std::string getBaseUrl(pcg32_fast& random);
  static const std::pair<std::string, std::string>& getNextChannelAndUrl(
      pcg32_fast& random);
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
