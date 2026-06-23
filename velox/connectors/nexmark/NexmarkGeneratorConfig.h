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

#include <folly/json/dynamic.h>

#include <cstdint>
#include <vector>

namespace facebook::velox::connector::nexmark {

/// `RateUnit` defines the unit of the event rate.
enum class RateUnit { PER_SECOND = 1'000'000L, PER_MINUTE = 60'000'000L };

/// Helper function to calculate the number of microseconds between events at a
/// given rate.
inline int64_t rateToPeriodUs(RateUnit rateUnit, int64_t rate) {
  int64_t usPerUnit = static_cast<int64_t>(rateUnit);
  return (usPerUnit + rate / 2) / rate;
}

/// `RateShape` defines the shape of the event rate.
enum class RateShape { SQUARE, SINE };

inline int calcStepLengthSec(RateShape shape, int ratePeriodSec) {
  int n = 0;
  switch (shape) {
    case RateShape::SQUARE:
      n = 2;
      break;
    case RateShape::SINE:
      n = 10; // Replace `N` with the actual value
      break;
  }
  return (ratePeriodSec + n - 1) / n;
}

/// Defines the configuration options for the NexmarkGenerator.
struct NexmarkConfiguration {
  int64_t numEvents = 0;
  int numEventGenerators = 1;
  RateShape rateShape = RateShape::SQUARE;
  int firstEventRate = 10000000;
  int nextEventRate = 10000000;
  RateUnit rateUnit = RateUnit::PER_SECOND;
  int ratePeriodSec = 600;
  int preloadSeconds = 0;
  int streamTimeout = 240;
  bool isRateLimited = false;
  bool useWallclockEventTime = false;
  int personProportion = 1;
  int auctionProportion = 3;
  int bidProportion = 46;
  int avgPersonByteSize = 200;
  int avgAuctionByteSize = 500;
  int avgBidByteSize = 100;
  int hotAuctionRatio = 2;
  int hotSellersRatio = 4;
  int hotBiddersRatio = 4;
  int64_t windowSizeSec = 10;
  int64_t windowPeriodSec = 5;
  int64_t watermarkHoldbackSec = 0;
  int numInFlightAuctions = 100;
  int numActivePeople = 1000;
  int64_t occasionalDelaySec = 3;
  double probDelayedEvent = 0.1;
  int64_t outOfOrderGroupSize = 1;

  folly::dynamic serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["numEvents"] = numEvents;
    obj["numEventGenerators"] = numEventGenerators;
    obj["rateShape"] = rateShape == RateShape::SQUARE ? "SQUARE" : "SINE";
    obj["firstEventRate"] = firstEventRate;
    obj["nextEventRate"] = nextEventRate;
    obj["rateUnit"] =
        rateUnit == RateUnit::PER_SECOND ? "PER_SECOND" : "PER_MINUTE";
    obj["ratePeriodSec"] = ratePeriodSec;
    obj["preloadSeconds"] = preloadSeconds;
    obj["streamTimeout"] = streamTimeout;
    obj["isRateLimited"] = isRateLimited;
    obj["useWallclockEventTime"] = useWallclockEventTime;
    obj["personProportion"] = personProportion;
    obj["auctionProportion"] = auctionProportion;
    obj["bidProportion"] = bidProportion;
    obj["avgPersonByteSize"] = avgPersonByteSize;
    obj["avgAuctionByteSize"] = avgAuctionByteSize;
    obj["avgBidByteSize"] = avgBidByteSize;
    obj["hotAuctionRatio"] = hotAuctionRatio;
    obj["hotSellersRatio"] = hotSellersRatio;
    obj["hotBiddersRatio"] = hotBiddersRatio;
    obj["windowSizeSec"] = windowSizeSec;
    obj["windowPeriodSec"] = windowPeriodSec;
    obj["watermarkHoldbackSec"] = watermarkHoldbackSec;
    obj["numInFlightAuctions"] = numInFlightAuctions;
    obj["numActivePeople"] = numActivePeople;
    obj["occasionalDelaySec"] = occasionalDelaySec;
    obj["probDelayedEvent"] = probDelayedEvent;
    obj["outOfOrderGroupSize"] = outOfOrderGroupSize;
    return obj;
  }

  static NexmarkConfiguration deserialize(const folly::dynamic& obj) {
    NexmarkConfiguration config;

    if (obj.find("numEvents") != obj.items().end()) {
      config.numEvents = obj["numEvents"].asInt();
    }
    if (obj.find("numEventGenerators") != obj.items().end()) {
      config.numEventGenerators = obj["numEventGenerators"].asInt();
    }
    if (obj.find("rateShape") != obj.items().end()) {
      config.rateShape = obj["rateShape"].asString() == "SQUARE"
          ? RateShape::SQUARE
          : RateShape::SINE;
    }
    if (obj.find("firstEventRate") != obj.items().end()) {
      config.firstEventRate = obj["firstEventRate"].asInt();
    }
    if (obj.find("nextEventRate") != obj.items().end()) {
      config.nextEventRate = obj["nextEventRate"].asInt();
    }
    if (obj.find("rateUnit") != obj.items().end()) {
      config.rateUnit = obj["rateUnit"].asString() == "PER_SECOND"
          ? RateUnit::PER_SECOND
          : RateUnit::PER_MINUTE;
    }
    if (obj.find("ratePeriodSec") != obj.items().end()) {
      config.ratePeriodSec = obj["ratePeriodSec"].asInt();
    }
    if (obj.find("preloadSeconds") != obj.items().end()) {
      config.preloadSeconds = obj["preloadSeconds"].asInt();
    }
    if (obj.find("streamTimeout") != obj.items().end()) {
      config.streamTimeout = obj["streamTimeout"].asInt();
    }
    if (obj.find("isRateLimited") != obj.items().end()) {
      config.isRateLimited = obj["isRateLimited"].asBool();
    }
    if (obj.find("useWallclockEventTime") != obj.items().end()) {
      config.useWallclockEventTime = obj["useWallclockEventTime"].asBool();
    }
    if (obj.find("personProportion") != obj.items().end()) {
      config.personProportion = obj["personProportion"].asInt();
    }
    if (obj.find("auctionProportion") != obj.items().end()) {
      config.auctionProportion = obj["auctionProportion"].asInt();
    }
    if (obj.find("bidProportion") != obj.items().end()) {
      config.bidProportion = obj["bidProportion"].asInt();
    }
    if (obj.find("avgPersonByteSize") != obj.items().end()) {
      config.avgPersonByteSize = obj["avgPersonByteSize"].asInt();
    }
    if (obj.find("avgAuctionByteSize") != obj.items().end()) {
      config.avgAuctionByteSize = obj["avgAuctionByteSize"].asInt();
    }
    if (obj.find("avgBidByteSize") != obj.items().end()) {
      config.avgBidByteSize = obj["avgBidByteSize"].asInt();
    }
    if (obj.find("hotAuctionRatio") != obj.items().end()) {
      config.hotAuctionRatio = obj["hotAuctionRatio"].asInt();
    }
    if (obj.find("hotSellersRatio") != obj.items().end()) {
      config.hotSellersRatio = obj["hotSellersRatio"].asInt();
    }
    if (obj.find("hotBiddersRatio") != obj.items().end()) {
      config.hotBiddersRatio = obj["hotBiddersRatio"].asInt();
    }
    if (obj.find("windowSizeSec") != obj.items().end()) {
      config.windowSizeSec = obj["windowSizeSec"].asInt();
    }
    if (obj.find("windowPeriodSec") != obj.items().end()) {
      config.windowPeriodSec = obj["windowPeriodSec"].asInt();
    }
    if (obj.find("watermarkHoldbackSec") != obj.items().end()) {
      config.watermarkHoldbackSec = obj["watermarkHoldbackSec"].asInt();
    }
    if (obj.find("numInFlightAuctions") != obj.items().end()) {
      config.numInFlightAuctions = obj["numInFlightAuctions"].asInt();
    }
    if (obj.find("numActivePeople") != obj.items().end()) {
      config.numActivePeople = obj["numActivePeople"].asInt();
    }
    if (obj.find("occasionalDelaySec") != obj.items().end()) {
      config.occasionalDelaySec = obj["occasionalDelaySec"].asInt();
    }
    if (obj.find("probDelayedEvent") != obj.items().end()) {
      config.probDelayedEvent = obj["probDelayedEvent"].asDouble();
    }
    if (obj.find("outOfOrderGroupSize") != obj.items().end()) {
      config.outOfOrderGroupSize = obj["outOfOrderGroupSize"].asInt();
    }
    return config;
  }
};

class NexmarkGeneratorConfig {
 public:
  static constexpr int64_t FIRST_AUCTION_ID = 1000L;
  static constexpr int64_t FIRST_PERSON_ID = 1000L;
  static constexpr int64_t FIRST_CATEGORY_ID = 10L;

 private:
  NexmarkConfiguration configuration;
  std::vector<double> interEventDelayUs;
  int64_t stepLengthSec;
  int64_t eventsPerEpoch;

 public:
  int personProportion;
  int auctionProportion;
  int bidProportion;
  int totalProportion;

  int64_t baseTime;
  int64_t firstEventId;
  int64_t maxEventsOrZero;
  int64_t maxEvents;
  int64_t firstEventNumber;
  int64_t epochPeriodMs;

  NexmarkGeneratorConfig(
      NexmarkConfiguration configuration_,
      int64_t baseTime_,
      int64_t firstEventId_,
      int64_t maxEventsOrZero_,
      int64_t firstEventNumber_);

  folly::dynamic serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["configuration"] = configuration.serialize();
    obj["baseTime"] = baseTime;
    obj["firstEventId"] = firstEventId;
    obj["maxEvents"] = maxEventsOrZero;
    obj["firstEventNumber"] = firstEventNumber;
    return obj;
  }

  static NexmarkGeneratorConfig deserialize(const folly::dynamic& obj) {
    NexmarkConfiguration configuration =
        NexmarkConfiguration::deserialize(obj["configuration"]);
    int64_t baseTime = obj["baseTime"].asInt();
    int64_t firstEventId = obj["firstEventId"].asInt();
    int64_t maxEventsOrZero = obj["maxEvents"].asInt();
    int64_t firstEventNumber = obj["firstEventNumber"].asInt();

    return NexmarkGeneratorConfig(
        std::move(configuration),
        baseTime,
        firstEventId,
        maxEventsOrZero,
        firstEventNumber);
  }

  FOLLY_ALWAYS_INLINE int getAvgPersonByteSize() const {
    return configuration.avgPersonByteSize;
  }

  FOLLY_ALWAYS_INLINE int getNumActivePeople() const {
    return configuration.numActivePeople;
  }

  FOLLY_ALWAYS_INLINE int getHotSellersRatio() const {
    return configuration.hotSellersRatio;
  }

  FOLLY_ALWAYS_INLINE int getNumInFlightAuctions() const {
    return configuration.numInFlightAuctions;
  }

  FOLLY_ALWAYS_INLINE int getHotAuctionRatio() const {
    return configuration.hotAuctionRatio;
  }

  FOLLY_ALWAYS_INLINE int getHotBiddersRatio() const {
    return configuration.hotBiddersRatio;
  }

  FOLLY_ALWAYS_INLINE int getAvgBidByteSize() const {
    return configuration.avgBidByteSize;
  }

  FOLLY_ALWAYS_INLINE int getAvgAuctionByteSize() const {
    return configuration.avgAuctionByteSize;
  }

  FOLLY_ALWAYS_INLINE double getProbDelayedEvent() const {
    return configuration.probDelayedEvent;
  }

  FOLLY_ALWAYS_INLINE int64_t getOccasionalDelaySec() const {
    return configuration.occasionalDelaySec;
  }

  FOLLY_ALWAYS_INLINE int64_t getEstimatedSizeBytes() const {
    return estimatedBytesForEvents(maxEvents);
  }

  FOLLY_ALWAYS_INLINE int64_t estimatedBytesForEvents(int64_t numEvents) const {
    int64_t numPersons = (numEvents * personProportion) / totalProportion;
    int64_t numAuctions = (numEvents * auctionProportion) / totalProportion;
    int64_t numBids = (numEvents * bidProportion) / totalProportion;
    return numPersons * configuration.avgPersonByteSize +
        numAuctions * configuration.avgAuctionByteSize +
        numBids * configuration.avgBidByteSize;
  }

  FOLLY_ALWAYS_INLINE int64_t getStartEventId() const {
    return firstEventId + firstEventNumber;
  }

  FOLLY_ALWAYS_INLINE int64_t getStopEventId() const {
    return firstEventId + firstEventNumber + maxEvents;
  }

  FOLLY_ALWAYS_INLINE int64_t nextEventNumber(int64_t numEvents) const {
    return firstEventNumber + numEvents;
  }

  FOLLY_ALWAYS_INLINE int64_t nextAdjustedEventNumber(int64_t numEvents) const {
    int64_t n = configuration.outOfOrderGroupSize;
    int64_t eventNumber = nextEventNumber(numEvents);
    int64_t base = (eventNumber / n) * n;
    int64_t offset = (eventNumber * 953) % n;
    return base + offset;
  }

  FOLLY_ALWAYS_INLINE int64_t
  nextEventNumberForWatermark(int64_t numEvents) const {
    int64_t n = configuration.outOfOrderGroupSize;
    int64_t eventNumber = nextEventNumber(numEvents);
    return (eventNumber / n) * n;
  }

  FOLLY_ALWAYS_INLINE int64_t timestampForEvent(int64_t eventNumber) const {
    return baseTime +
        static_cast<int64_t>(eventNumber * interEventDelayUs[0]) / 1000L;
  }
};

} // namespace facebook::velox::connector::nexmark
