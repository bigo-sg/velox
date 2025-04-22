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

#include "velox/connectors/nexmark/GeneratorConfig.h"

#include <cstdint>
#include <limits>

namespace facebook::velox::connector::nexmark {

GeneratorConfig::GeneratorConfig(
    NexmarkConfiguration configuration_,
    int64_t baseTime_,
    int64_t firstEventId_,
    int64_t maxEventsOrZero_,
    int64_t firstEventNumber_)
    : configuration(std::move(configuration_)),
      baseTime(baseTime_),
      firstEventId(firstEventId_),
      maxEventsOrZero(maxEventsOrZero_),
      firstEventNumber(firstEventNumber_) {
  auctionProportion = configuration.auctionProportion;
  personProportion = configuration.personProportion;
  bidProportion = configuration.bidProportion;
  totalProportion = auctionProportion + personProportion + bidProportion;

  interEventDelayUs.resize(1);
  interEventDelayUs[0] = 1'000'000.0 / configuration.firstEventRate *
      configuration.numEventGenerators;
  stepLengthSec = calcStepLengthSec(
      configuration.rateShape, configuration.ratePeriodSec);

  if (maxEventsOrZero_ == 0) {
    maxEvents = std::numeric_limits<int64_t>::max() /
        (totalProportion *
         std::max(
             configuration.avgAuctionByteSize,
             std::max(
                 configuration.avgBidByteSize,
                 configuration.avgPersonByteSize)));
  } else {
    maxEvents = maxEventsOrZero_;
  }

  eventsPerEpoch = 0;
  epochPeriodMs = 0;
}

FOLLY_NOINLINE int GeneratorConfig::getAvgPersonByteSize() const {
  return configuration.avgPersonByteSize;
}

FOLLY_NOINLINE int GeneratorConfig::getNumActivePeople() const {
  return configuration.numActivePeople;
}

FOLLY_NOINLINE int GeneratorConfig::getHotSellersRatio() const {
  return configuration.hotSellersRatio;
}

FOLLY_NOINLINE int GeneratorConfig::getNumInFlightAuctions() const {
  return configuration.numInFlightAuctions;
}

FOLLY_NOINLINE int GeneratorConfig::getHotAuctionRatio() const {
  return configuration.hotAuctionRatio;
}

FOLLY_NOINLINE int GeneratorConfig::getHotBiddersRatio() const {
  return configuration.hotBiddersRatio;
}

FOLLY_NOINLINE int GeneratorConfig::getAvgBidByteSize() const {
  return configuration.avgBidByteSize;
}

FOLLY_NOINLINE int GeneratorConfig::getAvgAuctionByteSize() const {
  return configuration.avgAuctionByteSize;
}

FOLLY_NOINLINE double GeneratorConfig::getProbDelayedEvent() const {
  return configuration.probDelayedEvent;
}

FOLLY_NOINLINE int64_t GeneratorConfig::getOccasionalDelaySec() const {
  return configuration.occasionalDelaySec;
}

FOLLY_NOINLINE int64_t GeneratorConfig::getEstimatedSizeBytes() const {
  return estimatedBytesForEvents(maxEvents);
}

FOLLY_NOINLINE int64_t GeneratorConfig::estimatedBytesForEvents(int64_t numEvents) const {
  int64_t numPersons = (numEvents * personProportion) / totalProportion;
  int64_t numAuctions = (numEvents * auctionProportion) / totalProportion;
  int64_t numBids = (numEvents * bidProportion) / totalProportion;
  return numPersons * configuration.avgPersonByteSize +
      numAuctions * configuration.avgAuctionByteSize +
      numBids * configuration.avgBidByteSize;
}

FOLLY_NOINLINE int64_t GeneratorConfig::getStartEventId() const {
  return firstEventId + firstEventNumber;
}

FOLLY_NOINLINE int64_t GeneratorConfig::getStopEventId() const {
  return firstEventId + firstEventNumber + maxEvents;
}

FOLLY_NOINLINE int64_t GeneratorConfig::nextEventNumber(int64_t numEvents) const {
  return firstEventNumber + numEvents;
}

FOLLY_NOINLINE int64_t GeneratorConfig::nextAdjustedEventNumber(int64_t numEvents) const {
  int64_t n = configuration.outOfOrderGroupSize;
  int64_t eventNumber = nextEventNumber(numEvents);
  int64_t base = (eventNumber / n) * n;
  int64_t offset = (eventNumber * 953) % n;
  return base + offset;
}

FOLLY_NOINLINE int64_t GeneratorConfig::nextEventNumberForWatermark(int64_t numEvents) const {
  int64_t n = configuration.outOfOrderGroupSize;
  int64_t eventNumber = nextEventNumber(numEvents);
  return (eventNumber / n) * n;
}

FOLLY_NOINLINE int64_t GeneratorConfig::timestampForEvent(int64_t eventNumber) const {
  return baseTime +
      static_cast<int64_t>(eventNumber * interEventDelayUs[0]) / 1000L;
}

} // namespace facebook::velox::connector::nexmark
