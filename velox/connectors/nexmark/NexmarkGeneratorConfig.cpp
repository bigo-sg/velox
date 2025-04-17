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

#include "velox/connectors/nexmark/NexmarkGeneratorConfig.h"

#include <cstdint>
#include <limits>

namespace facebook::velox::connector::nexmark {

NexmarkGeneratorConfig::NexmarkGeneratorConfig(
    NexmarkConfiguration configuration_,
    int64_t baseTime_,
    int64_t firstEventId_,
    int64_t maxEventsOrZero_,
    int64_t firstEventNumber_)
    : configuration(std::move(configuration_)),
      baseTime(baseTime_),
      firstEventId(firstEventId_),
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

int NexmarkGeneratorConfig::getAvgPersonByteSize() const {
  return configuration.avgPersonByteSize;
}

int NexmarkGeneratorConfig::getNumActivePeople() const {
  return configuration.numActivePeople;
}

int NexmarkGeneratorConfig::getHotSellersRatio() const {
  return configuration.hotSellersRatio;
}

int NexmarkGeneratorConfig::getNumInFlightAuctions() const {
  return configuration.numInFlightAuctions;
}

int NexmarkGeneratorConfig::getHotAuctionRatio() const {
  return configuration.hotAuctionRatio;
}

int NexmarkGeneratorConfig::getHotBiddersRatio() const {
  return configuration.hotBiddersRatio;
}

int NexmarkGeneratorConfig::getAvgBidByteSize() const {
  return configuration.avgBidByteSize;
}

int NexmarkGeneratorConfig::getAvgAuctionByteSize() const {
  return configuration.avgAuctionByteSize;
}

double NexmarkGeneratorConfig::getProbDelayedEvent() const {
  return configuration.probDelayedEvent;
}

int64_t NexmarkGeneratorConfig::getOccasionalDelaySec() const {
  return configuration.occasionalDelaySec;
}

int64_t NexmarkGeneratorConfig::getEstimatedSizeBytes() const {
  return estimatedBytesForEvents(maxEvents);
}

int64_t NexmarkGeneratorConfig::estimatedBytesForEvents(int64_t numEvents) const {
  int64_t numPersons = (numEvents * personProportion) / totalProportion;
  int64_t numAuctions = (numEvents * auctionProportion) / totalProportion;
  int64_t numBids = (numEvents * bidProportion) / totalProportion;
  return numPersons * configuration.avgPersonByteSize +
      numAuctions * configuration.avgAuctionByteSize +
      numBids * configuration.avgBidByteSize;
}

int64_t NexmarkGeneratorConfig::getStartEventId() const {
  return firstEventId + firstEventNumber;
}

int64_t NexmarkGeneratorConfig::getStopEventId() const {
  return firstEventId + firstEventNumber + maxEvents;
}

int64_t NexmarkGeneratorConfig::nextEventNumber(int64_t numEvents) const {
  return firstEventNumber + numEvents;
}

int64_t NexmarkGeneratorConfig::nextAdjustedEventNumber(int64_t numEvents) const {
  int64_t n = configuration.outOfOrderGroupSize;
  int64_t eventNumber = nextEventNumber(numEvents);
  int64_t base = (eventNumber / n) * n;
  int64_t offset = (eventNumber * 953) % n;
  return base + offset;
}

int64_t NexmarkGeneratorConfig::nextEventNumberForWatermark(int64_t numEvents) const {
  int64_t n = configuration.outOfOrderGroupSize;
  int64_t eventNumber = nextEventNumber(numEvents);
  return (eventNumber / n) * n;
}

int64_t NexmarkGeneratorConfig::timestampForEvent(int64_t eventNumber) const {
  return baseTime +
      static_cast<int64_t>(eventNumber * interEventDelayUs[0]) / 1000L;
}

} // namespace facebook::velox::connector::nexmark
