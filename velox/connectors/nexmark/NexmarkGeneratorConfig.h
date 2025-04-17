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
  int firstEventRate = 10000;
  int nextEventRate = 10000;
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
  int64_t maxEvents;
  int64_t firstEventNumber;
  int64_t epochPeriodMs;

  NexmarkGeneratorConfig(
      NexmarkConfiguration configuration_,
      int64_t baseTime_,
      int64_t firstEventId_,
      int64_t maxEventsOrZero_,
      int64_t firstEventNumber_);

  int getAvgPersonByteSize() const;
  int getNumActivePeople() const;
  int getHotSellersRatio() const;
  int getNumInFlightAuctions() const;
  int getHotAuctionRatio() const;
  int getHotBiddersRatio() const;
  int getAvgBidByteSize() const;
  int getAvgAuctionByteSize() const;
  double getProbDelayedEvent() const;
  int64_t getOccasionalDelaySec() const;
  int64_t getEstimatedSizeBytes() const;
  int64_t estimatedBytesForEvents(int64_t numEvents) const;
  int64_t getStartEventId() const;
  int64_t getStopEventId() const;
  int64_t nextEventNumber(int64_t numEvents) const;
  int64_t nextAdjustedEventNumber(int64_t numEvents) const;
  int64_t nextEventNumberForWatermark(int64_t numEvents) const;
  int64_t timestampForEvent(int64_t eventNumber) const;
};

} // namespace facebook::velox::connector::nexmark
