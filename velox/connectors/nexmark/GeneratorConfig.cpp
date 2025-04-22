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

} // namespace facebook::velox::connector::nexmark
