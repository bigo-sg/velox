#include "velox/connectors/nexmark/NexmarkGenerator.h"
#include <vector/ComplexVector.h>

#include <chrono>

namespace facebook::velox::connector::nexmark {

NextEvent NexmarkGenerator::nextEvent() {
  if (wallclockBaseTime_ < 0) {
    wallclockBaseTime_ =
        std::chrono::system_clock::now().time_since_epoch().count();
  }

  // When, in event time, we should generate the event. Monotonic.
  int64_t eventTimestamp =
      config_.timestampForEvent(config_.nextEventNumber(eventsCountSoFar_));

  // When, in event time, the event should say it was generated. Depending on
  // outOfOrderGroupSize may have local jitter.
  int64_t adjustedEventTimestamp = config_.timestampForEvent(
      config_.nextAdjustedEventNumber(eventsCountSoFar_));

  // The minimum of this and all future adjusted event timestamps. Accounts for
  // jitter in the event timestamp.
  int64_t watermark = config_.timestampForEvent(
      config_.nextEventNumberForWatermark(eventsCountSoFar_));

  // When, in wallclock time, we should emit the event.
  int64_t wallclockTimestamp =
      wallclockBaseTime_ + (eventTimestamp - config_.baseTime);

  int64_t newEventId = eventsCountSoFar_;
  int64_t rem = newEventId % config_.totalProportion;

  Event event;
  if (rem < config_.personProportion) {
    event = Event(PersonGenerator::nextPerson(
        newEventId, random_, adjustedEventTimestamp, config_));
  } else if (rem < config_.personProportion + config_.auctionProportion) {
    event = Event(AuctionGenerator::nextAuction(
        eventsCountSoFar_,
        newEventId,
        random_,
        adjustedEventTimestamp,
        config_));
  } else {
    event = Event(BidGenerator::nextBid(
        newEventId, random_, adjustedEventTimestamp, config_));
  }

  eventsCountSoFar_++;
  return NextEvent(
      wallclockTimestamp, adjustedEventTimestamp, std::move(event), watermark);
}

} // namespace facebook::velox::connector::nexmark
