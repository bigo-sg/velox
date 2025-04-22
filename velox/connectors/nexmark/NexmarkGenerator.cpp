#include "velox/connectors/nexmark/NexmarkGenerator.h"

#include <chrono>

namespace facebook::velox::connector::nexmark {

FOLLY_ALWAYS_INLINE int64_t NexmarkGenerator::getNextEventId() const {
  return config_.firstEventId +
      config_.nextAdjustedEventNumber(eventsCountSoFar_);
}

NextEvent NexmarkGenerator::next() {
  if (wallclockBaseTime_ < 0) {
    wallclockBaseTime_ =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
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

  int64_t newEventId = getNextEventId();
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
