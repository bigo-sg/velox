#include "velox/connectors/nexmark/NexmarkGenerator.h"

#include <chrono>

namespace facebook::velox::connector::nexmark {

FOLLY_ALWAYS_INLINE int64_t
NexmarkGenerator::getNextEventId(int64_t eventsCountSoFar) const {
  return config_.firstEventId +
      config_.nextAdjustedEventNumber(eventsCountSoFar);
}

std::pair<RowVectorPtr, int64_t> NexmarkGenerator::nextBatch(size_t rows) {
  if (wallclockBaseTime_ < 0) {
    wallclockBaseTime_ =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
  }

  rows = std::min<size_t>(rows, config_.maxEvents - eventsCountSoFar_);

  auto eventVector = Event::createVector(rows, pool_);

  int64_t maxWallclockTimestamp = 0;
  for (size_t i = 0; i < rows; ++i) {
    int64_t eventTimestamp = config_.timestampForEvent(
        config_.nextEventNumber(eventsCountSoFar_ + i));
    int64_t wallclockTimestamp =
        wallclockBaseTime_ + (eventTimestamp - config_.baseTime);
    maxWallclockTimestamp = std::max(maxWallclockTimestamp, wallclockTimestamp);
  }

  auto typeVector = eventVector->childAt(0)->asFlatVector<int32_t>();
  auto eventIdVector = BaseVector::create(BIGINT(), rows, pool_);
  auto concreteEventIdVector = eventIdVector->asFlatVector<int64_t>();
  for (size_t i = 0; i < rows; ++i) {
    int64_t newEventId = getNextEventId(eventsCountSoFar_ + i);
    int64_t rem = newEventId % config_.totalProportion;

    Event::Type type;
    if (rem < config_.personProportion) {
      type = Event::Type::PERSON;
    } else if (rem < config_.personProportion + config_.auctionProportion) {
      type = Event::Type::AUCTION;
    } else {
      type = Event::Type::BID;
    }

    concreteEventIdVector->set(i, newEventId);
    typeVector->set(i, static_cast<int32_t>(type));
  }

  auto adjustedEventTimestampVector = BaseVector::create(BIGINT(), rows, pool_);
  auto concreteAdjustedEventTimestampVector =
      adjustedEventTimestampVector->asFlatVector<int64_t>();
  for (size_t i = 0; i < rows; ++i) {
    int64_t adjustedEventTimestamp =
        config_.timestampForEvent(config_.nextAdjustedEventNumber(
            eventsCountSoFar_ + i));
    concreteAdjustedEventTimestampVector->set(i, adjustedEventTimestamp);
  }

  eventVector->childAt(1) = PersonGenerator::nextPersonBatch(
      rows,
      *concreteEventIdVector,
      random_,
      *concreteAdjustedEventTimestampVector,
      config_);

  eventVector->childAt(2) = AuctionGenerator::nextAuctionBatch(
      rows,
      eventsCountSoFar_,
      *concreteEventIdVector,
      random_,
      *concreteAdjustedEventTimestampVector,
      config_);

  eventVector->childAt(3) = BidGenerator::nextBidBatch(
      rows,
      *concreteEventIdVector,
      random_,
      *concreteAdjustedEventTimestampVector,
      config_);

  eventsCountSoFar_ += rows;
  return {eventVector, maxWallclockTimestamp};
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

  int64_t newEventId = getNextEventId(eventsCountSoFar_);
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
