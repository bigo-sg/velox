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

#include <memory>

#include "velox/connectors/nexmark/AuctionGenerator.h"
#include "velox/connectors/nexmark/BidGenerator.h"
#include "velox/connectors/nexmark/GeneratorConfig.h"
#include "velox/connectors/nexmark/PersonGenerator.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::connector::nexmark {

/// Represents an event containing either a Person, an Auction, or a Bid.
class Event {
 public:
  enum class Type { PERSON = 0, AUCTION = 1, BID = 2 };

  Event() = default;

  Event(Person person)
      : person_(std::make_unique<Person>(std::move(person))),
        auction_(nullptr),
        bid_(nullptr),
        type_(Type::PERSON) {}

  Event(Auction auction)
      : person_(nullptr),
        auction_(std::make_unique<Auction>(std::move(auction))),
        bid_(nullptr),
        type_(Type::AUCTION) {}

  Event(Bid bid)
      : person_(nullptr),
        auction_(nullptr),
        bid_(std::make_unique<Bid>(std::move(bid))),
        type_(Type::BID) {}

  Type getType() const {
    return type_;
  }

  const Person* getPerson() const {
    return person_.get();
  }

  const Auction* getAuction() const {
    return auction_.get();
  }

  const Bid* getBid() const {
    return bid_.get();
  }

  std::string toString() const {
    switch (type_) {
      case Type::PERSON:
        return person_->toString();
      case Type::AUCTION:
        return auction_->toString();
      case Type::BID:
        return bid_->toString();
    }
    return "";
  }

  // Event RowType
  static TypePtr createType() {
    return ROW(
        {
            "event_type", // Event type
            "person", // Person object
            "auction", // Auction object
            "bid" // Bid object
        },
        {
            INTEGER(), // type (Event::Type enum)
            Person::createType(), // person
            Auction::createType(), // auction
            Bid::createType() // bid
        });
  }

  static RowVectorPtr createVector(int rows, memory::MemoryPool* pool) {
    auto typeVector = BaseVector::create(INTEGER(), rows, pool);
    auto personVector = Person::createVector(rows, pool);
    auto auctionVector = Auction::createVector(rows, pool);
    auto bidVector = Bid::createVector(rows, pool);

    return std::make_shared<RowVector>(
        pool,
        createType(),
        nullptr,
        rows,
        std::vector<VectorPtr>{typeVector, personVector, auctionVector, bidVector});
  }

  static void fillVector(RowVector* eventVector, int index, const Event& event) {
    auto typeVector = eventVector->childAt(0)->asFlatVector<int32_t>();
    auto personVector = eventVector->childAt(1)->as<RowVector>();
    auto auctionVector = eventVector->childAt(2)->as<RowVector>();
    auto bidVector = eventVector->childAt(3)->as<RowVector>();

    typeVector->set(index, static_cast<int32_t>(event.getType()));
    Person::fillVector(personVector, index, event.getPerson());
    Auction::fillVector(auctionVector, index, event.getAuction());
    Bid::fillVector(bidVector, index, event.getBid());
  }

 private:
  std::unique_ptr<Person> person_;
  std::unique_ptr<Auction> auction_;
  std::unique_ptr<Bid> bid_;
  Type type_;
};

/// Represents the next event to be emitted, including its wallclock timestamp,
/// event timestamp, the event itself, and the watermark.
class NextEvent {
 public:
  NextEvent(
      int64_t wallclockTimestamp,
      int64_t eventTimestamp,
      Event event,
      int64_t watermark)
      : wallclockTimestamp_(wallclockTimestamp),
        eventTimestamp_(eventTimestamp),
        event_(std::move(event)),
        watermark_(watermark) {}

  /// When, in wallclock time, should this event be emitted?
  int64_t getWallclockTimestamp() const {
    return wallclockTimestamp_;
  }

  /// When, in event time, should this event be considered to have occurred?
  int64_t getEventTimestamp() const {
    return eventTimestamp_;
  }

  /// The event itself.
  const Event& getEvent() const {
    return event_;
  }

  /// The minimum of this and all future event timestamps.
  int64_t getWatermark() const {
    return watermark_;
  }

 private:
  int64_t wallclockTimestamp_;
  int64_t eventTimestamp_;
  Event event_;
  int64_t watermark_;
};

/// `NexmarkGenerator` is the c++ implements of Flink NexmarkGenerator.
/// https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/NexmarkGenerator.java
class NexmarkGenerator {
 public:
  NexmarkGenerator(
      const GeneratorConfig& config,
      int64_t eventsCountSoFar,
      int64_t wallclockBaseTime)
      : config_(config),
        eventsCountSoFar_(eventsCountSoFar),
        wallclockBaseTime_(wallclockBaseTime) {}

  ~NexmarkGenerator() = default;

  bool hasNext() const {
    return eventsCountSoFar_ < config_.maxEvents;
  }

  NextEvent next();

 private:
  FOLLY_NOINLINE int64_t getNextEventId() const;

  const GeneratorConfig config_;
  int64_t eventsCountSoFar_;
  int64_t wallclockBaseTime_;
  std::mt19937 random_;
};

} // namespace facebook::velox::connector::nexmark
