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

#include "velox/connectors/nexmark/PersonGenerator.h"
#include "velox/connectors/nexmark/AuctionGenerator.h"
#include "velox/connectors/nexmark/BidGenerator.h"
#include "velox/connectors/nexmark/GeneratorConfig.h"

namespace facebook::velox::connector::nexmark {

/// Represents an event containing either a Person, an Auction, or a Bid.
class Event {
 public:
  enum class Type { PERSON = 0, AUCTION = 1, BID = 2 };

  Event() = default;

  Event(Person newPerson)
      : newPerson_(std::make_unique<Person>(std::move(newPerson))),
        newAuction_(nullptr),
        bid_(nullptr),
        type_(Type::PERSON) {}

  Event(Auction newAuction)
      : newPerson_(nullptr),
        newAuction_(std::make_unique<Auction>(std::move(newAuction))),
        bid_(nullptr),
        type_(Type::AUCTION) {}

  Event(Bid bid)
      : newPerson_(nullptr),
        newAuction_(nullptr),
        bid_(std::make_unique<Bid>(std::move(bid))),
        type_(Type::BID) {}

  Type getType() const {
    return type_; }

    const Person* getPerson() const { return newPerson_.get(); }
    const Auction* getAuction() const { return newAuction_.get(); }
    const Bid* getBid() const { return bid_.get(); }

 private:
    std::unique_ptr<Person> newPerson_;
    std::unique_ptr<Auction> newAuction_;
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
      GeneratorConfig config,
      int64_t eventsCountSoFar,
      int64_t wallclockBaseTime)
      : config_(std::move(config)),
        eventsCountSoFar_(eventsCountSoFar),
        wallclockBaseTime_(wallclockBaseTime) {}

  ~NexmarkGenerator() = default;

//   VectorPtr nextEvent(int rows);

 private:
  NextEvent nextEvent();

  GeneratorConfig config_;
  int64_t eventsCountSoFar_;
  int64_t wallclockBaseTime_;
  std::mt19937 random_;
};



} // namespace facebook::velox::connector::nexmark
