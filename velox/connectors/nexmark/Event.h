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

#include "velox/connectors/nexmark/NexmarkUtils.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::nexmark {

/** A person either creating an auction or making a bid. */
struct Person {
 public:
  /** Id of person. */
  int64_t id; // primary key

  /** Extra person properties. */
  std::string name;
  std::string emailAddress;
  std::string creditCard;
  std::string city;
  std::string state;
  int64_t dateTime; // unit: ms

  /** Additional arbitrary payload for performance testing. */
  std::string_view extra;

  Person(
      int64_t id,
      std::string name,
      std::string emailAddress,
      std::string creditCard,
      std::string city,
      std::string state,
      int64_t dateTime,
      std::string_view extra)
      : id(id),
        name(std::move(name)),
        emailAddress(std::move(emailAddress)),
        creditCard(std::move(creditCard)),
        city(std::move(city)),
        state(std::move(state)),
        dateTime(dateTime),
        extra(std::move(extra)) {}

  std::string toString() const {
    return "Person{id=" + std::to_string(id) + ", name='" + name + '\'' +
        ", emailAddress='" + emailAddress + '\'' + ", creditCard='" +
        creditCard + '\'' + ", city='" + city + '\'' + ", state='" + state +
        '\'' + ", dateTime=" + formatDateTime(dateTime) + ", extra='" +
        std::string(extra) + '\'' + '}';
  }

  // Person RowType
  static TypePtr createType() {
    return ROW(
        {
            "id", // Person ID
            "name", // Name
            "emailAddress", // Email
            "creditCard", // Credit Card
            "city", // City
            "state", // State/Province
            "dateTime", // State/Province
            "extra" // Extra Information
        },
        {
            BIGINT(), // id
            VARCHAR(), // name
            VARCHAR(), // emailAddress
            VARCHAR(), // creditCard
            VARCHAR(), // city
            VARCHAR(), // state
            TIMESTAMP(), // dateTime
            VARCHAR() // extra
        });
  }

  static RowVectorPtr createVector(int rows, memory::MemoryPool* pool) {
    auto idVector = BaseVector::create(BIGINT(), rows, pool);
    auto nameVector = BaseVector::create(VARCHAR(), rows, pool);
    auto emailAddressVector = BaseVector::create(VARCHAR(), rows, pool);
    auto creditCardVector = BaseVector::create(VARCHAR(), rows, pool);
    auto cityVector = BaseVector::create(VARCHAR(), rows, pool);
    auto stateVector = BaseVector::create(VARCHAR(), rows, pool);
    auto dateTimeVector = BaseVector::create(TIMESTAMP(), rows, pool);
    auto extraVector = BaseVector::create(VARCHAR(), rows, pool);

    return std::make_shared<RowVector>(
        pool,
        createType(),
        nullptr,
        rows,
        std::vector<VectorPtr>{
            idVector,
            nameVector,
            emailAddressVector,
            creditCardVector,
            cityVector,
            stateVector,
            dateTimeVector,
            extraVector});
  }
};

/** An auction submitted by a person. */
struct Auction {
  /** Id of auction. */
  int64_t id; // primary key

  /** Extra auction properties. */
  std::string itemName;

  std::string description;

  /** Initial bid price, in cents. */
  int64_t initialBid;

  /** Reserve price, in cents. */
  int64_t reserve;

  int64_t dateTime;

  /** When does auction expire? (ms since epoch). Bids at or after this time are
   * ignored. */
  int64_t expires;

  /** Id of person who instigated auction. */
  int64_t seller; // foreign key: Person.id

  /** Id of category auction is listed under. */
  int64_t category; // foreign key: Category.id

  /** Additional arbitrary payload for performance testing. */
  std::string_view extra;

  /** Constructor with all fields */
  Auction(
      int64_t id,
      std::string itemName,
      std::string description,
      int64_t initialBid,
      int64_t reserve,
      int64_t dateTime,
      int64_t expires,
      int64_t seller,
      int64_t category,
      std::string_view extra)
      : id(id),
        itemName(std::move(itemName)),
        description(std::move(description)),
        initialBid(initialBid),
        reserve(reserve),
        dateTime(dateTime),
        expires(expires),
        seller(seller),
        category(category),
        extra(std::move(extra)) {}

  std::string toString() const {
    return "Auction{id=" + std::to_string(id) + ", itemName='" + itemName +
        '\'' + ", description='" + description + '\'' +
        ", initialBid=" + std::to_string(initialBid) +
        ", reserve=" + std::to_string(reserve) +
        ", dateTime=" + formatDateTime(dateTime) +
        ", expires=" + formatDateTime(expires) +
        ", seller=" + std::to_string(seller) +
        ", category=" + std::to_string(category) + ", extra='" +
        std::string(extra) + '\'' + '}';
  }

  // Auction RowType
  static TypePtr createType() {
    return ROW(
        {
            "id", // Auction ID
            "itemName", // Item name
            "description", // Description
            "initialBid", // Initial bid
            "reserve", // Reserve price
            "dateTime", // Timestamp
            "expires", // Expiration time
            "seller", // Seller ID
            "category", // Category
            "extra" // Additional information
        },
        {
            BIGINT(), // id
            VARCHAR(), // itemName
            VARCHAR(), // description
            BIGINT(), // initialBid
            BIGINT(), // reserve
            TIMESTAMP(), // timestamp
            TIMESTAMP(), // expires
            BIGINT(), // seller
            BIGINT(), // category
            VARCHAR() // extra
        });
  }

  static RowVectorPtr createVector(int rows, memory::MemoryPool* pool) {
    auto idVector = BaseVector::create(BIGINT(), rows, pool);
    auto itemNameVector = BaseVector::create(VARCHAR(), rows, pool);
    auto descriptionVector = BaseVector::create(VARCHAR(), rows, pool);
    auto initialBidVector = BaseVector::create(BIGINT(), rows, pool);
    auto reserveVector = BaseVector::create(BIGINT(), rows, pool);
    auto dateTimeVector = BaseVector::create(TIMESTAMP(), rows, pool);
    auto expiresVector = BaseVector::create(TIMESTAMP(), rows, pool);
    auto sellerVector = BaseVector::create(BIGINT(), rows, pool);
    auto categoryVector = BaseVector::create(BIGINT(), rows, pool);
    auto extraVector = BaseVector::create(VARCHAR(), rows, pool);

    return std::make_shared<RowVector>(
        pool,
        createType(),
        nullptr,
        rows,
        std::vector<VectorPtr>{
            idVector,
            itemNameVector,
            descriptionVector,
            initialBidVector,
            reserveVector,
            dateTimeVector,
            expiresVector,
            sellerVector,
            categoryVector,
            extraVector});
  }
};

struct Bid {
  /** Id of auction this bid is for. */
  int64_t auction; // foreign key: Auction.id

  /** Id of person bidding in auction. */
  int64_t bidder; // foreign key: Person.id

  /** Price of bid, in cents. */
  int64_t price;

  /** The channel introduced this bidding. */
  std::string channel;

  /** The url of this bid. */
  std::string url;

  /**
   * Instant at which bid was made (ms since epoch). NOTE: This may be earlier
   * than the system's event time.
   */
  int64_t dateTime;

  /** Additional arbitrary payload for performance testing. */
  std::string_view extra;

  Bid(int64_t auction,
      int64_t bidder,
      int64_t price,
      std::string channel,
      std::string url,
      int64_t dateTime,
      std::string_view extra)
      : auction(auction),
        bidder(bidder),
        price(price),
        channel(std::move(channel)),
        url(std::move(url)),
        dateTime(dateTime),
        extra(std::move(extra)) {}

  std::string toString() const {
    return "Bid{auction=" + std::to_string(auction) +
        ", bidder=" + std::to_string(bidder) +
        ", price=" + std::to_string(price) + ", channel=" + channel +
        ", url=" + url + ", dateTime=" + formatDateTime(dateTime) +
        ", extra='" + std::string(extra) + '\'' + '}';
  }

  // Bid RowType
  static TypePtr createType() {
    return ROW(
        {
            "auction", // Auction ID
            "bidder", // Bidder ID
            "price", // Price
            "channel", // Channel
            "url", // Url
            "dateTime", // Timestamp
            "extra" // Additional information
        },
        {
            BIGINT(), // auction
            BIGINT(), // bidder
            BIGINT(), // price
            VARCHAR(), // channel
            VARCHAR(), // url
            TIMESTAMP(), // timestamp
            VARCHAR() // extra
        });
  }

  static RowVectorPtr createVector(int rows, memory::MemoryPool* pool) {
    auto auctionVector = BaseVector::create(BIGINT(), rows, pool);
    auto bidderVector = BaseVector::create(BIGINT(), rows, pool);
    auto priceVector = BaseVector::create(BIGINT(), rows, pool);
    auto channelVector = BaseVector::create(VARCHAR(), rows, pool);
    auto urlVector = BaseVector::create(VARCHAR(), rows, pool);
    auto dateTimeVector = BaseVector::create(TIMESTAMP(), rows, pool);
    auto extraVector = BaseVector::create(VARCHAR(), rows, pool);

    return std::make_shared<RowVector>(
        pool,
        createType(),
        nullptr,
        rows,
        std::vector<VectorPtr>{
            auctionVector,
            bidderVector,
            priceVector,
            channelVector,
            urlVector,
            dateTimeVector,
            extraVector});
  }
};

/// Represents an event containing either a Person, an Auction, or a Bid.
class Event {
 public:
  enum class Type { PERSON = 0, AUCTION = 1, BID = 2 };

  Event() = default;

  Event(Person person)
      : person_(std::make_unique<Person>(std::move(person))),
        auction_(nullptr),
        bid_(nullptr),
        eventType_(Type::PERSON) {}

  Event(Auction auction)
      : person_(nullptr),
        auction_(std::make_unique<Auction>(std::move(auction))),
        bid_(nullptr),
        eventType_(Type::AUCTION) {}

  Event(Bid bid)
      : person_(nullptr),
        auction_(nullptr),
        bid_(std::make_unique<Bid>(std::move(bid))),
        eventType_(Type::BID) {}

  Type getType() const {
    return eventType_;
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
    switch (eventType_) {
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
    auto eventTypeVector = BaseVector::create(INTEGER(), rows, pool);
    auto personVector = Person::createVector(rows, pool);
    auto auctionVector = Auction::createVector(rows, pool);
    auto bidVector = Bid::createVector(rows, pool);

    return std::make_shared<RowVector>(
        pool,
        createType(),
        nullptr,
        rows,
        std::vector<VectorPtr>{
            eventTypeVector, personVector, auctionVector, bidVector});
  }

 private:
  std::unique_ptr<Person> person_;
  std::unique_ptr<Auction> auction_;
  std::unique_ptr<Bid> bid_;
  Type eventType_;
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

} // namespace facebook::velox::connector::nexmark
