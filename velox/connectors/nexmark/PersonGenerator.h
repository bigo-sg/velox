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
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <string>
#include <vector>
#include <random>

#include "NexmarkUtils.h"

namespace facebook::velox::connector::nexmark {

class GeneratorConfig;

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
  std::string extra;

  Person(int64_t id,
         std::string name,
         std::string emailAddress,
         std::string creditCard,
         std::string city,
         std::string state,
         int64_t dateTime,
         std::string extra)
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
        '\'' + ", dateTime=" + formatDateTime(dateTime) + ", extra='" + extra +
        '\'' + '}';
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
            VARCHAR(), // email
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
    auto emailVector = BaseVector::create(VARCHAR(), rows, pool);
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
            emailVector,
            creditCardVector,
            cityVector,
            stateVector,
            dateTimeVector,
            extraVector});
  }

  FOLLY_NOINLINE static void
  fillVector(RowVector* personVector, int index, const Person* person) {
    if (!person) {
      personVector->setNull(index, true);
      return;
    }

    auto idVector = personVector->childAt(0)->asFlatVector<int64_t>();
    auto nameVector = personVector->childAt(1)->asFlatVector<StringView>();
    auto emailVector = personVector->childAt(2)->asFlatVector<StringView>();
    auto creditCardVector =
        personVector->childAt(3)->asFlatVector<StringView>();
    auto cityVector = personVector->childAt(4)->asFlatVector<StringView>();
    auto stateVector = personVector->childAt(5)->asFlatVector<StringView>();
    auto dateTimeVector = personVector->childAt(6)->asFlatVector<Timestamp>();
    auto extraVector = personVector->childAt(7)->asFlatVector<StringView>();

    idVector->set(index, person->id);
    nameVector->set(index, StringView(person->name));
    emailVector->set(index, StringView(person->emailAddress));
    creditCardVector->set(index, StringView(person->creditCard));
    cityVector->set(index, StringView(person->city));
    stateVector->set(index, StringView(person->state));
    dateTimeVector->set(
        index,
        Timestamp(person->dateTime / 1000, (person->dateTime) % 1000 * 1000));
    extraVector->set(index, StringView(person->extra));
  }
};

/** Generates people. */
class PersonGenerator {
public:
  /** Number of yet-to-be-created people and auction ids allowed. */
  static constexpr int PERSON_ID_LEAD = 10;

  /** Generate and return a random person with next available id. */
  static Person nextPerson(
      int64_t nextEventId,
      std::mt19937& random,
      int64_t timestamp,
      const GeneratorConfig & config);

  /** Return a random person id (base 0). */
  static int64_t nextBase0PersonId(
      int64_t eventId,
      std::mt19937& random,
      const GeneratorConfig& config);

  /**
   * Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
   * due to generate a person.
   */
  static int64_t lastBase0PersonId(
      const GeneratorConfig& config,
      int64_t eventId);

private:
  /** Return a random US state. */
  static std::string nextUSState(std::mt19937& random);

  /** Return a random US city. */
  static std::string nextUSCity(std::mt19937& random);

  /** Return a random person name. */
  static std::string nextPersonName(std::mt19937& random);

  /** Return a random email address. */
  static std::string nextEmail(std::mt19937& random);

  /** Return a random credit card number. */
  static std::string nextCreditCard(std::mt19937& random);

  /** Create an array of credit card strings. */
  static std::vector<std::string> createCreditCardStrings();

  /** Return a random string of specified length. */
  static std::string nextString(std::mt19937& random, int length);

  /** Generate a random "extra" string to pad the person to the desired size. */
  static std::string nextExtra(
      std::mt19937& random,
      int currentSize,
      int targetSize);

  /** Generate a random int64_t within a range. */
  static int64_t nextLong(std::mt19937& random, int64_t n);

  static const std::vector<std::string> US_STATES;
  static const std::vector<std::string> US_CITIES;
  static const std::vector<std::string> FIRST_NAMES;
  static const std::vector<std::string> LAST_NAMES;
  static const std::vector<std::string> CREDIT_CARD_STRINGS;
};

} // namespace facebook::velox::connector::nexmark
