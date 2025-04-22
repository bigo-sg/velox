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
#include "velox/connectors/nexmark/GeneratorConfig.h"
#include "velox/connectors/nexmark/LongGenerator.h"
#include "velox/connectors/nexmark/NexmarkUtils.h"
#include "velox/connectors/nexmark/StringsGenerator.h"
#include "velox/connectors/nexmark/pcg_random.hpp"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <string>
#include <vector>

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
  std::string_view extra;

  Person(int64_t id,
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

  static void
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
      pcg32_fast& random,
      int64_t timestamp,
      const GeneratorConfig & config);

  static RowVectorPtr nextPersonBatch(
      size_t rows,
      const FlatVector<int32_t>& eventTypeVector,
      const FlatVector<int64_t>& eventIdVector,
      pcg32_fast& random,
      const FlatVector<int64_t>& timestampVector,
      const GeneratorConfig& config, memory::MemoryPool* pool);

  /**
   * Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
   * due to generate a person.
   */
  FOLLY_ALWAYS_INLINE static int64_t nextBase0PersonId(
      int64_t eventId,
      pcg32_fast& random,
      const GeneratorConfig& config) {
    // Choose a random person from any of the 'active' people, plus a few
    // 'leads'. By limiting to 'active' we ensure the density of bids or
    // auctions per person does not decrease over time for int64_t running jobs.
    // By choosing a person id ahead of the last valid person id we will make
    // newPerson and newAuction events appear to have been swapped in time.
    int64_t numPeople = lastBase0PersonId(config, eventId) + 1;
    int64_t activePeople =
        std::min<int64_t>(numPeople, config.getNumActivePeople());
    int64_t n = LongGenerator::nextLong(random, activePeople + PERSON_ID_LEAD);
    return numPeople - activePeople + n;
  }

    FOLLY_ALWAYS_INLINE static int64_t lastBase0PersonId(const GeneratorConfig& config, int64_t eventId) {
      int64_t epoch = eventId / config.totalProportion;
      int64_t offset = eventId % config.totalProportion;
      if (offset >= config.personProportion) {
        // About to generate an auction or bid.
        // Go back to the last person generated in this epoch.
        offset = config.personProportion - 1;
      }
      // About to generate a person.
      return epoch * config.personProportion + offset;
    }

private:
 FOLLY_ALWAYS_INLINE static std::string nextUSState(pcg32_fast& random) {
   return US_STATES[random() % US_STATES.size()];
 }

 FOLLY_ALWAYS_INLINE static std::string nextUSCity(pcg32_fast& random) {
   return US_CITIES[random() % US_CITIES.size()];
 }

 FOLLY_ALWAYS_INLINE static std::string nextPersonName(pcg32_fast& random) {
   return FIRST_NAMES[random() % FIRST_NAMES.size()] + " " +
       LAST_NAMES[random() % LAST_NAMES.size()];
 }

 FOLLY_ALWAYS_INLINE static std::string nextEmail(pcg32_fast& random) {
   return StringsGenerator::nextString(random, 7) + "@" +
       StringsGenerator::nextString(random, 5) + ".com";
 }

 FOLLY_ALWAYS_INLINE static std::string nextCreditCard(pcg32_fast& random) {
   std::string result;
   result.reserve(19); // 16 digits + 3 spaces
   result += CREDIT_CARD_STRINGS[random() % CREDIT_CARD_STRINGS.size()];
   result += " ";
   result += CREDIT_CARD_STRINGS[random() % CREDIT_CARD_STRINGS.size()];
   result += " ";
   result += CREDIT_CARD_STRINGS[random() % CREDIT_CARD_STRINGS.size()];
   result += " ";
   result += CREDIT_CARD_STRINGS[random() % CREDIT_CARD_STRINGS.size()];
   return result;
 }

 /** Create an array of credit card strings. */
 static std::vector<std::string> createCreditCardStrings();

 static const std::vector<std::string> US_STATES;
 static const std::vector<std::string> US_CITIES;
 static const std::vector<std::string> FIRST_NAMES;
 static const std::vector<std::string> LAST_NAMES;
 static const std::vector<std::string> CREDIT_CARD_STRINGS;
};

} // namespace facebook::velox::connector::nexmark
