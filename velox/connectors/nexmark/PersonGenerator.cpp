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

#include "velox/connectors/nexmark/PersonGenerator.h"
#include <algorithm>
#include <sstream>

namespace facebook::velox::connector::nexmark {

const std::vector<std::string> PersonGenerator::US_STATES = {
    "AZ", "CA", "ID", "OR", "WA", "WY"
};

const std::vector<std::string> PersonGenerator::US_CITIES = {
    "Phoenix", "Los Angeles", "San Francisco", "Boise",
    "Portland", "Bend", "Redmond", "Seattle", "Kent", "Cheyenne"
};

const std::vector<std::string> PersonGenerator::FIRST_NAMES = {
    "Peter", "Paul", "Luke", "John", "Saul",
    "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter"
};

const std::vector<std::string> PersonGenerator::LAST_NAMES = {
    "Shultz", "Abrams", "Spencer", "White",
    "Bartels", "Walton", "Smith", "Jones", "Noris"
};

const std::vector<std::string> PersonGenerator::CREDIT_CARD_STRINGS =
    PersonGenerator::createCreditCardStrings();

Person PersonGenerator::nextPerson(
    int64_t nextEventId,
    std::mt19937& random,
    int64_t timestamp,
    const GeneratorConfig& config) {

  int64_t id = lastBase0PersonId(config, nextEventId) + GeneratorConfig::FIRST_PERSON_ID;
  std::string name = nextPersonName(random);
  std::string email = nextEmail(random);
  std::string creditCard = nextCreditCard(random);
  std::string city = nextUSCity(random);
  std::string state = nextUSState(random);
  int currentSize = 8 + name.length() + email.length() +
                    creditCard.length() + city.length() + state.length();
  std::string extra = nextExtra(random, currentSize, config.getAvgPersonByteSize());

  return Person(id, name, email, creditCard, city, state, timestamp, extra);
}

int64_t PersonGenerator::nextBase0PersonId(
    int64_t eventId,
    std::mt19937& random,
    const GeneratorConfig& config) {
  // Choose a random person from any of the 'active' people, plus a few 'leads'.
  // By limiting to 'active' we ensure the density of bids or auctions per person
  // does not decrease over time for int64_t running jobs.
  // By choosing a person id ahead of the last valid person id we will make
  // newPerson and newAuction events appear to have been swapped in time.
  int64_t numPeople = lastBase0PersonId(config, eventId) + 1;
  int64_t activePeople = std::min<int64_t>(numPeople, config.getNumActivePeople());
  int64_t n = nextLong(random, activePeople + PERSON_ID_LEAD);
  return numPeople - activePeople + n;
}

int64_t PersonGenerator::lastBase0PersonId(const GeneratorConfig& config, int64_t eventId) {
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

std::string PersonGenerator::nextUSState(std::mt19937& random) {
  return US_STATES[random() % US_STATES.size()];
}

std::string PersonGenerator::nextUSCity(std::mt19937& random) {
  return US_CITIES[random() % US_CITIES.size()];
}

std::string PersonGenerator::nextPersonName(std::mt19937& random) {
  return FIRST_NAMES[random() % FIRST_NAMES.size()] + " " +
         LAST_NAMES[random() % LAST_NAMES.size()];
}

std::string PersonGenerator::nextEmail(std::mt19937& random) {
  return nextString(random, 7) + "@" + nextString(random, 5) + ".com";
}

std::vector<std::string> PersonGenerator::createCreditCardStrings() {
  std::vector<std::string> creditCardStrings(10000);
  for (int i = 0; i < 10000; ++i) {
    std::stringstream ss;
    ss << std::setw(4) << std::setfill('0') << i;
    creditCardStrings[i] = ss.str();
  }
  return creditCardStrings;
}

std::string PersonGenerator::nextCreditCard(std::mt19937& random) {
  std::stringstream sb;
  for (int i = 0; i < 4; i++) {
    if (i > 0) {
      sb << " ";
    }
    sb << CREDIT_CARD_STRINGS[random() % CREDIT_CARD_STRINGS.size()];
  }
  return sb.str();
}

std::string PersonGenerator::nextString(std::mt19937& random, int length) {
  static const char alphanum[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::string result;
  result.reserve(length);

  for (int i = 0; i < length; ++i) {
    result += alphanum[random() % (sizeof(alphanum) - 1)];
  }

  return result;
}

std::string PersonGenerator::nextExtra(std::mt19937& random, int currentSize, int targetSize) {
  int extraSize = std::max(0, targetSize - currentSize);
  return nextString(random, extraSize);
}

int64_t PersonGenerator::nextLong(std::mt19937& random, int64_t n) {
  std::uniform_int_distribution<int64_t> dist(0, n - 1);
  return dist(random);
}

} // namespace facebook::velox::connector::nexmark
