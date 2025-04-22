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
#include <iomanip>
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
    pcg32_fast& random,
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
  std::string_view extra = StringsGenerator::nextExtra(
      random, currentSize, config.getAvgPersonByteSize());

  return Person(
      id,
      std::move(name),
      std::move(email),
      std::move(creditCard),
      std::move(city),
      std::move(state),
      timestamp,
      std::move(extra));
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



} // namespace facebook::velox::connector::nexmark
