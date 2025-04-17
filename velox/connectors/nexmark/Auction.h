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

#include <chrono>
#include <string>

namespace facebook::velox::connector::nexmark {

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
  std::string extra;
};

} // namespace facebook::velox::connector::nexmark
