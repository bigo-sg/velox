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

#include <string>

namespace facebook::velox::connector::nexmark {

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
  std::string extra;
};

} // namespace facebook::velox::connector::nexmark
