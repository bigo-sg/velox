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
#include <folly/CPortability.h>

#include "velox/connectors/nexmark/pcg_random.hpp"

namespace facebook::velox::connector::nexmark {

/// Generates strings which are used for different field in other model objects.
class StringsGenerator {
 public:
  /// Smallest random string size.
  static constexpr int MIN_STRING_LENGTH = 3;

  /// Return a random string of up to `maxLength`.
  static std::string nextString(pcg32_fast& random, int maxLength);

  /// Return a random string of up to `maxLength` with special character.
  static std::string
  nextString(pcg32_fast& random, int maxLength, char special);

  /// Return a random string of exactly `length`.
  static std::string_view nextExactString(pcg32_fast& random, int length);

  /**
   * Return a random `string` such that `currentSize + string.length()` is on
   * average `averageSize`.
   */
  static std::string_view
  nextExtra(pcg32_fast& random, int currentSize, int desiredAverageSize);

 private:
  static std::string getReusableExtraString(pcg32_fast& random, int length);
  static const std::string REUSABLE_EXTRA_STRING;
};

} // namespace facebook::velox::connector::nexmark
