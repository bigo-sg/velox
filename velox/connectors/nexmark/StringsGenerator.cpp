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

#include "velox/connectors/nexmark/StringsGenerator.h"
#include "velox/connectors/nexmark/NexmarkUtils.h"

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <cmath>

namespace facebook::velox::connector::nexmark {

// Initialize static member
static pcg32_fast staticRandomGenerator;
const std::string StringsGenerator::REUSABLE_EXTRA_STRING =
    StringsGenerator::getReusableExtraString(
        staticRandomGenerator,
        1024 * 1024);

std::string StringsGenerator::nextString(
    pcg32_fast& random,
    int maxLength) {
  return nextString(random, maxLength, ' ');
}

std::string StringsGenerator::nextString(
    pcg32_fast& random,
    int maxLength,
    char special) {
  int len = MIN_STRING_LENGTH + getNextInt(random, maxLength - MIN_STRING_LENGTH);
  std::string result(len, 0);
  for (int i = 0; i < len; ++i) {
    if (getNextInt(random, 13) == 0) {
      result[i] = special;
    } else {
      result[i] = static_cast<int>('a') + getNextInt(random, 26);
    }
  }

  // Trim trailing spaces (equivalent to Java's trim())
  if (special == ' ')
    boost::algorithm::trim(result);

  return result;
}

std::string StringsGenerator::getReusableExtraString(pcg32_fast& random, int length) {
  std::string result(length, 0);
  int rnd = 0;
  int n = 0; // number of random characters left in rnd
  for (int i = 0; i < length; ++i) {
    if (n == 0) {
      rnd = static_cast<int>(random());
      n = 6; // log_26(2^31)
    }

    result[i] = static_cast<int>('a') + rnd % 26;
    rnd /= 26;
    n--;
  }
  return result;
}

std::string_view StringsGenerator::nextExactString(
    pcg32_fast& random,
    int length) {
  if (length >= REUSABLE_EXTRA_STRING.length() / 2) {
    throw std::runtime_error(fmt::format(
        "Requested extra string length {} exceeds {}",
        length,
        REUSABLE_EXTRA_STRING.length() / 2));
  }

  int offset = getNextInt(random, REUSABLE_EXTRA_STRING.length() - length);
  return std::string_view(REUSABLE_EXTRA_STRING.data() + offset, length);
}

std::string_view StringsGenerator::nextExtra(
    pcg32_fast& random,
    int currentSize,
    int desiredAverageSize) {
  if (currentSize > desiredAverageSize) {
    /// empty string_view
    return std::string_view(REUSABLE_EXTRA_STRING.data(), 0);
  }

  desiredAverageSize -= currentSize;
  int delta = static_cast<int>(std::round(desiredAverageSize * 0.2));
  int minSize = desiredAverageSize - delta;
  int desiredSize = minSize;

  if (delta != 0) {
    desiredSize += getNextInt(random, 2 * delta);
  }

  return nextExactString(random, desiredSize);
}

} // namespace facebook::velox::connector::nexmark
