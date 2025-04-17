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

#include <cmath>
#include <limits>

namespace facebook::velox::connector::nexmark {

// Initialize static member
static std::mt19937 staticRandomGenerator;
const std::string StringsGenerator::REUSABLE_EXTRA_STRING =
    StringsGenerator::nextExactString(staticRandomGenerator, 1024 * 1024);

std::string StringsGenerator::nextString(std::mt19937& random, int maxLength) {
  return nextString(random, maxLength, ' ');
}

std::string StringsGenerator::nextString(
    std::mt19937& random,
    int maxLength,
    char special) {
  std::uniform_int_distribution<int> lenDist(MIN_STRING_LENGTH, maxLength - 1);
  int len = lenDist(random);
  std::uniform_int_distribution<int> specialDist(0, 12);
  std::uniform_int_distribution<int> charDist(0, 25);

  std::string result;
  result.reserve(len);

  while (len-- > 0) {
    if (specialDist(random) == 0) {
      result += special;
    } else {
      result += ('a' + charDist(random));
    }
  }

  // Trim trailing spaces (equivalent to Java's trim())
  while (!result.empty() && result.back() == ' ') {
    result.pop_back();
  }

  return result;
}

std::string StringsGenerator::nextExactString(
    std::mt19937& random,
    int length) {
  if (!REUSABLE_EXTRA_STRING.empty() &&
      length < REUSABLE_EXTRA_STRING.length() / 2) {
    std::uniform_int_distribution<int> offsetDist(
        0, REUSABLE_EXTRA_STRING.length() - length - 1);
    int offset = offsetDist(random);
    return REUSABLE_EXTRA_STRING.substr(offset, length);
  }

  std::string result;
  result.reserve(length);

  std::uniform_int_distribution<int> rndDist(
      0, std::numeric_limits<int>::max());
  int rnd = 0;
  int n = 0; // number of random characters left in rnd

  while (length-- > 0) {
    if (n == 0) {
      rnd = rndDist(random);
      n = 6; // log_26(2^31)
    }
    result += ('a' + rnd % 26);
    rnd /= 26;
    n--;
  }

  return result;
}

std::string StringsGenerator::nextExtra(
    std::mt19937& random,
    int currentSize,
    int desiredAverageSize) {
  if (currentSize > desiredAverageSize) {
    return "";
  }

  desiredAverageSize -= currentSize;
  int delta = static_cast<int>(std::round(desiredAverageSize * 0.2));
  int minSize = desiredAverageSize - delta;
  int desiredSize = minSize;

  if (delta != 0) {
    std::uniform_int_distribution<int> sizeDist(0, 2 * delta - 1);
    desiredSize += sizeDist(random);
  }

  return nextExactString(random, desiredSize);
}

} // namespace facebook::velox::connector::nexmark
