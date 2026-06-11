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

#include "velox/connectors/nexmark/pcg_random.hpp"

#include <string>

namespace facebook::velox::connector::nexmark {
inline std::string formatDateTime(int64_t dateTime) {
  // Convert milliseconds to seconds for std::chrono
  std::time_t seconds = dateTime / 1000;
  int milliseconds = dateTime % 1000;

  // Convert to UTC time
  std::tm tm;
  gmtime_r(&seconds, &tm);

  // Format the time into a string
  char buffer[30];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &tm);

  // Append milliseconds (zero-padded to 3 digits) and 'Z' for UTC
  char ms_buf[4];
  snprintf(ms_buf, sizeof(ms_buf), "%03d", milliseconds);
  return std::string(buffer) + "." + ms_buf + "Z";
}

/// return a random integer in [0, bound)
/// This is a replacement for the Java's Random.nextInt(int bound) method.
/// It is used instead of std::uniform_int_distribution<int> for performance gain.
inline int getNextInt(pcg32_fast& random, int bound) {
  int m = bound - 1;
  int r = static_cast<int>(random());
  if ((bound & m) == 0) {
    r &= m;
  } else {
    for (int u = static_cast<int>(static_cast<unsigned int>(r) >> 1);
         u + m - (r = u % bound) < 0;
         u = static_cast<int>(random() >> 1)) {
    }
  }
  return r;
}

} // namespace facebook::velox::connector::nexmark
