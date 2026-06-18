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

#include <cstdint>
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

  // Append milliseconds and 'Z' for UTC
  return std::string(buffer) + "." + std::to_string(milliseconds).substr(0, 3) +
      "Z";
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

/// Compute a deterministic RNG seed from the deterministic fields of a
/// NexmarkGeneratorConfig. The seed is intentionally derived only from
/// fields that are fixed at planning time (firstEventId, maxEvents,
/// firstEventNumber) so that:
///   - Re-running the same query with the same config yields identical
///     data, making results reproducible across runs.
///   - Distinct subtasks of a multi-parallel source have different
///     firstEventId / maxEvents and therefore different seeds, so the
///     per-subtask random fields (names, prices, ...) do not collide.
/// Fields that vary per invocation (baseTime, wallclockBaseTime) are
/// deliberately excluded. Uses FNV-1a 64-bit for a stable, cross-platform
/// hash (std::hash is not portable).
inline uint64_t computeNexmarkSeed(
    int64_t firstEventId,
    int64_t maxEvents,
    int64_t firstEventNumber) {
  constexpr uint64_t kFnvOffsetBasis = 1469598103934665603ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  uint64_t h = kFnvOffsetBasis;
  auto mix = [&](uint64_t v) {
    h ^= v;
    h *= kFnvPrime;
  };
  mix(static_cast<uint64_t>(firstEventId));
  mix(static_cast<uint64_t>(maxEvents));
  mix(static_cast<uint64_t>(firstEventNumber));
  return h;
}

} // namespace facebook::velox::connector::nexmark
