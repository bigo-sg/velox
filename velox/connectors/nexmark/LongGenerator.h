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

#include "velox/connectors/nexmark/NexmarkUtils.h"
#include "velox/connectors/nexmark/pcg_random.hpp"

#include <cstdint>
#include <cstdlib>

namespace facebook::velox::connector::nexmark {

class LongGenerator {
 public:
  /** Return a random long from [0, n). */
  static int64_t nextLong(pcg32_fast& random, int64_t n) {
    if (n < static_cast<int64_t>(std::numeric_limits<int>::max())) {
      return getNextInt(random, n);
    } else {
      // WARNING: Very skewed distribution! Bad!
      int64_t r = (random() << 31) | random();
      return std::abs(static_cast<int64_t>(r) % n);
    }
  }
};

} // namespace facebook::velox::connector::nexmark
