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
