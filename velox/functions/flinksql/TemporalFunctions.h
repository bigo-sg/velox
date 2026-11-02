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
#include <vector>

#include "folly/CPortability.h"

#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions::flinksql {

// Flink CURRENT_DATE: the current SQL date under the session time zone,
// which arrives through QueryConfig::sessionTimezone() (gluten-flink
// populates it from Flink's table.local-time-zone). Streaming-mode Flink
// evaluates CURRENT_DATE per record; this implementation reads the clock
// per row, matching.
template <typename TExec>
struct CurrentDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Date>& result) {
    auto now = Timestamp::now();
    if (timeZone_ != nullptr) {
      now.toTimezone(*timeZone_);
    }
    const std::chrono::
        time_point<std::chrono::system_clock, std::chrono::milliseconds>
            localTimepoint(std::chrono::milliseconds(now.toMillis()));
    result = std::chrono::floor<date::days>((localTimepoint).time_since_epoch())
                 .count();
  }

 private:
  const tz::TimeZone* timeZone_ = nullptr;
};

// Flink CURRENT_TIME: the wall-clock time of day under the session time
// zone, as the millisecond-of-day INT — the same encoding Flink RowData
// uses for TIME, so the gluten bridge maps Flink TIME onto velox INTEGER
// and the value crosses unchanged. Reads the clock per row, matching
// streaming-mode Flink.
template <typename TExec>
struct CurrentTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& result) {
    auto now = Timestamp::now();
    if (timeZone_ != nullptr) {
      now.toTimezone(*timeZone_);
    }
    result = static_cast<int32_t>(now.toMillis() % 86'400'000);
  }

 private:
  const tz::TimeZone* timeZone_ = nullptr;
};

} // namespace facebook::velox::functions::flinksql
