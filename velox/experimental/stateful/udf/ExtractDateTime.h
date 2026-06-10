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
#include <cstdint>

#include "velox/functions/Macros.h"

#include <algorithm>
#include <ctime>

namespace facebook::velox::stateful::udf {

template <typename T>
struct ExtractFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& field,
      const arg_type<Timestamp>& timestamp) {
    std::string fieldLower(field.data(), field.size());
    std::transform(
        fieldLower.begin(), fieldLower.end(), fieldLower.begin(), ::tolower);

    // TODO: maybe should support timezone
    time_t time = timestamp.getSeconds();
    struct tm timeInfo;
    gmtime_r(&time, &timeInfo);

    // here to fix compile warning
    if (fieldLower == "year") {
      result = timeInfo.tm_year + 1900;
    } else if (fieldLower == "month") {
      result = timeInfo.tm_mon + 1;
    } else if (fieldLower == "day" || fieldLower == "day_of_month") {
      result = timeInfo.tm_mday;
    } else if (fieldLower == "hour") {
      result = timeInfo.tm_hour;
    } else if (fieldLower == "minute") {
      result = timeInfo.tm_min;
    } else if (fieldLower == "second") {
      result = timeInfo.tm_sec;
    } else if (fieldLower == "day_of_week" || fieldLower == "dow") {
      result = timeInfo.tm_wday == 0 ? 7 : timeInfo.tm_wday;
    } else if (fieldLower == "day_of_year" || fieldLower == "doy") {
      result = timeInfo.tm_yday + 1;
    } else {
      return false;
    }
    return true;
  }
};

} // namespace facebook::velox::stateful::udf
