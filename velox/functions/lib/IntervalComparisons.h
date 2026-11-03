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

#include "velox/type/Type.h"

namespace facebook::velox::functions::lib {

// Interval-to-interval comparisons, common to all dialects: comparing two
// intervals of the same family compares their underlying storage (millis
// for day-time, months for year-month). Registered per dialect under
// dialect-owned names.
template <typename T>
struct IntervalDayTimeLessThanFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<IntervalDayTime>& b) {
    result = a < b;
    return true;
  }
};

template <typename T>
struct IntervalDayTimeLessThanOrEqualFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<IntervalDayTime>& b) {
    result = a <= b;
    return true;
  }
};

template <typename T>
struct IntervalDayTimeGreaterThanFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<IntervalDayTime>& b) {
    result = a > b;
    return true;
  }
};

template <typename T>
struct IntervalDayTimeGreaterThanOrEqualFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<IntervalDayTime>& b) {
    result = a >= b;
    return true;
  }
};

template <typename T>
struct IntervalDayTimeEqualToFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<IntervalDayTime>& b) {
    result = a == b;
    return true;
  }
};

template <typename T>
struct IntervalDayTimeNotEqualToFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<IntervalDayTime>& b) {
    result = a != b;
    return true;
  }
};

template <typename T>
struct IntervalYearMonthLessThanFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalYearMonth>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = a < b;
    return true;
  }
};

template <typename T>
struct IntervalYearMonthLessThanOrEqualFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalYearMonth>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = a <= b;
    return true;
  }
};

template <typename T>
struct IntervalYearMonthGreaterThanFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalYearMonth>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = a > b;
    return true;
  }
};

template <typename T>
struct IntervalYearMonthGreaterThanOrEqualFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalYearMonth>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = a >= b;
    return true;
  }
};

template <typename T>
struct IntervalYearMonthEqualToFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalYearMonth>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = a == b;
    return true;
  }
};

template <typename T>
struct IntervalYearMonthNotEqualToFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<IntervalYearMonth>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = a != b;
    return true;
  }
};

} // namespace facebook::velox::functions::lib
