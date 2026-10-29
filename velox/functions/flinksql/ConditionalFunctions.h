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

#include <type_traits>

#include "folly/CPortability.h"

#include "velox/functions/Macros.h"
#include "velox/functions/UDFOutputString.h"

namespace facebook::velox::functions::flinksql {

// Flink IFNULL(a, b): returns a when a is not null, else b; null when both
// are null. Flink's planner coerces both operands to their common type
// before the call, so same-type signatures cover the function. Custom null
// handling (callNullable) is required: default null behavior would turn a
// null first argument into a null result regardless of b.
template <typename TExec>
struct IfNullFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // Same bare C++ type for input and output (numerics, Date, Timestamp,
  // decimals through their unscaled native types).
  template <typename T>
  FOLLY_ALWAYS_INLINE bool callNullable(T& result, const T* a, const T* b) {
    const T* picked = (a != nullptr) ? a : b;
    if (picked == nullptr) {
      return false;
    }
    result = *picked;
    return true;
  }

  // Varchar: the output is a string writer, the input a string view.
  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<Varchar>& result,
      const arg_type<Varchar>* a,
      const arg_type<Varchar>* b) {
    const arg_type<Varchar>* picked = (a != nullptr) ? a : b;
    if (picked == nullptr) {
      return false;
    }
    UDFOutputString::assign(
        result, std::string_view(picked->data(), picked->size()));
    return true;
  }
};

} // namespace facebook::velox::functions::flinksql
