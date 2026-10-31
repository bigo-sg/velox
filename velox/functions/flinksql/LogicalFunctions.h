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

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::flinksql {

// Flink IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE: three-valued
// predicates whose result is never NULL. callNullable takes over NULL
// handling: the default behavior would propagate NULLs, while Flink maps
// UNKNOWN to a definite TRUE/FALSE per the truth table below.
//
//   x        | IS TRUE | IS FALSE | IS NOT TRUE | IS NOT FALSE
//   TRUE     | TRUE    | FALSE    | FALSE       | TRUE
//   FALSE    | FALSE   | TRUE     | TRUE        | FALSE
//   UNKNOWN  | FALSE   | FALSE    | TRUE        | TRUE
template <typename T>
struct IsTrueFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<bool>& result,
      const arg_type<bool>* x) {
    result = (x != nullptr) && *x;
  }
};

template <typename T>
struct IsFalseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<bool>& result,
      const arg_type<bool>* x) {
    result = (x != nullptr) && !*x;
  }
};

template <typename T>
struct IsNotTrueFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<bool>& result,
      const arg_type<bool>* x) {
    result = (x == nullptr || !*x);
  }
};

template <typename T>
struct IsNotFalseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<bool>& result,
      const arg_type<bool>* x) {
    result = (x == nullptr || *x);
  }
};

} // namespace facebook::velox::functions::flinksql
