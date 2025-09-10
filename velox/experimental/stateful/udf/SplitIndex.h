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
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::stateful::udf {

template <typename T>
struct SplitIndexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      const int64_t& index) {
    if (index <= 0) {
      return false;
    }
    return functions::stringImpl::splitPart<false>(
        result, input, delimiter, index);
  }

  FOLLY_ALWAYS_INLINE bool callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      const int64_t& index) {
    if (index <= 0) {
      return false;
    }
    return functions::stringImpl::splitPart<true>(
        result, input, delimiter, index);
  }
};

} // namespace facebook::velox::stateful::udf