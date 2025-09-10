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

#include <string>
#include <algorithm>

namespace facebook::velox::stateful::udf {

template <typename T>
struct CountCharFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool
  call(
      out_type<int64_t>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& str) {
    char character = str.data()[0];
    result = std::count(input.begin(), input.end(), character);
    return true;
  }
};

} // namespace facebook::velox::stateful::udf
