/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/common/encode/Base64.h"
#include "velox/functions/Macros.h"
#include "velox/functions/lib/string/StringCore.h"

namespace facebook::velox::functions::flinksql {

/// Flink REPLACE: empty `from` inserts `to` around each character
template <typename TExec>
struct ReplaceFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& replaced,
      const arg_type<Varchar>& replacement) {
    size_t reserveSize = input.size();
    if (replaced.size() == 0) {
      if (replacement.size() > 0) {
        reserveSize = (input.size() + 1) * replacement.size() + input.size();
      }
    } else if (replacement.size() > replaced.size()) {
      reserveSize = (input.size() / replaced.size()) * replacement.size() +
          input.size() % replaced.size();
    }
    result.reserve(reserveSize);
    auto resultSize = stringCore::replace<false>(
        result.data(),
        std::string_view(input.data(), input.size()),
        std::string_view(replaced.data(), replaced.size()),
        std::string_view(replacement.data(), replacement.size()));
    result.resize(resultSize);
  }
};

/// Flink RIGHT: rightmost n UTF-8 code points; n <= 0 returns empty, n >= len returns whole.
template <typename TExec>
struct RightFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<int32_t>& n) {
    if (n <= 0) {
      result.resize(0);
      return;
    }
    int32_t remaining = n;
    size_t offset = input.size();
    while (offset > 0 && remaining > 0) {
      --offset;
      if ((static_cast<unsigned char>(input.data()[offset]) & 0xC0) != 0x80) {
        --remaining;
      }
    }
    if (remaining > 0) {
      offset = 0;
    }
    result.copy_from(std::string_view(input.data() + offset, input.size() - offset));
  }
};

/// Flink FROM_BASE64: decodes base64 VARCHAR to VARCHAR (presto returns VARBINARY).
template <typename TExec>
struct FromBase64FlinkFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    auto inputSize = input.size();
    auto decodedSize =
        encoding::Base64::calculateDecodedSize(input.data(), inputSize);
    if (decodedSize.hasError()) {
      return decodedSize.error();
    }
    result.resize(decodedSize.value());
    return encoding::Base64::decode(
        input.data(), inputSize, result.data(), result.size());
  }
};

} // namespace facebook::velox::functions::flinksql
