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

#include <openssl/evp.h>

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::flinksql {

/// Flink-flavored MD5: MD5(string) -> STRING, the MD5 digest of the string's
/// UTF-8 bytes as 32 lowercase hex digits. Mirrors Flink's
/// MessageDigest("MD5") + EncodingUtils.hex chain; implemented on OpenSSL
/// directly instead of reusing the prestosql/sparksql registrations (which
/// take VARBINARY).
template <typename TExec>
struct Md5Function {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digestLen = 0;
    EVP_Digest(input.data(), input.size(), digest, &digestLen, EVP_md5(), nullptr);
    constexpr char kHexDigits[] = "0123456789abcdef";
    result.resize(digestLen * 2);
    for (unsigned int i = 0; i < digestLen; ++i) {
      result.data()[2 * i] = kHexDigits[digest[i] >> 4];
      result.data()[2 * i + 1] = kHexDigits[digest[i] & 0xF];
    }
  }
};

/// Flink-flavored SHA1: SHA1(string) -> STRING, the SHA-1 digest of the
/// string's UTF-8 bytes as 40 lowercase hex digits. Mirrors Flink's
/// MessageDigest("SHA") + EncodingUtils.hex chain (Java "SHA" = SHA-1);
/// implemented on OpenSSL directly instead of reusing the
/// prestosql/sparksql registrations (which take VARBINARY).
template <typename TExec>
struct Sha1Function {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digestLen = 0;
    EVP_Digest(input.data(), input.size(), digest, &digestLen, EVP_sha1(), nullptr);
    constexpr char kHexDigits[] = "0123456789abcdef";
    result.resize(digestLen * 2);
    for (unsigned int i = 0; i < digestLen; ++i) {
      result.data()[2 * i] = kHexDigits[digest[i] >> 4];
      result.data()[2 * i + 1] = kHexDigits[digest[i] & 0xF];
    }
  }
};

} // namespace facebook::velox::functions::flinksql