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
#include <string_view>

#include "folly/ssl/OpenSSLHash.h"
#include "velox/functions/Udf.h"

namespace facebook::velox::functions::flinksql {

// Flink hash expressions take a string (hashed as its UTF-8 bytes) and
// return a lowercase hex VARCHAR — Flink anchors on commons-codec
// DigestUtils.*Hex. The flinksql VARCHAR-input signatures coexist with the
// baselines' varbinary-input hash functions by input-type dispatch.
// Digest primitive here: folly::ssl::OpenSSLHash.
//
// Implemented: the SHA-2 family — sha2(s, hashLength) plus the fixed-length
// spellings sha224/sha256/sha384/sha512 that bind the hash length and route
// to the same core. sha2 renders hashLength/4 lowercase hex chars; hashLength
// 0 is rejected like any other unsupported length, mirroring Flink 1.19.2,
// which rejects 0 outright — Hive's 0-means-256 convention is deliberately
// not adopted so the offloaded and native execution paths stay consistent.
//
// Implementation pattern: one struct per function with
// VELOX_DEFINE_FUNCTION_TYPES, registered with explicit signatures in
// registration/HashFunctionsRegistration.cpp (see velox/functions/prestosql
// for the convention).

// Expands each digest byte to two lowercase hex chars, in place and back to
// front: the digest occupies the first digestSize bytes of output (whose full
// size is 2 * digestSize), and 2 * i >= i keeps the expansion from
// overwriting bytes that have not been read yet. This is Flink's
// EncodingUtils.hex rendering ("0123456789abcdef").
FOLLY_ALWAYS_INLINE void encodeDigestToHexString(
    uint8_t* output,
    int32_t digestSize) {
  static const char* const kHexDigits = "0123456789abcdef";
  for (int32_t i = digestSize - 1; i >= 0; --i) {
    const uint8_t digestByte = output[i];
    output[i * 2] = kHexDigits[(digestByte >> 4) & 0xf];
    output[i * 2 + 1] = kHexDigits[digestByte & 0xf];
  }
}

// SHA-2 core shared by the whole family: computes the digest selected by
// hashLength over the bytes of s and renders it as lowercase hex in result.
// hashLength must be 224, 256, 384, or 512; 0 and any other unsupported
// length fail the job, mirroring Flink 1.19.2's rejection of unsupported
// algorithms (0 included). TResult is the caller's out_type<Varchar> writer.
template <typename TResult>
FOLLY_ALWAYS_INLINE void
sha2Hex(TResult& result, std::string_view s, int64_t hashLength) {
  const EVP_MD* hashAlgorithm;
  switch (hashLength) {
    case 224:
      hashAlgorithm = EVP_sha224();
      break;
    case 256:
      hashAlgorithm = EVP_sha256();
      break;
    case 384:
      hashAlgorithm = EVP_sha384();
      break;
    case 512:
      hashAlgorithm = EVP_sha512();
      break;
    default:
      VELOX_USER_FAIL("Unsupported SHA-2 hash length: {}", hashLength);
  }
  const int32_t digestLength = static_cast<int32_t>(hashLength >> 3);
  result.resize(digestLength * 2);
  folly::ssl::OpenSSLHash::hash(
      folly::MutableByteRange((uint8_t*)result.data(), digestLength),
      hashAlgorithm,
      folly::ByteRange((const uint8_t*)s.data(), s.size()));
  encodeDigestToHexString((uint8_t*)result.data(), digestLength);
}

// sha2(s, hashLength) -> varchar
// SHA-2 family digest (224, 256, 384, or 512 bits) as a lowercase hex
// string of hashLength/4 chars. NULL in -> NULL out; hashLength 0, like any
// other unsupported length, fails the job (Flink 1.19.2 rejects 0 too).
template <typename TExec>
struct Sha2Function {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // The SQL operator accepts any integer width for hashLength
  // (NUMERIC_INTEGER family); the core widens each to int64_t.
  template <typename TBitLength>
  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& s,
      const TBitLength& hashLength) {
    sha2Hex(result, std::string_view(s), static_cast<int64_t>(hashLength));
  }
};

// Fixed-length spellings (SHA224/SHA256/SHA384/SHA512): the sha2 core with
// the hash length bound, as in Flink where the shorthand routes to the same
// machinery with the algorithm fixed.
template <typename TExec, int64_t kHashLength>
struct Sha2FixedLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& s) {
    sha2Hex(result, std::string_view(s), kHashLength);
  }
};

// sha224(s) -> varchar: 56 lowercase hex chars.
template <typename TExec>
struct Sha224Function : Sha2FixedLengthFunction<TExec, 224> {};

// sha256(s) -> varchar: 64 lowercase hex chars.
template <typename TExec>
struct Sha256Function : Sha2FixedLengthFunction<TExec, 256> {};

// sha384(s) -> varchar: 96 lowercase hex chars.
template <typename TExec>
struct Sha384Function : Sha2FixedLengthFunction<TExec, 384> {};

// sha512(s) -> varchar: 128 lowercase hex chars.
template <typename TExec>
struct Sha512Function : Sha2FixedLengthFunction<TExec, 512> {};

} // namespace facebook::velox::functions::flinksql
