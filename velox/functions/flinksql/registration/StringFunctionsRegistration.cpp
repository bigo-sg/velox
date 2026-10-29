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
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"

#include "velox/expression/VectorFunction.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/flinksql/StringFunctions.h"
#include "velox/functions/lib/Re2Functions.h"

namespace facebook::velox::functions::flinksql {

namespace {
// Flink semantics: returns NULL on no match, allows non-constant patterns.
std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return makeRe2Extract(name, inputArgs, config, /*emptyNoMatch=*/false);
}
} // namespace

void registerStringFunctions(const std::string& prefix) {
  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract", re2ExtractSignatures(), makeRegexExtract);

  // Flink-semantics string functions; registered with an empty prefix by
  // velox4j's FLINK preset they overwrite the sparksql baseline entries
  // registered under the same (name, signature) keys.
  // trim(str[, seek]) removes 0x20 spaces (1-arg) or any character of the
  // seek character set (2-arg); Flink argument order is (str, seek).
  registerFunction<TrimFunction, Varchar, Varchar>({prefix + "trim"});
  registerFunction<TrimFunction, Varchar, Varchar, Varchar>({prefix + "trim"});
  registerFunction<LTrimFunction, Varchar, Varchar>({prefix + "ltrim"});
  registerFunction<LTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "ltrim"});
  registerFunction<RTrimFunction, Varchar, Varchar>({prefix + "rtrim"});
  registerFunction<RTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "rtrim"});
  // Two-argument forms with a runtime-sourced seek: Flink's isSpaceString
  // checks only the first byte for segment-backed strings, degrading a seek
  // that starts with a space to space-only trimming. Plan-constant seeks take
  // the plain functions above (character-set walk).
  registerFunction<TrimRuntimeFunction, Varchar, Varchar, Varchar>(
      {prefix + "trim_runtime"});
  registerFunction<LTrimRuntimeFunction, Varchar, Varchar, Varchar>(
      {prefix + "ltrim_runtime"});
  registerFunction<RTrimRuntimeFunction, Varchar, Varchar, Varchar>(
      {prefix + "rtrim_runtime"});
  // lpad/rpad(str, len, pad): len is Flink INT (int32); a negative len or an
  // empty pad yields NULL via the bool-returning call.
  registerFunction<LPadFunction, Varchar, Varchar, int32_t, Varchar>(
      {prefix + "lpad"});
  registerFunction<RPadFunction, Varchar, Varchar, int32_t, Varchar>(
      {prefix + "rpad"});
}

} // namespace facebook::velox::functions::flinksql
