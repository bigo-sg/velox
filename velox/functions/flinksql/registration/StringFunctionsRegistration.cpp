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
#include "velox/functions/flinksql/LikeFunctions.h"
#include "velox/functions/flinksql/StringFunctions.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"

#include "velox/expression/VectorFunction.h"
#include "velox/functions/Registerer.h"
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
  registerFunction<ReplaceFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "replace"});
  registerFunction<RightFunction, Varchar, Varchar, int32_t>(
      {prefix + "right"});
  registerFunction<FromBase64FlinkFunction, Varchar, Varchar>(
      {prefix + "from_base64"});
  exec::registerStatefulVectorFunction(
      prefix + "like", likeSignatures(), makeLikeFlink);
}

} // namespace facebook::velox::functions::flinksql
