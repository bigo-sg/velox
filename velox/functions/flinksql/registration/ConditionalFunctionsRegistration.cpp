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
#include "velox/functions/flinksql/ConditionalFunctions.h"

#include "velox/functions/Registerer.h"

namespace facebook::velox::functions::flinksql {

void registerConditionalFunctions(const std::string& prefix) {
  // IFNULL(T, T) -> T. Flink's planner inserts the casts that bring both
  // operands to their common type, so every supported type needs exactly
  // one same-typed signature (decimals through the generic bindings).
  registerFunction<IfNullFunction, bool, bool, bool>({prefix + "ifnull"});
  registerFunction<IfNullFunction, int8_t, int8_t, int8_t>(
      {prefix + "ifnull"});
  registerFunction<IfNullFunction, int16_t, int16_t, int16_t>(
      {prefix + "ifnull"});
  registerFunction<IfNullFunction, int32_t, int32_t, int32_t>(
      {prefix + "ifnull"});
  registerFunction<IfNullFunction, int64_t, int64_t, int64_t>(
      {prefix + "ifnull"});
  registerFunction<IfNullFunction, float, float, float>({prefix + "ifnull"});
  registerFunction<IfNullFunction, double, double, double>({prefix + "ifnull"});
  registerFunction<IfNullFunction, Varchar, Varchar, Varchar>(
      {prefix + "ifnull"});
  registerFunction<IfNullFunction, Date, Date, Date>({prefix + "ifnull"});
  registerFunction<IfNullFunction, Timestamp, Timestamp, Timestamp>(
      {prefix + "ifnull"});
  registerFunction<
      IfNullFunction,
      ShortDecimal<P1, S1>,
      ShortDecimal<P1, S1>,
      ShortDecimal<P1, S1>>({prefix + "ifnull"});
  registerFunction<
      IfNullFunction,
      LongDecimal<P1, S1>,
      LongDecimal<P1, S1>,
      LongDecimal<P1, S1>>({prefix + "ifnull"});
}

} // namespace facebook::velox::functions::flinksql
