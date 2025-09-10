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
#include "velox/experimental/stateful/udf/Register.h"
#include "velox/experimental/stateful/udf/BigoUDF.h"
#include "velox/experimental/stateful/udf/ExtractDateTime.h"
#include "velox/functions/Registerer.h"

namespace facebook::velox::stateful::udf {

void registerFunctions(const std::string& prefix) {
  registerFunction<CountCharFunction, int64_t, Varchar, Varchar>(
      {prefix + "count_char"});
  registerFunction<ExtractFunction, int64_t, Varchar, Timestamp>(
      {prefix + "extract"});
}

} // namespace facebook::velox::stateful::udf
