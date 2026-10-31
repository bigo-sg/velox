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
#include "velox/functions/flinksql/LogicalFunctions.h"

namespace facebook::velox::functions {

// Re-registers the prestosql NOT implementation (velox/functions/
// prestosql/Not.cpp) under the flinksql prefix, the way sparksql does for
// its own prefix in registration/RegisterComparison.cpp. This helper must
// live in namespace facebook::velox::functions:
// VELOX_REGISTER_VECTOR_FUNCTION's block-scope extern declaration binds
// registerVectorFunction_udf_not to an entity of the innermost enclosing
// namespace, which has to match Not.cpp's namespace to link.
void registerFlinksqlNot(const std::string& prefix) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");
}

} // namespace facebook::velox::functions

namespace facebook::velox::functions::flinksql {

void registerLogicalFunctions(const std::string& prefix) {
  // NOT reuses the single prestosql NOT implementation. NULLs are preserved
  // by the vector function framework's default null behavior, matching
  // Flink's three-valued NOT.
  registerFlinksqlNot(prefix);
  // Three-valued-logic predicates that never return NULL.
  registerFunction<IsTrueFunction, bool, bool>({prefix + "is_true"});
  registerFunction<IsFalseFunction, bool, bool>({prefix + "is_false"});
  registerFunction<IsNotTrueFunction, bool, bool>({prefix + "is_not_true"});
  registerFunction<IsNotFalseFunction, bool, bool>({prefix + "is_not_false"});
}

} // namespace facebook::velox::functions::flinksql
