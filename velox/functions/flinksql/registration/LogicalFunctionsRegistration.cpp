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

namespace facebook::velox::functions::flinksql {

void registerLogicalFunctions(const std::string& prefix) {
  // NOT overwrites the prestosql/sparksql vector registration (same
  // semantics, same metadata, same kNot canonical name).
  exec::registerStatefulVectorFunction(
      prefix + "not", notSignatures(), makeNot);
  // Three-valued-logic predicates that never return NULL.
  registerFunction<IsFalseFunction, bool, bool>({prefix + "is_false"});
  registerFunction<IsNotTrueFunction, bool, bool>({prefix + "is_not_true"});
  registerFunction<IsNotFalseFunction, bool, bool>({prefix + "is_not_false"});
}

} // namespace facebook::velox::functions::flinksql
