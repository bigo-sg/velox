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

#include "velox/functions/Registerer.h"
#include "velox/functions/flinksql/HashFunctions.h"

namespace facebook::velox::functions::flinksql {

void registerHashFunctions(const std::string& prefix) {
  registerFunction<Sha2Function, Varchar, Varchar, int8_t>({prefix + "sha2"});
  registerFunction<Sha2Function, Varchar, Varchar, int16_t>({prefix + "sha2"});
  registerFunction<Sha2Function, Varchar, Varchar, int32_t>({prefix + "sha2"});
  registerFunction<Sha2Function, Varchar, Varchar, int64_t>({prefix + "sha2"});

  registerFunction<Sha224Function, Varchar, Varchar>({prefix + "sha224"});
  registerFunction<Sha256Function, Varchar, Varchar>({prefix + "sha256"});
  registerFunction<Sha384Function, Varchar, Varchar>({prefix + "sha384"});
  registerFunction<Sha512Function, Varchar, Varchar>({prefix + "sha512"});
}

} // namespace facebook::velox::functions::flinksql
