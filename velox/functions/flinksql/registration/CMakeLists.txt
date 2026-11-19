# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

velox_add_library(
  velox_functions_flink
  AggregateFunctionsRegistration.cpp
  ArithmeticFunctionsRegistration.cpp
  ConditionalFunctionsRegistration.cpp
  HashFunctionsRegistration.cpp
  JsonFunctionsRegistration.cpp
  LogicalFunctionsRegistration.cpp
  RegistrationFunctions.cpp
  StringFunctionsRegistration.cpp
  TemporalFunctionsRegistration.cpp)

# The flinksql "not" registration reuses the prestosql udf_not implementation
# (velox/functions/prestosql/Not.cpp), the way sparksql does.
# The flinksql aggregate registrations reuse the shared sum aggregate base
# from velox_functions_lib and register through the aggregate registry in
# velox_exec.
velox_link_libraries(
  velox_functions_flink
  velox_functions_flink_impl
  velox_functions_prestosql_impl
  velox_functions_json
  velox_exec
  Folly::folly)
