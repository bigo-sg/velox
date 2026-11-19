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
#include <string>

namespace facebook::velox::functions::flinksql {

// Register Flink SQL functions. Idempotent within a process; existing
// registrations with the same name are replaced.
void registerFunctions(const std::string& prefix = "");

void registerArithmeticFunctions(const std::string& prefix = "");
void registerConditionalFunctions(const std::string& prefix = "");
void registerHashFunctions(const std::string& prefix = "");
void registerJsonFunctions(const std::string& prefix = "");
void registerLogicalFunctions(const std::string& prefix = "");
void registerStringFunctions(const std::string& prefix = "");
void registerTemporalFunctions(const std::string& prefix = "");

} // namespace facebook::velox::functions::flinksql

namespace facebook::velox::aggregate::flinksql {

// Register Flink SQL aggregate functions under bare names (prefix ""),
// replacing same-name prestosql/sparksql registrations (the Init.cc
// registration order puts the flinksql registrations last in the FLINK
// preset). Flink semantics for the P0 window supply: SUM (result type keeps
// the input type where the baseline widens to BIGINT) and COUNT (BIGINT,
// never null). Idempotent within a process.
void registerAggregateFunctions(
    const std::string& prefix = "",
    bool withCompanionFunctions = false,
    bool overwrite = true);

void registerSumAggregate(
    const std::string& prefix = "",
    bool withCompanionFunctions = false,
    bool overwrite = true);
void registerCountAggregate(
    const std::string& prefix = "",
    bool withCompanionFunctions = false,
    bool overwrite = true);

} // namespace facebook::velox::aggregate::flinksql
