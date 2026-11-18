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

#include "velox/functions/flinksql/DateFormatFunction.h"

#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneRegistration.h"

namespace facebook::velox::functions::flinksql {

void registerDateFormatFunctions(const std::string& prefix) {
  // Required before the TIMESTAMP WITH TIME ZONE signature is validated.
  registerTimestampWithTimeZoneType();

  // Wall-clock rendering; overrides the inherited sparksql
  // FormatDateTimeFunction.
  registerFunction<DateFormatFlinkFunction, Varchar, Timestamp, Varchar>(
      {prefix + "date_format"});

  // TIMESTAMP WITH TIME ZONE: render the real moment in the session
  // timezone (same policy as FROM_UNIXTIME). The sparksql layer registers
  // date_format only for TIMESTAMP, so this overload is new LTZ coverage
  // with Flink semantics.
  registerFunction<
      DateFormatWithTzFlinkFunction,
      Varchar,
      TimestampWithTimezone,
      Varchar>({prefix + "date_format"});

  // TIMESTAMP carrying a genuine instant (the gluten planner's erased
  // representation of a Timestamp_LTZ column): same session-timezone
  // rendering, reached by name from the planner's DATE_FORMAT mapping
  // when the Flink operand type is TIMESTAMP_LTZ.
  registerFunction<DateFormatLtzFlinkFunction, Varchar, Timestamp, Varchar>(
      {prefix + "date_format_ltz"});
}

} // namespace facebook::velox::functions::flinksql
