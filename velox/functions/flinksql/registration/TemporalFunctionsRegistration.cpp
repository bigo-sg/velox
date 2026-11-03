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
#include "velox/functions/flinksql/DateTimeFunctions.h"
#include "velox/functions/flinksql/TemporalFunctions.h"

namespace facebook::velox::functions::flinksql {

void registerTemporalFunctions(const std::string& prefix) {
  registerFunction<CurrentDateFunction, Date>({prefix + "current_date"});
  registerFunction<CurrentTimeFunction, int32_t>({prefix + "current_time"});
  // Per-record evaluation (Flink streaming semantics): the instant is read
  // from the RecordClock pinned at the record-emission boundary
  // (StatefulOperator::pushOutput), so all temporal occurrences in one
  // record share a single instant; an unpinned thread falls back to a fresh
  // wall-clock reading per call. "current_timestamp" overwrites the sparksql
  // registration of identical shape (the runtime path moves to the flinksql
  // namespace).
  registerFunction<FlinkCurrentTimestampFunction, Timestamp>(
      {prefix + "current_timestamp", prefix + "now"});
  registerFunction<FlinkLocalTimestampFunction, Timestamp>(
      {prefix + "localtimestamp"});
  // LOCALTIME returns INTEGER: the milliseconds-of-day carrier of Flink's
  // TIME(0) (the exact RowData representation, so raw projections flow back
  // to the sink without carrier conversion). The conversion uses floor-mod,
  // keeping the result domain [0, 86'400'000) even for pre-1970 local
  // millis.
  registerFunction<FlinkLocalTimeFunction, int32_t>({prefix + "localtime"});

  registerFunction<ToDateFunction, Date, Varchar>({prefix + "to_date"});
  registerFunction<ToDateWithFormatFunction, Date, Varchar, Varchar>(
      {prefix + "to_date"});

  registerFunction<ToTimestampFunction, Timestamp, Varchar>(
      {prefix + "to_timestamp"});
  registerFunction<ToTimestampWithFormatFunction, Timestamp, Varchar, Varchar>(
      {prefix + "to_timestamp"});

  registerFunction<ToTimestampLtzIntegralFunction, Timestamp, int8_t, int32_t>(
      {prefix + "to_timestamp_ltz"});
  registerFunction<ToTimestampLtzIntegralFunction, Timestamp, int16_t, int32_t>(
      {prefix + "to_timestamp_ltz"});
  registerFunction<ToTimestampLtzIntegralFunction, Timestamp, int32_t, int32_t>(
      {prefix + "to_timestamp_ltz"});
  registerFunction<ToTimestampLtzIntegralFunction, Timestamp, int64_t, int32_t>(
      {prefix + "to_timestamp_ltz"});
  registerFunction<ToTimestampLtzDoubleFunction, Timestamp, double, int32_t>(
      {prefix + "to_timestamp_ltz"});
  registerFunction<
      ToTimestampLtzDecimalFunction,
      Timestamp,
      ShortDecimal<P1, S1>,
      int32_t>({prefix + "to_timestamp_ltz"});
  registerFunction<
      ToTimestampLtzDecimalFunction,
      Timestamp,
      LongDecimal<P1, S1>,
      int32_t>({prefix + "to_timestamp_ltz"});
}

} // namespace facebook::velox::functions::flinksql
