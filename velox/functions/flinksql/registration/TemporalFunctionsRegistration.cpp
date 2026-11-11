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
#include "velox/functions/flinksql/IntervalArithmetic.h"
#include "velox/functions/flinksql/IntervalComparisons.h"
#include "velox/functions/flinksql/TemporalFunctions.h"

namespace facebook::velox::functions::flinksql {

void registerTemporalFunctions(const std::string& prefix) {
  // FROM_UNIXTIME / UNIX_TIMESTAMP with Flink semantics.
  registerFunction<FromUnixtimeFunction, Varchar, int64_t>(
      {prefix + "from_unixtime"});
  registerFunction<FromUnixtimeWithFormatFunction, Varchar, int64_t, Varchar>(
      {prefix + "from_unixtime"});
  registerFunction<UnixTimestampFunction, int64_t>({prefix + "unix_timestamp"});
  registerFunction<UnixTimestampParseFunction, int64_t, Varchar>(
      {prefix + "unix_timestamp"});
  registerFunction<
      UnixTimestampParseWithFormatFunction,
      int64_t,
      Varchar,
      Varchar>({prefix + "unix_timestamp"});

  // TIMESTAMPADD (Flink rewrites it into interval arithmetic, so these
  // register plus/minus/multiply for the rewritten shapes).
  registerFunction<
      TimestampPlusIntervalDayTimeFunction,
      Timestamp,
      Timestamp,
      IntervalDayTime>({prefix + "plus"});
  registerFunction<
      TimestampMinusIntervalDayTimeFunction,
      Timestamp,
      Timestamp,
      IntervalDayTime>({prefix + "minus"});
  registerFunction<
      TimestampPlusIntervalYearMonthFunction,
      Timestamp,
      Timestamp,
      IntervalYearMonth>({prefix + "plus"});
  registerFunction<
      TimestampMinusIntervalYearMonthFunction,
      Timestamp,
      Timestamp,
      IntervalYearMonth>({prefix + "minus"});
  registerFunction<
      DatePlusIntervalDayTimeFunction,
      Date,
      Date,
      IntervalDayTime>({prefix + "plus"});
  registerFunction<
      DatePlusIntervalYearMonthFunction,
      Date,
      Date,
      IntervalYearMonth>({prefix + "plus"});
  registerFunction<
      DateMinusIntervalDayTimeFunction,
      Date,
      Date,
      IntervalDayTime>({prefix + "minus"});
  registerFunction<
      DateMinusIntervalYearMonthFunction,
      Date,
      Date,
      IntervalYearMonth>({prefix + "minus"});
  registerFunction<
      IntervalDayTimeMultiplyFunction,
      IntervalDayTime,
      IntervalDayTime,
      int64_t>({prefix + "multiply"});
  registerFunction<
      IntervalYearMonthMultiplyFunction,
      IntervalYearMonth,
      IntervalYearMonth,
      int64_t>({prefix + "multiply"});

  // Interval-to-interval comparisons (Flink compares same-family intervals by
  // their underlying storage). The implementations live in
  // velox/functions/flinksql/IntervalComparisons.h. They are registered under
  // dedicated flinksql names: the sparksql baseline already owns the standard
  // comparison names as stateful vector functions with interval-covering
  // signatures, and vector functions take precedence over simple functions at
  // resolution time, so same-name simple registrations would be shadowed.
  registerFunction<
      IntervalDayTimeLessThanFunction,
      bool,
      IntervalDayTime,
      IntervalDayTime>({prefix + "interval_lessthan"});
  registerFunction<
      IntervalDayTimeLessThanOrEqualFunction,
      bool,
      IntervalDayTime,
      IntervalDayTime>({prefix + "interval_lessthanorequal"});
  registerFunction<
      IntervalDayTimeGreaterThanFunction,
      bool,
      IntervalDayTime,
      IntervalDayTime>({prefix + "interval_greaterthan"});
  registerFunction<
      IntervalDayTimeGreaterThanOrEqualFunction,
      bool,
      IntervalDayTime,
      IntervalDayTime>({prefix + "interval_greaterthanorequal"});
  registerFunction<
      IntervalDayTimeEqualToFunction,
      bool,
      IntervalDayTime,
      IntervalDayTime>({prefix + "interval_equalto"});
  registerFunction<
      IntervalDayTimeNotEqualToFunction,
      bool,
      IntervalDayTime,
      IntervalDayTime>({prefix + "interval_notequalto"});
  registerFunction<
      IntervalYearMonthLessThanFunction,
      bool,
      IntervalYearMonth,
      IntervalYearMonth>({prefix + "interval_lessthan"});
  registerFunction<
      IntervalYearMonthLessThanOrEqualFunction,
      bool,
      IntervalYearMonth,
      IntervalYearMonth>({prefix + "interval_lessthanorequal"});
  registerFunction<
      IntervalYearMonthGreaterThanFunction,
      bool,
      IntervalYearMonth,
      IntervalYearMonth>({prefix + "interval_greaterthan"});
  registerFunction<
      IntervalYearMonthGreaterThanOrEqualFunction,
      bool,
      IntervalYearMonth,
      IntervalYearMonth>({prefix + "interval_greaterthanorequal"});
  registerFunction<
      IntervalYearMonthEqualToFunction,
      bool,
      IntervalYearMonth,
      IntervalYearMonth>({prefix + "interval_equalto"});
  registerFunction<
      IntervalYearMonthNotEqualToFunction,
      bool,
      IntervalYearMonth,
      IntervalYearMonth>({prefix + "interval_notequalto"});

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
