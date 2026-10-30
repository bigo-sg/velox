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
}

} // namespace facebook::velox::functions::flinksql
