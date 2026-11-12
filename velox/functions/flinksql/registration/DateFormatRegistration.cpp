/*
 * Copyright (c) Contributors to the GFVBot project.
 * Licensed under the Apache License, Version 2.0.
 */

#include "velox/functions/flinksql/DateFormatFunction.h"

#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneRegistration.h"

namespace facebook::velox::functions::flinksql {

void registerDateFormatFunctions(const std::string& prefix) {
  // The TIMESTAMP WITH TIME ZONE overload needs the custom type registered
  // before its signature is validated (same pattern as prestosql's
  // registerComparisonFunctions).
  registerTimestampWithTimeZoneType();

  // TIMESTAMP (no timezone): wall-clock rendering, session timezone does NOT
  // apply (Flink's DateTimeUtils.formatTimestamp hardcodes UTC). Overrides
  // the inherited sparksql FormatDateTimeFunction under the FLINK preset.
  registerFunction<DateFormatFlinkFunction, Varchar, Timestamp, Varchar>(
      {prefix + "date_format"});

  // TIMESTAMP WITH TIME ZONE: render the real moment in the session
  // timezone (same policy as FROM_UNIXTIME). Overrides the inherited
  // sparksql implementation, which renders in the value's embedded zone.
  registerFunction<
      DateFormatWithTzFlinkFunction,
      Varchar,
      TimestampWithTimezone,
      Varchar>({prefix + "date_format"});
}

} // namespace facebook::velox::functions::flinksql
