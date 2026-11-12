/*
 * Copyright (c) Contributors to the GFVBot project.
 * Licensed under the Apache License, Version 2.0.
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

  // Session-timezone rendering; overrides the inherited sparksql
  // implementation (embedded-zone rendering).
  registerFunction<
      DateFormatWithTzFlinkFunction,
      Varchar,
      TimestampWithTimezone,
      Varchar>({prefix + "date_format"});
}

} // namespace facebook::velox::functions::flinksql
