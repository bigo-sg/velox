/*
 * Copyright (c) Contributors to the GFVBot project.
 * Licensed under the Apache License, Version 2.0.
 */

#include "velox/functions/flinksql/DateFormatFunction.h"

#include "velox/functions/Registerer.h"

namespace facebook::velox::functions::flinksql {

void registerDateFormatFunctions(const std::string& prefix) {
  // TIMESTAMP (no timezone): wall-clock rendering, session timezone does NOT
  // apply (Flink's DateTimeUtils.formatTimestamp hardcodes UTC). Overrides
  // the inherited sparksql FormatDateTimeFunction under the FLINK preset.
  registerFunction<DateFormatFlinkFunction, Varchar, Timestamp, Varchar>(
      {prefix + "date_format"});
}

} // namespace facebook::velox::functions::flinksql
