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

 #include <type/Type.h>
 #include <vector/ComplexVector.h>
 #include <vector/FlatVector.h>
 #include <string>
 #include <vector>
 #include <random>

namespace facebook::velox::connector::nexmark {
inline std::string formatDateTime(int64_t dateTime) {
  // Convert milliseconds to seconds for std::chrono
  std::time_t seconds = dateTime / 1000;
  int milliseconds = dateTime % 1000;

  // Convert to UTC time
  std::tm tm;
  gmtime_r(&seconds, &tm);

  // Format the time into a string
  char buffer[30];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &tm);

  // Append milliseconds and 'Z' for UTC
  return std::string(buffer) + "." + std::to_string(milliseconds).substr(0, 3) +
      "Z";
}

} // namespace facebook::velox::connector::nexmark
