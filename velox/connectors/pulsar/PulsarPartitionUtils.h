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

#include <fmt/format.h>
#include <string>

namespace facebook::velox::connector::pulsar {

inline std::string partitionedTopicName(
    const std::string& topic,
    int32_t partitionIndex) {
  if (partitionIndex < 0) {
    return topic;
  }
  return fmt::format("{}-partition-{}", topic, partitionIndex);
}

} // namespace facebook::velox::connector::pulsar
