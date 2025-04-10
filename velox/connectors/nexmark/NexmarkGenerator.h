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

#include "velox/common/config/Config.h"

namespace facebook::velox::connector::nexmark {

/// `NexmarkGenerator` is the c++ implements of Flink NexmarkGenerator.
/// https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/NexmarkGenerator.java

class NexmarkGenerator {
 public:
  struct Options {
  };

  NexmarkGenerator(NexmarkGenerator::Options options)
      : nexmarkOptions(options) {}

  VectorPtr nextEvent(int rows);

  const NexmarkGenerator::Options nexmarkOptions;
};

} // namespace facebook::velox::connector::nexmark
