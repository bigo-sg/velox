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

#include "velox/connectors/nexmark/AuctionGenerator.h"
#include "velox/connectors/nexmark/BidGenerator.h"
#include "velox/connectors/nexmark/NexmarkGeneratorConfig.h"
#include "velox/connectors/nexmark/NexmarkUtils.h"
#include "velox/connectors/nexmark/PersonGenerator.h"

#include <random>

namespace facebook::velox::connector::nexmark {

/// `NexmarkGenerator` is the c++ implements of Flink NexmarkGenerator.
/// https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/NexmarkGenerator.java
class NexmarkGenerator {
 public:
  NexmarkGenerator(
      const NexmarkGeneratorConfig& config,
      int64_t eventsCountSoFar,
      int64_t wallclockBaseTime,
      memory::MemoryPool* pool)
      : config_(config),
        eventsCountSoFar_(eventsCountSoFar),
        wallclockBaseTime_(wallclockBaseTime),
        pool_(pool),
        random_(computeNexmarkSeed(
            config_.firstEventId,
            config_.maxEvents,
            config_.firstEventNumber)) {}

  ~NexmarkGenerator() = default;

  bool hasNext() const {
    return eventsCountSoFar_ < config_.maxEvents;
  }

  NextEvent next();

  std::pair<RowVectorPtr, int64_t> nextBatch(size_t rows);

 private:
  int64_t getNextEventId(int64_t eventsCountSoFar) const;

  const NexmarkGeneratorConfig config_;
  int64_t eventsCountSoFar_;
  int64_t wallclockBaseTime_;
  memory::MemoryPool* pool_;

  pcg32_fast random_;
};

} // namespace facebook::velox::connector::nexmark
