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
#include <cstdint>

#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"

namespace facebook::velox::stateful {

/// It is related to
/// org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator
/// in Flink. It extracts timestamp from each row and generates periodic
/// watermark.
class WatermarkAssigner : public StatefulOperator {
 public:
  WatermarkAssigner(
      std::unique_ptr<exec::Operator> op,
      std::vector<std::unique_ptr<StatefulOperator>> targets,
      const int64_t idleTimeout,
      const int rowtimeFieldIndex,
      const int64_t watermarkInterval);

  void addInput(StreamElementPtr input) override;

  void advance() override;

  std::string name() const override {
    return "WatermarkAssigner";
  }

  void close() override;

 private:
  void advanceWatermark();

  RowVectorPtr input_;
  const int64_t idleTimeout_;
  const int rowtimeFieldIndex_;
  const int64_t watermarkInterval_;

  int64_t currentWatermark = 0;
  int64_t lastWatermark = 0;
};
} // namespace facebook::velox::stateful
