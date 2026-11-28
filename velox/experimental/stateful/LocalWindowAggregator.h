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

#include <map>
#include <memory>

#include "velox/exec/AggregateInfo.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/state/KeyTable.h"
#include "velox/experimental/stateful/window/WindowSliceAssigner.h"

namespace facebook::velox::stateful {

/// Local stage of the two-phase window TVF aggregation (Flink
/// LocalSlicingWindowAggOperator, event-time slicing windows only - the
/// planner rule excludes processing-time and session). Pre-aggregates
/// per (key, sliceEnd) into in-memory accumulator rows and flushes
/// partial rows - [key columns, intermediate aggregate columns,
/// windowEnd(BIGINT)] - when the watermark fires the buffered slices,
/// BEFORE forwarding the watermark downstream: the global stage must
/// see the data before the watermark that fires its windows. Keyless
/// windows (empty grouping) are rejected at initialize (they are
/// rejected at plan translation too).
class LocalWindowAggregator : public StatefulOperator {
 public:
  LocalWindowAggregator(
      std::vector<StatefulOperatorPtr> targets,
      std::unique_ptr<WindowSliceAssigner> sliceAssigner,
      std::shared_ptr<const StreamWindowAggregationNode> node,
      exec::DriverCtx* driverCtx);

  void initialize() override;
  void addInput(StreamElementPtr input) override;
  void advance() override;

  /// Flushes every buffered slice that the watermark fires, then
  /// forwards the watermark (data before watermark - the two-phase
  /// ordering contract).
  void processWatermark(int64_t timestamp) override {
    flushFiredSlices(timestamp);
    StatefulOperator::processWatermark(timestamp);
  }

  void close() override;

  std::string name() const override {
    return "LocalWindowAggregator";
  }

 private:
  /// Flushes all buffered entries when the earliest buffered sliceEnd
  /// has fired (Flink RecordsWindowBuffer.advanceProgress flushes on
  /// isWindowFired(minSliceEnd); early delivery of not-yet-fired
  /// slices is harmless - the global accumulates them into slice
  /// state).
  void flushFiredSlices(int64_t progress);

  /// Emits one multi-row StreamRecord carrying every buffered
  /// (key, sliceEnd) partial and drops the buffered rows.
  void flushAll();

  /// Resolves (creating on first touch) the accumulator row for
  /// (keyRow, sliceEnd).
  char* accumulatorRow(const char* keyRow, int64_t sliceEnd);

  const std::shared_ptr<const StreamWindowAggregationNode> node_;
  std::unique_ptr<WindowSliceAssigner> sliceAssigner_;
  std::vector<exec::AggregateInfo> aggregates_;

  /// Detected at initialize only to reject (translation rejects it
  /// first; the member keeps the detection local and explicit).
  bool keyless_ = false;
  RowTypePtr declaredInputType_;

  /// Probe-based keying over an operator-owned table.
  std::unique_ptr<KeyTable> keyTable_;
  std::unique_ptr<KeySelector> keySelector_;

  /// Accumulator rows: aggregates only, the same layout as the global
  /// stage's value rows (keys live in the key table).
  std::unique_ptr<exec::RowContainer> accRows_;

  /// sliceEnd -> key row -> accumulator row.
  std::map<int64_t, std::map<const char*, char*>> entries_;

  const tzdb::time_zone* tz_ = nullptr;
  RowVectorPtr input_;

  std::unordered_map<const char*, vector_size_t> rowToDistinct_;
  std::vector<char*> rowGroups_;
};

} // namespace facebook::velox::stateful
