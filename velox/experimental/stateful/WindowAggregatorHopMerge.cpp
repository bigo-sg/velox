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
#include "velox/experimental/stateful/WindowAggregatorHopMerge.h"

#include <vector>

#include "velox/exec/Aggregate.h"

namespace facebook::velox::stateful {

void mergeAccumulatorRowsBatch(
    char* targetRow,
    const char* const* sourceRows,
    size_t numSourceRows,
    const std::vector<exec::AggregateInfo>& aggregates,
    std::vector<VectorPtr>& scratchColumns,
    memory::MemoryPool* pool) {
  // Lazily (re)size the per-aggregate intermediate scratch columns; the
  // count grows only when the window layout widens (more covered slices),
  // never between calls for the same window shape.
  const auto numRows = static_cast<vector_size_t>(numSourceRows);
  if (scratchColumns.size() != aggregates.size()) {
    scratchColumns.assign(aggregates.size(), nullptr);
  }
  for (size_t i = 0; i < aggregates.size(); ++i) {
    if (scratchColumns[i] == nullptr || scratchColumns[i]->size() < numRows) {
      scratchColumns[i] =
          BaseVector::create(aggregates[i].intermediateType, numRows, pool);
    }
  }
  // One multi-row extraction per aggregate: row r of the scratch column
  // i carries sourceRows[r]'s intermediate state for aggregate i.
  std::vector<char*> rows;
  rows.reserve(numSourceRows);
  for (size_t r = 0; r < numSourceRows; ++r) {
    rows.push_back(const_cast<char*>(sourceRows[r]));
  }
  for (size_t i = 0; i < aggregates.size(); ++i) {
    aggregates[i].function->extractAccumulators(
        rows.data(), numRows, &scratchColumns[i]);
  }
  // One single-group fold per aggregate: every row of the intermediate
  // column lands in the same target group (addIntermediateResults'
  // contract explicitly allows repeated groups), which is exactly the
  // row-by-row merge folded in one call. A member-scratch single-element
  // args vector is avoided: this TU owns nothing per-call mutable, and
  // building {column} per aggregate here is N-per-window, not
  // N-per-merge-unit.
  const SelectivityVector allRows(numRows);
  for (size_t i = 0; i < aggregates.size(); ++i) {
    aggregates[i].function->addSingleGroupIntermediateResults(
        targetRow, allRows, {scratchColumns[i]}, false /* mayPushdown */);
  }
}

} // namespace facebook::velox::stateful
