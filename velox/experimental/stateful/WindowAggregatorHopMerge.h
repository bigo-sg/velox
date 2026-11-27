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

#include <memory>
#include <vector>

#include "velox/experimental/stateful/agg/AggregateInfos.h"

namespace facebook::velox::memory {
class MemoryPool;
}

namespace facebook::velox::stateful {

/// Batched HOP fire merge: folds 'numSourceRows' source slice accumulator
/// rows into 'targetRow' through the aggregate functions' intermediate
/// representation - one extractAccumulators(numSourceRows) and one
/// addSingleGroupIntermediateResults(...) per aggregate instead of one
/// pair per source row (the row-by-row path pays a virtual extract/merge
/// pair per covered slice; on HOP windows that is size/slide - 1 pairs
/// per fired window, and the fire merges dominate the HOP window busy
/// time).
///
/// The rows passed to addSingleGroupIntermediateResults all point at the
/// same target group, so the fold order is the row order supplied here -
/// callers pass slices in ascending sliceEnd, the same order the
/// row-by-row merge consumed them, keeping the observable result
/// identical (also for float sums, where addition order matters).
///
/// Implemented out-of-line in WindowAggregatorHopMerge.cpp and kept out
/// of the WindowAggregator TU: cold code added to that TU perturbs the
/// hot advance() codegen (the W1 lesson; see WindowAggregatorCountStar
/// for the same pattern).
void mergeAccumulatorRowsBatch(
    char* targetRow,
    const char* const* sourceRows,
    size_t numSourceRows,
    const std::vector<exec::AggregateInfo>& aggregates,
    std::vector<VectorPtr>& scratchColumns,
    memory::MemoryPool* pool);

} // namespace facebook::velox::stateful
