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
#include "velox/experimental/stateful/WindowAggregator.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

// Count star detection (Flink indexOfCountStar): the first
// zero-argument UNFILTERED COUNT call (AggregateUtil requires
// filterArg < 0 - a filtered COUNT(*) is not a count star, the
// empty-window check stays disabled and zero-count windows emit).
// Hopping windows require one - Flink checkArgument.
int32_t findCountStarIndex(
    const std::vector<core::AggregationNode::Aggregate>& aggregates,
    bool requireCountStar) {
  int32_t index = -1;
  for (size_t i = 0; i < aggregates.size(); ++i) {
    const auto& aggregate = aggregates[i];
    if (aggregate.call->name() == "count" && aggregate.call->inputs().empty() &&
        aggregate.mask == nullptr && !aggregate.distinct &&
        aggregate.sortingKeys.empty()) {
      index = static_cast<int32_t>(i);
      break;
    }
  }
  if (requireCountStar) {
    VELOX_CHECK(
        index >= 0,
        "Hopping window requires a COUNT(*) in the aggregate functions.");
  }
  return index;
}

// Empty-window check on an accumulator row (Flink
// WindowIsEmptySupplier): true when the count star reads 0. With no
// count star in the query the check is disabled (Flink returns false
// when indexOfCountStar < 0).
bool countStarIsZero(const char* row, int32_t countStarOffset) {
  if (countStarOffset < 0) {
    return false;
  }
  return *reinterpret_cast<const int64_t*>(row + countStarOffset) == 0;
}

// The count star accumulator column must be a never-null inline int64
// (SimpleNumericAggregate<bool, int64_t, int64_t>), so a raw read at
// the captured offset is the live count.
int32_t countStarColumnOffset(TypeKind intermediateKind, int32_t offset) {
  VELOX_CHECK_EQ(
      intermediateKind,
      TypeKind::BIGINT,
      "The count star accumulator column must be BIGINT");
  return offset;
}

} // namespace facebook::velox::stateful
