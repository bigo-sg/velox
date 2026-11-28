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
#include <vector>

#include "velox/common/memory/MemoryPool.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/type/Type.h"

namespace facebook::velox::stateful {

/// Resolves plan aggregate calls into AggregateInfo entries for stateful
/// operators that drive exec::Aggregate functions over an AggregatingState:
/// argument channels and constants against the declared input schema, the
/// mask channel with its BOOLEAN check, the intermediate type, a
/// cross-check that the declared output column carries exactly the
/// signature-bound result type, and the function instances (single step:
/// raw input in, final values out). Supported shapes are the plain ones -
/// field or constant arguments, an optional column mask, no distinct, no
/// sorting keys. Shared by the aggregation operators: group aggregation
/// today, window aggregation later. 'numLeadingColumns' is the number of
/// output channels ahead of the first aggregate's output (the key columns
/// for group aggregation, keys and window columns for window aggregation).
std::vector<exec::AggregateInfo> makeAggregateInfos(
    const std::vector<core::AggregationNode::Aggregate>& aggregates,
    const RowTypePtr& inputType,
    const RowTypePtr& outputType,
    column_index_t numLeadingColumns,
    const core::QueryConfig& queryConfig,
    memory::MemoryPool* pool);

} // namespace facebook::velox::stateful
