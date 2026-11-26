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

#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::stateful {

/// Mix-in interface for aggregate functions that support retracting a raw
/// input row's contribution from an accumulator, the counterpart of Flink's
/// retract methods on the WithRetract aggregate functions. An aggregate
/// opts into retraction by implementing this interface next to its
/// exec::Aggregate base; the group aggregation operator detects it with a
/// dynamic_cast when the plan asks for retraction.
class RetractableAggregate {
 public:
  virtual ~RetractableAggregate() = default;

  /// Removes the contribution of the selected raw input rows from the group
  /// accumulators, mirroring exec::Aggregate::addRawInput parameter for
  /// parameter. Implementations must apply the same null-argument filtering
  /// their accumulate path applies, so a null argument skips the accumulator
  /// instead of retracting it.
  virtual void retractRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;
};

} // namespace facebook::velox::stateful
