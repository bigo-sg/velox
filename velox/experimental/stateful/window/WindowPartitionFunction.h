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

#include "velox/core/PlanNode.h"

namespace facebook::velox::stateful {

/// Partition data according to the timestamp field in RowVector.
/// This is used to partition data for window processing.
class WindowPartitionFunction : public core::PartitionFunction {
 public:
  WindowPartitionFunction(
      const RowTypePtr& inputType,
      const int32_t rowtimeIndex,
      int64_t size,
      int64_t step,
      int64_t offset,
      int windowType);

  std::optional<uint32_t> partition(
      const RowVector& input,
      std::vector<uint32_t>& partitions) override;

  std::optional<int64_t> partition(
     const RowVector& input,
     std::vector<int64_t>& partitions) override;

 private:
  RowTypePtr inputType_;
  int32_t rowtimeIndex_;
  int64_t size_;
  int64_t step_;
  int64_t offset_;
  int windowType_;
  int64_t sliceSize_;
};

class StreamWindowPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  StreamWindowPartitionFunctionSpec(
      const RowTypePtr& inputType,
      int32_t rowtimeIndex,
      int64_t size,
      int64_t step,
      int64_t offset,
      int windowType)
      : inputType_(std::move(inputType)),
        rowtimeIndex_(rowtimeIndex),
        size_(size),
        step_(step),
        offset_(offset),
        windowType_(windowType) {}

  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions,
      bool localExchange) const override;

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& obj,
      void* /* context */);

 private:
  RowTypePtr inputType_;
  int32_t rowtimeIndex_;
  int64_t size_;
  int64_t step_;
  int64_t offset_;
  int windowType_;
};

} // namespace facebook::velox::stateful
