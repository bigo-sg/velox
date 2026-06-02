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

#include <climits>
#include <map>
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

/// This class is relevant to flink KeySelector.
/// It can partition the RowVector according to the key fields.
class KeySelector {
 public:
  KeySelector(
      std::unique_ptr<core::PartitionFunction> partitionFunction,
      memory::MemoryPool* pool,
      int numPartitions = INT_MAX);

  std::map<int64_t, RowVectorPtr> partition(const RowVectorPtr& input);

 private:
  void prepareForInput(const RowVectorPtr& input);

  void allocateIndexBuffers(
      const std::map<int64_t, vector_size_t>& numOfKeys,
      std::map<int64_t, BufferPtr>& keyToIndexBuffers,
      std::map<int64_t, vector_size_t*>& keyToRawIndices);

  RowVectorPtr wrapChildren(
      const RowVectorPtr& input,
      vector_size_t size,
      const BufferPtr& indices);

  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  memory::MemoryPool* pool_;
  const int numPartitions_;
};

} // namespace facebook::velox::stateful
