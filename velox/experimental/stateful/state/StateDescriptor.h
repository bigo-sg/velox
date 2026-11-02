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

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink org.apache.flink.api.common.StateDescriptor.
class StateDescriptor {
 public:
  StateDescriptor(
      const std::string& name,
      const std::string& operatorId = "",
      memory::MemoryPool* pool = nullptr)
      : name_(name), operatorId_(operatorId), pool_(pool) {}

  const std::string name() const {
    return name_;
  }

  const std::string operatorId() const {
    return operatorId_;
  }

  memory::MemoryPool* memoryPool() const {
    return pool_;
  }

 protected:
 private:
  const std::string name_;
  const std::string operatorId_;
  memory::MemoryPool* pool_;
};

// Descriptor of an AggregatingState: the value rows are an accumulator
// layout RowContainer built from 'accumulators' (one entry per aggregate, in
// aggregate order), plus how rows are constructed and merged back from
// intermediate results (operator-registered callbacks wrapping
// Aggregate::initializeNewGroups / addSingleGroupIntermediateResults over the
// operator's own aggregates). Mirrors Flink AggregatingStateDescriptor
// holding createAccumulator. The state layer has zero dependency on
// AggregateInfo: it drives construction and checkpoint round-trips entirely
// through the accumulator array and the two callbacks.
class AggregatingStateDescriptor : public StateDescriptor {
 public:
  // Initializes freshly materialized value rows; invoked by the state once
  // per batch of misses (a single miss arrives as a one-row array). The
  // batch shape matches Aggregate::initializeNewGroups. Operator-supplied so
  // that the state never calls any Aggregate method itself.
  using InitRowsCallback = std::function<void(char** rows, size_t numRows)>;

  // Merges row 0 of 'columns' (one one-row intermediate column per
  // accumulator, in descriptor order; produced by Accumulator::extractForSpill
  // on the snapshot side) into the accumulators of 'row'. Invoked by the state
  // on restore, after 'initializeRows' has constructed the fresh accumulators —
  // the same construct-then-merge sequence velox uses to reload spilled
  // aggregation state. One keyed entry restores one row, so the columns
  // always carry exactly one row.
  using MergeRowCallback =
      std::function<void(char* row, const std::vector<VectorPtr>& columns)>;

  AggregatingStateDescriptor(
      const std::string& name,
      std::vector<exec::Accumulator> accumulators,
      InitRowsCallback initializeRows,
      MergeRowCallback mergeRow,
      memory::MemoryPool* pool = nullptr)
      : StateDescriptor(name, "", pool),
        accumulators_(std::move(accumulators)),
        initializeRows_(std::move(initializeRows)),
        mergeRow_(std::move(mergeRow)) {}

  const std::vector<exec::Accumulator>& accumulators() const {
    return accumulators_;
  }

  // The spill type of each accumulator, in descriptor order — the type of the
  // serialized intermediate state the checkpoint stores per accumulator (the
  // same type native spilling keys on; equal to the aggregate's intermediate
  // type for plain aggregates).
  std::vector<TypePtr> spillTypes() const {
    std::vector<TypePtr> types;
    types.reserve(accumulators_.size());
    for (const auto& accumulator : accumulators_) {
      types.push_back(accumulator.spillType());
    }
    return types;
  }

  const InitRowsCallback& initializeRows() const {
    return initializeRows_;
  }

  const MergeRowCallback& mergeRow() const {
    return mergeRow_;
  }

 private:
  // Non-owning: each entry's closures capture the operator's raw Aggregate
  // pointers. The Aggregate objects must outlive every state handle created
  // from this descriptor (the value container destroys accumulators through
  // these closures) and must have setAllocator/setOffsets wired to the
  // state's valueRows() container before the first rows() call.
  const std::vector<exec::Accumulator> accumulators_;
  const InitRowsCallback initializeRows_;
  const MergeRowCallback mergeRow_;
};

// Descriptor of a ValueState: the value type V is serialized with
// 'serializer'. Relevant to Flink ValueStateDescriptor.
template <typename V>
class ValueStateDescriptor : public StateDescriptor {
 public:
  ValueStateDescriptor(
      const std::string& name,
      TypeSerializerPtr serializer,
      memory::MemoryPool* pool = nullptr)
      : StateDescriptor(name, "", pool), serializer_(std::move(serializer)) {}

  const TypeSerializerPtr& serializer() const {
    return serializer_;
  }

 private:
  const TypeSerializerPtr serializer_;
};

// Descriptor of a ListState: elements of type T are serialized with
// 'serializer'. Relevant to Flink ListStateDescriptor.
template <typename T>
class ListStateDescriptor : public StateDescriptor {
 public:
  ListStateDescriptor(
      const std::string& name,
      TypeSerializerPtr serializer,
      memory::MemoryPool* pool = nullptr)
      : StateDescriptor(name, "", pool), serializer_(std::move(serializer)) {}

  const TypeSerializerPtr& serializer() const {
    return serializer_;
  }

 private:
  const TypeSerializerPtr serializer_;
};

// Descriptor of a MapState: user keys of type UK and user values of type UV
// are serialized with the respective serializers. Relevant to Flink
// MapStateDescriptor.
template <typename UK, typename UV>
class MapStateDescriptor : public StateDescriptor {
 public:
  MapStateDescriptor(
      const std::string& name,
      TypeSerializerPtr keySerializer,
      TypeSerializerPtr valueSerializer,
      memory::MemoryPool* pool = nullptr)
      : StateDescriptor(name, "", pool),
        keySerializer_(std::move(keySerializer)),
        valueSerializer_(std::move(valueSerializer)) {}

  const TypeSerializerPtr& keySerializer() const {
    return keySerializer_;
  }

  const TypeSerializerPtr& valueSerializer() const {
    return valueSerializer_;
  }

 private:
  const TypeSerializerPtr keySerializer_;
  const TypeSerializerPtr valueSerializer_;
};

} // namespace facebook::velox::stateful
