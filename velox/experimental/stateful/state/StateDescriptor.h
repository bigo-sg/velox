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
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/type/Type.h"

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

  int32_t keyGroupNumber() const {
    return keyGroupNumber_;
  }

  const std::string operatorId() const {
    return operatorId_;
  }

  memory::MemoryPool* memoryPool() {
    return pool_;
  }

 protected:
 private:
  const std::string name_;
  const std::string operatorId_;
  memory::MemoryPool* pool_;
  int32_t keyGroupNumber_ = 1024;
};

// Descriptor of an AccState: pure description of the acc value — the value
// row layout (accTypes, the intermediate type of each aggregate) plus how a
// fresh row is initialized (an operator-registered callback wrapping
// Aggregate::initializeNewGroups over the operator's own aggregates).
// Mirrors Flink AggregatingStateDescriptor holding createAccumulator. The
// state layer has zero dependency on AggregateInfo: it derives the value
// RowContainer layout from accTypes once at creation time and invokes the
// callback on miss.
class AccStateDescriptor : public StateDescriptor {
 public:
  // Initializes a freshly materialized value row; invoked by the state on
  // each miss. Operator-supplied so that the state never calls any Aggregate
  // method itself.
  using InitRowCallback = std::function<void(char* row)>;

  AccStateDescriptor(
      const std::string& name,
      std::vector<TypePtr> accTypes,
      InitRowCallback initializeRow,
      memory::MemoryPool* pool = nullptr)
      : StateDescriptor(name, "", pool),
        accTypes_(std::move(accTypes)),
        initializeRow_(std::move(initializeRow)) {}

  const std::vector<TypePtr>& accTypes() const {
    return accTypes_;
  }

  const InitRowCallback& initializeRow() const {
    return initializeRow_;
  }

 private:
  const std::vector<TypePtr> accTypes_;
  const InitRowCallback initializeRow_;
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
