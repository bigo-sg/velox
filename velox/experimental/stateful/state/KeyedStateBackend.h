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

#include "velox/experimental/stateful/InternalTimerService.h"
#include "velox/experimental/stateful/state/CheckpointListener.h"
#include "velox/experimental/stateful/state/NamespaceSerializer.h"
#include "velox/experimental/stateful/state/Snapshotable.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink
// org.apache.flink.runtime.state.KeyedStateBackend.
class KeyedStateBackend : public Snapshotable, public CheckpointListener {
 public:
  /// Virtual because owners hold the backend through base pointers: the
  /// operator's typed face (TypedKeyedStateBackend<K> below) and the
  /// type-erased checkpoint-walk door alike.
  virtual ~KeyedStateBackend() = default;

  virtual std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>>
  getOrCreateMapState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<ListState<uint32_t, int64_t, RowVectorPtr>>
  getOrCreateListState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<ValueState<int64_t, int64_t, RowVectorPtr>>
  getOrCreateValueState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>>
  getOrCreateGroupValueState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>>
  getOrCreateGroupMapState(StateDescriptor& stateDescriptor) = 0;

  virtual std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>>
  getOrCreateRankMapState(StateDescriptor& stateDescriptor) = 0;

  // TODO: Flink create PriorityQueue.
  virtual std::shared_ptr<InternalTimerService<int64_t, int64_t>>
  createTimerService(Triggerable<int64_t, int64_t>* triggerable) = 0;

  virtual std::shared_ptr<InternalTimerService<int64_t, TimeWindow>>
  createGroupWindowAggTimerService(
      Triggerable<int64_t, TimeWindow>* triggerable) = 0;

  /// Serializes the whole backend into one checkpoint byte stream. This is
  /// what the stateful operators' checkpoint walk collects through
  /// StatefulOperator::snapshotState - the migration of the pre-generic
  /// snapshot() above: byte-level checkpointing owned by the backend, not
  /// the operator.
  virtual std::string snapshotBytes() = 0;

  /// Restores the stream written by snapshotBytes() into the states this
  /// backend has registered; the states must be registered before the call.
  virtual void restoreBytes(const std::string& bytes) = 0;

  /// Batch-boundary key reclamation (see KeyTable::advance); operators
  /// call it at the end of each input batch, and it runs first inside
  /// snapshotBytes().
  virtual void advance() = 0;
};

class KeyIndex;

/// The K-typed face of a keyed state backend: what an operator works
/// through without knowing which engine storage is behind it. K is the
/// operator's key type (a StateKey subclass) - only the operator can name
/// it, so this base is a template layered on the type-erased
/// KeyedStateBackend above (the door the checkpoint walk uses), and
/// creation goes through the free function template
/// createKeyedStateBackend<K> declared in KeyedStateBackendFactory.h rather
/// than a virtual: a virtual function cannot be a template. The same limit
/// keeps the value / list / map factories (getOrCreate<...> templates,
/// which construct engine handle classes over (K, N)) below this
/// interface, on the engine classes, until their operators migrate; the
/// aggregating factory fits below this interface as a single virtual, with
/// the namespace demoted to a runtime serializer object.
///
/// This class is relevant to Flink
/// org.apache.flink.runtime.state.KeyedStateBackend<K>.
template <typename K>
class TypedKeyedStateBackend : public KeyedStateBackend {
 public:
  /// The key-resolution door KeySelectors bind to and probe through.
  virtual KeyIndex& keyIndex() = 0;

  /// Size of the key-group space: keyGroup = hash % maxParallelism.
  virtual uint32_t maxParallelism() const = 0;

  /// Inclusive start of this backend's key-group sub-range.
  virtual uint32_t startKeyGroup() const = 0;

  /// Number of key groups in this backend's sub-range.
  virtual uint32_t numKeyGroups() const = 0;

  /// Creates or returns the aggregating state of 'descriptor' under the
  /// namespace type carried by 'nsSerializer' — the namespace as a runtime
  /// object instead of a static type, in the shape of Flink's
  /// createInternalState, which receives the namespace serializer as an
  /// argument. The returned state's methods take the Namespace base type.
  virtual std::shared_ptr<AggregatingState<K>> getOrCreateAggregatingState(
      const AggregatingStateDescriptor& descriptor,
      std::shared_ptr<NamespaceSerializer> nsSerializer) = 0;
};

using KeyedStateBackendPtr = std::shared_ptr<KeyedStateBackend>;

} // namespace facebook::velox::stateful
