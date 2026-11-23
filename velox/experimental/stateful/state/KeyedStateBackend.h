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
#include "velox/experimental/stateful/state/CheckpointStream.h"
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
  // Keep the checkpoint-side snapshot(int64_t, int64_t, CheckpointOptions)
  // visible: the streaming snapshot() below would otherwise hide it for
  // callers going through this base class.
  using Snapshotable::snapshot;

  /// Writes the whole keyed state as a checkpoint stream to 'sink'. The
  /// backend commits its in-memory changes (commit()) and releases dead
  /// resources (reclaim()) first, so the stream reflects every state entry
  /// and no dead key row sits across the barrier. Implementations must
  /// write through the sink and must not require the stream to be buffered
  /// in full.
  virtual void snapshot(CheckpointSink& sink) = 0;

  /// Reads a checkpoint stream produced by snapshot(CheckpointSink&) back
  /// into the backend, merging its entries into the states registered
  /// before the call (as in Flink): an entry in the stream overwrites a
  /// live entry of the same key, and a live entry the stream does not carry
  /// is left alone. Implementations must consume the stream through
  /// 'source' and must not require it to be buffered in full.
  virtual void restore(CheckpointSource& source) = 0;

  /// Commits state changes this backend still holds in memory to its
  /// storage. No-op for backends whose state is committed in place.
  virtual void commit() = 0;

  /// Releases resources no state entry refers to any more (index entries,
  /// cached rows, ...). Callers must not hold any accessor result across
  /// it. Implementations may make it a no-op.
  virtual void reclaim() = 0;

  /// Convenience: writes the checkpoint stream into an in-memory buffer and
  /// returns it. Backends whose state is fully resident materialize the
  /// stream; the streaming interface exists so they need not.
  std::string snapshotBytes() {
    BufferCheckpointSink sink;
    snapshot(sink);
    return sink.finish();
  }

  /// Convenience: feeds the given stream to restore(CheckpointSource&).
  void restoreBytes(std::string_view bytes) {
    BufferCheckpointSource source(bytes);
    restore(source);
  }

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
};

using KeyedStateBackendPtr = std::shared_ptr<KeyedStateBackend>;

} // namespace facebook::velox::stateful
