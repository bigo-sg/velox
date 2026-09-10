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

#include <folly/Range.h>
#include <map>
#include <memory>
#include <string>
#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/state/CheckpointStream.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {
class RowContainer;
} // namespace facebook::velox::exec

namespace facebook::velox::stateful {

// This class is relevant to Flink org.apache.flink.api.common.State.
class State {
 public:
  static const int VOID_NAMESPACE = 0;
  virtual ~State() = default;
  virtual void clear() = 0;

  /// Checkpoint plane of the heap storage: serializes this state's entries
  /// of one key group / deserializes one entry, and reports the schema
  /// strings the backend cross-checks against the checkpoint header.
  /// Implemented by the heap state handles; states that predate the typed
  /// API keep the defaults and never take part in heap checkpoints.
  virtual void snapshotKeyGroup(
      int32_t /* keyGroupId */,
      CheckpointWriter& /* writer */) {
    VELOX_NYI("This state does not support key-group snapshots");
  }

  virtual void restoreEntry(CheckpointReader& /* reader */) {
    VELOX_NYI("This state does not support entry restore");
  }

  virtual std::string namespaceSchema() const {
    return "";
  }

  virtual std::string valueSchema() const {
    return "";
  }
};

using StatePtr = std::shared_ptr<State>;

/// Velox-specialized aggregating state: the value is a row of a value
/// RowContainer whose layout the state derives once from the descriptor's
/// acc types. On a miss the state materializes a fresh row and initializes
/// it through the operator-registered callback (the operator wraps
/// Aggregate::initializeNewGroups over its own aggregates); the state layer
/// has zero dependency on AggregateInfo. The operator drives addRawInput
/// itself with its own aggregate metadata; the state never orchestrates
/// accumulation. Relevant to Flink AggregatingState.
///
/// The API is batch and explicit: keys may repeat (per-row use, 1:1 with the
/// input rows fed to addRawInput) or be deduplicated (distinct use for join
/// build / rank / sorted aggregation). A single implementation covers both
/// granularities; repeated keys return the same row pointer.
template <typename K, typename N>
class AggregatingState : public State {
 public:
  /// Batch lookup of value row pointers under namespace 'ns'. outRows[i]
  /// receives the value row for keys[i]; a miss creates a new row in the
  /// value RowContainer, initializes it via the descriptor's init callback
  /// and inserts it into the state table. outRows is caller-owned (out
  /// parameter, reused across batches for zero allocation on the hot path).
  virtual void
  rows(folly::Range<const K*> keys, const N& ns, char** outRows) = 0;

  /// Single-key lookup, e.g. for timer callbacks. Same miss semantics as
  /// rows().
  virtual char* row(const K& key, const N& ns) = 0;

  /// The value RowContainer holding the accumulator rows. Created by the
  /// state from the descriptor's acc types, not by the operator.
  virtual exec::RowContainer* valueRows() = 0;
};

// This class is relevant to Flink org.apache.flink.api.common.MapState.
template <typename K, typename N, typename UK, typename UV>
class MapState : public State {
 public:
  virtual UV get(const K& key, const N& ns, const UK& userKey) = 0;

  virtual void
  put(const K& key, const N& ns, const UK& userKey, const UV& value) = 0;

  virtual std::map<UK, UV> entries(const K& key, const N& ns) = 0;

  virtual void remove(const K& key, const N& ns, const UK& userKey) = 0;

  virtual MapVectorPtr vectorGet(const K& key, const N& ns) {
    return nullptr;
  }

  virtual void vectorPut(const K& key, const N& ns, const MapVectorPtr& vec) {}
};

// This class is relevant to Flink org.apache.flink.api.common.ListState.
template <typename K, typename N, typename T>
class ListState : public State {
 public:
  virtual std::vector<T> get(const K& key, const N& ns) = 0;

  virtual void add(const K& key, const N& ns, const T& value) = 0;

  virtual void remove(const K& key, const N& ns) = 0;

  virtual ArrayVectorPtr vectorGet(const K& key, const N& ns) {
    return nullptr;
  }

  virtual void
  vectorUpdate(const K& key, const N& ns, const ArrayVectorPtr& vec) {}

  virtual void vectorAdd(const K& key, const N& ns, const ArrayVectorPtr& vec) {
  }
};

// This class is relevant to Flink org.apache.flink.api.common.ValueState.
template <typename K, typename N, typename V>
class ValueState : public State {
 public:
  virtual V value(const K& key, const N& ns) = 0;

  virtual void update(const K& key, const N& ns, const V& value) = 0;

  virtual void remove(const K& key, const N& ns) = 0;
};

} // namespace facebook::velox::stateful
