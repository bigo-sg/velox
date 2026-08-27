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
#include <memory>

#include "velox/exec/RowContainer.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/StateDescriptor.h"
#include "velox/experimental/stateful/state/StateTable.h"

namespace facebook::velox::stateful {

/// AccState on the heap storage: the value is a char* row of a value
/// RowContainer whose layout is derived once from the descriptor's acc
/// types. On a miss the state materializes a fresh row, initializes it
/// through the descriptor's callback (the operator wraps
/// Aggregate::initializeNewGroups over its own aggregates) and inserts it
/// into the state table. The state never calls any Aggregate method itself.
/// Relevant to Flink HeapAggregatingState.
/// @param <K> type of key, a StateKey subclass
/// @param <N> type of namespace, a Namespace subclass
template <typename K, typename N>
class HeapAccState : public AccState<K, N> {
 public:
  HeapAccState(
      std::shared_ptr<StateTable<K, N, char*>> stateTable,
      const AccStateDescriptor& descriptor)
      : stateTable_(std::move(stateTable)),
        valueRows_(std::make_unique<exec::RowContainer>(
            descriptor.accTypes(),
            descriptor.memoryPool())),
        initializeRow_(descriptor.initializeRow()) {}

  void rows(folly::Range<const K*> keys, const N& ns, char** outRows)
      override {
    for (size_t i = 0; i < keys.size(); ++i) {
      outRows[i] = row(keys[i], ns);
    }
  }

  char* row(const K& key, const N& ns) override {
    if (char* value = stateTable_->get(key, ns)) {
      return value;
    }
    char* newRow = valueRows_->newRow();
    initializeRow_(newRow);
    stateTable_->put(key, ns, newRow);
    return newRow;
  }

  exec::RowContainer* valueRows() override {
    return valueRows_.get();
  }

  void clear() override {
    stateTable_->clear();
  }

 private:
  std::shared_ptr<StateTable<K, N, char*>> stateTable_;
  std::unique_ptr<exec::RowContainer> valueRows_;
  AccStateDescriptor::InitRowCallback initializeRow_;
};
} // namespace facebook::velox::stateful
