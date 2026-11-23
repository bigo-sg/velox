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
#include <memory>
#include <optional>
#include <vector>

#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/rank/DeduplicateRanker.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"

namespace facebook::velox::stateful {

/// Operator-owned keyed state wrapper for DeduplicateRanker: builds the typed
/// keyed state backend for the dedup operator at initialize time, probes the
/// input by key, and drives the dedup state machine per key. Supports both
/// keyed deduplicate (non-empty key channels) and global deduplicate (empty
/// key channels -> a single synthetic constant key).
class StreamDeduplicateOperator : public StatefulOperator {
 public:
  StreamDeduplicateOperator(
      std::unique_ptr<DeduplicateRanker> ranker,
      std::vector<column_index_t> keyChannels,
      std::vector<TypePtr> keyTypes,
      std::vector<std::unique_ptr<StatefulOperator>> targets);

  void initializeStateBackend(StateBackend* stateBackend) override;

  void initializeState() override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  /// The operator-owned typed keyed state backend (实现契约§5 访问器)；快照与
  /// 恢复字节持久化由 task 侧 checkpoint 编排驱动。
  HeapKeyedStateBackend<RowContainerStateKey>* keyedStateBackend() const {
    return backend_.get();
  }

 private:
  DeduplicateRanker* ranker_;
  const std::vector<column_index_t> keyChannels_;
  const std::vector<TypePtr> keyTypes_;
  const bool global_;
  std::unique_ptr<HeapKeyedStateBackend<RowContainerStateKey>> backend_;
  std::unique_ptr<KeySelector> keySelector_;
  std::optional<RowContainerStateKey> globalKey_;

  RowVectorPtr input_;
};

} // namespace facebook::velox::stateful
