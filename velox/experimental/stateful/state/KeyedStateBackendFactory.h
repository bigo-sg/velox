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
#include <memory>

#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"

namespace facebook::velox::stateful {

/// The typed creation entry of the keyed state, called once per operator
/// at operator initialization: builds the operator's keyed backend from
/// the plan's key schema (the KeyedStateSpec) and the task backend's
/// key-group parameters - the one place those numbers exist at runtime -
/// dispatched on the task backend's declared type. A declared type with
/// no engine behind it fails here, at operator initialization, instead
/// of silently producing a heap backend. K is the operator's key type (a
/// StateKey subclass); a free function template because the entry cannot
/// be virtual (a virtual function cannot be a template) and needs none:
/// dispatch is the switch on the declared type, and the operator receives
/// the typed base, not a concrete backend class.
///
/// The entry lives in this header, apart from any engine's own header:
/// it is the one place that must see every concrete backend, and an
/// engine header must not carry another engine's wiring - the RocksDB
/// case lands here once that backend has a typed creation path.
template <typename K>
std::unique_ptr<TypedKeyedStateBackend<K>> createKeyedStateBackend(
    const StateBackend& taskBackend,
    const KeyedStateSpec& spec,
    memory::MemoryPool* pool) {
  switch (taskBackend.parameters()->getBackendType()) {
    case StateBackendType::HEAP:
      return std::make_unique<HeapKeyedStateBackend<K>>(
          spec.keyTypes(),
          taskBackend.parameters()->getMaxParallelism(),
          taskBackend.parameters()->getStartKeyGroup(),
          taskBackend.parameters()->getNumKeyGroups(),
          pool);
    case StateBackendType::ROCKSDB:
      VELOX_UNSUPPORTED("The RocksDB keyed state backend is not implemented");
  }
  VELOX_UNREACHABLE();
}

} // namespace facebook::velox::stateful
