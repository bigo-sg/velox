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

namespace facebook::velox::stateful {

/// Abstract namespace of keyed state: the second dimension of (key, ns)
/// addressing, e.g. the window id for window operators. Relevant to Flink's
/// namespace type N. Orthogonal to StateKey: the namespace must be stripped
/// out of the key RowContainer because it is dynamically generated (window
/// ids are not known up front) and cannot be probed jointly with the key.
class Namespace {
 public:
  virtual ~Namespace() = default;

  virtual bool equals(const Namespace& other) const = 0;

  /// Participates in the (K, N) composite hash of StateMap.
  virtual uint64_t hash() const = 0;
};

/// Namespace for non-window operators (GroupAggregator etc.). All instances
/// are equal and the hash is constant, so a shared singleton suffices.
///
/// A concrete namespace for window operators (GroupWindowAggregator etc.)
/// is deferred to Phase 2 and should be named after its domain concept
/// (the window), not after a storage type.
class VoidNamespace : public Namespace {
 public:
  static const VoidNamespace& instance() {
    static VoidNamespace kInstance;
    return kInstance;
  }

  bool equals(const Namespace& other) const override {
    return dynamic_cast<const VoidNamespace*>(&other) != nullptr;
  }

  uint64_t hash() const override {
    return 0;
  }
};

} // namespace facebook::velox::stateful
