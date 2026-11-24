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

namespace facebook::velox::stateful {

/// Abstract namespace of keyed state: the second dimension of (key, ns)
/// addressing, e.g. the window id for window operators. Relevant to Flink's
/// namespace type N. Orthogonal to StateKey: the namespace must be stripped
/// out of the key RowContainer because it is dynamically generated (window
/// ids are not known up front) and cannot be probed jointly with the key.
///
/// The namespace crosses engine boundaries as this runtime contract, not as
/// a static type: state storages erase their namespace dimension to
/// Namespace and hold entries' namespaces as immutable clones, so a state
/// handle created through one namespace type never depends on that type at
/// storage level.
class Namespace {
 public:
  virtual ~Namespace() = default;

  /// Equality is equality of kind plus equality of the kind's own payload;
  /// two namespaces of different kinds are never equal.
  virtual bool equals(const Namespace& other) const = 0;

  /// Participates in the (K, N) composite hash of StateMap.
  virtual uint64_t hash() const = 0;

  /// Identifies the namespace kind - the domain concept the namespace
  /// belongs to (the void namespace, a time window, ...). Every namespace
  /// type declares its kind next to its class, as a constant numbered from
  /// 1; 0 is reserved. The kind is a process-stable number (it may be
  /// recorded in checkpoint metadata next to the serializer's schema id),
  /// and a backend that mixes namespace kinds across its states checks it
  /// at the state's entry points: a namespace of another kind handed to a
  /// state fails there instead of probing with it.
  virtual uint32_t kind() const = 0;

  /// An owning immutable copy safe to store in a state entry; stateless
  /// namespaces return a shared singleton.
  virtual std::shared_ptr<const Namespace> clone() const = 0;
};

/// Kind of VoidNamespace.
constexpr uint32_t kVoidNamespaceKind = 1;

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

  // The kind is the whole identity: nothing of another kind equals the
  // void namespace, and every void namespace equals every other.
  bool equals(const Namespace& other) const override {
    return other.kind() == kind();
  }

  uint64_t hash() const override {
    return 0;
  }

  uint32_t kind() const override {
    return kVoidNamespaceKind;
  }

  std::shared_ptr<const Namespace> clone() const override {
    static const std::shared_ptr<const VoidNamespace> kShared =
        std::make_shared<VoidNamespace>();
    return kShared;
  }
};

} // namespace facebook::velox::stateful
