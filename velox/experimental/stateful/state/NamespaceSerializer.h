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
#include <string>
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/Namespace.h"

namespace facebook::velox::stateful {

/// Serializes VoidNamespace. The namespace is stateless: any payload is
/// empty and all instances are equal.
class VoidNamespaceSerializer : public TypeSerializer<VoidNamespace> {
 public:
  std::string schemaId() const override {
    return "void";
  }

  std::string serialize(const VoidNamespace& /* ns */) override {
    return "";
  }

  VoidNamespace deserialize(std::string_view /* str */) override {
    return VoidNamespace{};
  }
};

/// The namespace's runtime carrier across the engine boundary: what a
/// state creation entry receives in place of the static namespace type,
/// in the shape of Flink's createInternalState, which receives the
/// namespace serializer as an argument. Erases the namespace dimension to
/// the Namespace contract; the schema id identifies the namespace type in
/// the registry's name-reuse checks and the checkpoint schema table.
class NamespaceSerializer : public TypeBaseSerializer {
 public:
  /// Serializes 'ns'; all namespaces stored in one state share the type
  /// this serializer was created for.
  virtual std::string serialize(const Namespace& ns) = 0;

  /// Returns an owning namespace; implementations must not keep the view
  /// (or pointers into it) beyond the returned value's own construction.
  virtual std::shared_ptr<const Namespace> deserialize(
      std::string_view bytes) = 0;

  /// The namespace kind this serializer serves (Namespace::kind()). A state
  /// remembers it at creation and admits only namespaces of that kind, so
  /// a namespace of another kind fails at the state's entry points instead
  /// of being probed with; the schema id above stays for checkpoint
  /// metadata.
  virtual uint32_t kind() const = 0;
};

/// The N-typed binding of NamespaceSerializer: wraps a TypeSerializer<N>,
/// upcasting namespaces on serialize and boxing the deserialized value.
template <typename N>
class TypedNamespaceSerializer : public NamespaceSerializer {
 public:
  explicit TypedNamespaceSerializer(
      std::shared_ptr<TypeSerializer<N>> serializer)
      : serializer_(std::move(serializer)) {}

  std::string schemaId() const override {
    return serializer_->schemaId();
  }

  // Namespace types are default-constructible value objects, so the kind
  // reads off a fresh one.
  uint32_t kind() const override {
    return N{}.kind();
  }

  std::string serialize(const Namespace& ns) override {
    return serializer_->serialize(static_cast<const N&>(ns));
  }

  std::shared_ptr<const Namespace> deserialize(
      std::string_view bytes) override {
    return std::make_shared<N>(serializer_->deserialize(bytes));
  }

 private:
  std::shared_ptr<TypeSerializer<N>> serializer_;
};

/// Compile-time binding of a namespace type to its serializer, in the
/// shape of TypeTraits: the primary template is undefined, so a namespace
/// type without a specialization fails to compile where a handle
/// constructs its serializer; a new namespace type plugs in here.
template <typename N>
struct NamespaceSerializerTraits;

template <>
struct NamespaceSerializerTraits<VoidNamespace> {
  static std::shared_ptr<NamespaceSerializer> create() {
    return std::make_shared<TypedNamespaceSerializer<VoidNamespace>>(
        std::make_shared<VoidNamespaceSerializer>());
  }
};

} // namespace facebook::velox::stateful
