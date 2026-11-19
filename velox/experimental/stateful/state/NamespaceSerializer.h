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

#include <string>
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/window/Window.h"

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

/// Serializes a TimeWindow namespace: a fixed-width 16-byte payload of the
/// start and end epoch-millis, each little-endian, mirroring Flink's
/// TimeWindow.Serializer layout. The schema id is fixed once ("time-window"):
/// checkpoint restore cross-checks it, so an evolution needs a versioned id.
class TimeWindowSerializer : public TypeSerializer<TimeWindow> {
 public:
  std::string schemaId() const override {
    return "time-window";
  }

  std::string serialize(const TimeWindow& window) override {
    return littleEndianBytes(window.start()) + littleEndianBytes(window.end());
  }

  TimeWindow deserialize(std::string_view str) override {
    VELOX_CHECK_EQ(
        str.size(),
        sizeof(int64_t) + sizeof(int64_t),
        "Corrupt checkpoint: truncated time-window namespace (expected 16 bytes of start/end)");
    return TimeWindow(
        littleEndianValue<int64_t>(str.substr(0, sizeof(int64_t))),
        littleEndianValue<int64_t>(str.substr(sizeof(int64_t))));
  }
};

/// Compile-time binding of a namespace type to its serializer, in the
/// shape of TypeTraits: the primary template is undefined, so a namespace
/// type without a specialization fails to compile where a handle
/// constructs its serializer; a new namespace type plugs in here.
template <typename N>
struct NamespaceSerializerTraits;

template <>
struct NamespaceSerializerTraits<VoidNamespace> {
  static std::shared_ptr<TypeSerializer<VoidNamespace>> create() {
    return std::make_shared<VoidNamespaceSerializer>();
  }
};

template <>
struct NamespaceSerializerTraits<TimeWindow> {
  static std::shared_ptr<TypeSerializer<TimeWindow>> create() {
    return std::make_shared<TimeWindowSerializer>();
  }
};

} // namespace facebook::velox::stateful
