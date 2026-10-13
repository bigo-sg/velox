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
#include <vector>

#include <folly/dynamic.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/serialization/Serializable.h"
#include "velox/type/Type.h"

namespace facebook::velox::stateful {

/// The keyed-state configuration of a stateful operator as declared by the
/// plan: which input channels make up the user key, and the key column
/// types. Pure serializable plan data - no runtime objects, no memory
/// pool, nothing that only exists at execution time.
///
/// The keyed state itself is built from this spec in the operator's
/// initializeStateBackend, not at plan time: the backend additionally
/// needs the key-group range, which lives only in the task's state
/// backend parameters (maxParallelism has that one source), and building
/// there gives the backend ownership of the key table before the first
/// input arrives. The spec is the plan-side half of that handshake.
class KeyedStateSpec : public ISerializable {
 public:
  KeyedStateSpec(
      std::vector<column_index_t> keyChannels,
      std::vector<TypePtr> keyTypes)
      : keyChannels_(std::move(keyChannels)), keyTypes_(std::move(keyTypes)) {
    VELOX_CHECK(!keyChannels_.empty(), "A keyed state needs a key channel");
    VELOX_CHECK_EQ(
        keyChannels_.size(),
        keyTypes_.size(),
        "Every key channel needs a key type");
  }

  const std::vector<column_index_t>& keyChannels() const {
    return keyChannels_;
  }

  const std::vector<TypePtr>& keyTypes() const {
    return keyTypes_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["keyChannels"] = ISerializable::serialize(keyChannels_);
    obj["keyTypes"] = ISerializable::serialize(keyTypes_);
    return obj;
  }

  static std::shared_ptr<const KeyedStateSpec> create(
      const folly::dynamic& obj,
      void* /*context*/) {
    std::vector<column_index_t> keyChannels;
    keyChannels.reserve(obj["keyChannels"].size());
    for (const auto& channel : obj["keyChannels"]) {
      keyChannels.push_back(
          ISerializable::deserialize<column_index_t>(channel));
    }
    std::vector<TypePtr> keyTypes;
    keyTypes.reserve(obj["keyTypes"].size());
    for (const auto& type : obj["keyTypes"]) {
      keyTypes.push_back(Type::create(type));
    }
    return std::make_shared<KeyedStateSpec>(
        std::move(keyChannels), std::move(keyTypes));
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("KeyedStateSpec", create);
  }

 private:
  const std::vector<column_index_t> keyChannels_;
  const std::vector<TypePtr> keyTypes_;
};

} // namespace facebook::velox::stateful
