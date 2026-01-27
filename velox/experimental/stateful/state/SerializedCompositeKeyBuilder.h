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
#include "velox/experimental/stateful/TypeSerializer.h"

namespace facebook::velox::stateful {

template <typename K, typename N>
class SerializedCompositeKeyBuilder {
 public:
  SerializedCompositeKeyBuilder(
      const std::shared_ptr<TypeSerializer<K>> keySerializer,
      const std::shared_ptr<TypeSerializer<N>> namespaceSerializer,
      int32_t keyGroupPrefixBytes,
      int32_t initialSize)
      : keySerializer_(keySerializer),
        namespaceSerializer_(namespaceSerializer),
        keyGroupPrefixBytes_(keyGroupPrefixBytes),
        initialSize_(initialSize) {}

  const std::string buildCompositeKeyNamespace(K key, N ns) {
    std::string keySlice = keySerializer_->serialize(key);
    std::string namespaceSlice = namespaceSerializer_->serialize(ns);
    std::string compositeString;
    compositeString.reserve(keySlice.size() + namespaceSlice.size());
    compositeString.append(keySlice.data(), keySlice.size());
    compositeString.append(namespaceSlice.data(), namespaceSlice.size());
    return compositeString;
  }

 private:
  const std::shared_ptr<TypeSerializer<K>> keySerializer_;
  const std::shared_ptr<TypeSerializer<N>> namespaceSerializer_;
  const int32_t keyGroupPrefixBytes_;
  const int32_t initialSize_;
};

} // namespace facebook::velox::stateful
