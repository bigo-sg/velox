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

#include "velox/common/serialization/Serializable.h"
#include "velox/experimental/stateful/state/KeyedStateBackend.h"
#include "velox/experimental/stateful/state/StateBackend.h"

namespace facebook::velox::stateful {

// This class is relevent to flink org.apache.flink.runtime.state.hashmap.HashMapStateBackend.
class HashMapStateBackend : public StateBackend {
 public:
  std::string getName() const {
    return "HashMapStateBackend";
  }

  folly::dynamic serialize() const override {
    return nullptr;
  }

  std::shared_ptr<KeyedStateBackend> createKeyedStateBackend(
      KeyedStateBackendParameters parameters) override;
};

} // namespace facebook::velox::stateful
