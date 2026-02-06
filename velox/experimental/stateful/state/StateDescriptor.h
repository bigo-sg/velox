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
#include "velox/common/memory/MemoryPool.h"

namespace facebook::velox::stateful {

// This class is relevant to Flink org.apache.flink.api.common.StateDescriptor.
class StateDescriptor {
 public:
  StateDescriptor(
      const std::string& name,
      const std::string& operatorId = "",
      memory::MemoryPool* pool = nullptr)
      : name_(name), operatorId_(operatorId), pool_(pool) {}

  const std::string name() const {
    return name_;
  }

  int32_t keyGroupNumber() const {
    return keyGroupNumber_;
  }

  const std::string operatorId() const {
    return operatorId_;
  }

  memory::MemoryPool* memoryPool() {
    return pool_;
  }

 protected:
 private:
  const std::string name_;
  const std::string operatorId_;
  memory::MemoryPool* pool_;
  int32_t keyGroupNumber_ = 1024;
};

} // namespace facebook::velox::stateful
