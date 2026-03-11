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

namespace facebook::velox::stateful {

class KeyedStateBackendParameters;

// This class is relevant to Flink org.apache.flink.runtime.state.StateBackend.
class StateBackend : public ISerializable {
 public:
  StateBackend(
      const std::shared_ptr<const KeyedStateBackendParameters> parameters)
      : parameters_(parameters) {}
  virtual std::string getName() const = 0;

  virtual std::shared_ptr<KeyedStateBackend> createKeyedStateBackend() = 0;

 protected:
  const std::shared_ptr<const KeyedStateBackendParameters> parameters_;
};

enum class StateBackendType { HEAP, ROCKSDB };

class KeyedStateBackendParameters : public ISerializable {
 public:
  KeyedStateBackendParameters(
      const StateBackendType backendType,
      const std::string& jobId,
      const std::string operatorId)
      : backendType_(backendType), jobId_(jobId), operatorId_(operatorId) {}

  const std::string& getJobId() const {
    return jobId_;
  }

  const std::string& getOperatorIdentifier() const {
    return operatorId_;
  }

  StateBackendType getBackendType() const {
    return backendType_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj;
    obj["jobId"] = jobId_;
    obj["operatorId"] = operatorId_;
    obj["stateBackendType"] = static_cast<int32_t>(backendType_);
    return obj;
  }

  static std::shared_ptr<const KeyedStateBackendParameters> create(
      const folly::dynamic& obj,
      void* context) {
    if (!obj.count("stateBackendType")) {
      return nullptr;
    }
    const std::string jobId = obj["jobId"].asString();
    const std::string operatorId = obj["operatorId"].asString();
    const StateBackendType backendType =
        static_cast<StateBackendType>(obj["stateBackendType"].asInt());
    return std::make_shared<const KeyedStateBackendParameters>(
        backendType, jobId, operatorId);
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("KeyedStateBackendParameters", create);
  }

 private:
  const StateBackendType backendType_;
  const std::string jobId_;
  const std::string operatorId_;
};

} // namespace facebook::velox::stateful
