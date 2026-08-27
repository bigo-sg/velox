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
  /// Lower bound of Flink's default max parallelism, used when the field is
  /// absent (e.g. plans written before the field existed).
  static constexpr uint32_t kDefaultMaxParallelism = 128;

  KeyedStateBackendParameters(
      const StateBackendType backendType,
      const std::string& jobId,
      const std::string operatorId,
      uint32_t maxParallelism = kDefaultMaxParallelism,
      uint32_t startKeyGroup = 0,
      uint32_t numKeyGroups = kDefaultMaxParallelism)
      : backendType_(backendType),
        jobId_(jobId),
        operatorId_(operatorId),
        maxParallelism_(maxParallelism),
        startKeyGroup_(startKeyGroup),
        numKeyGroups_(numKeyGroups) {}

  const std::string& getJobId() const {
    return jobId_;
  }

  const std::string& getOperatorIdentifier() const {
    return operatorId_;
  }

  StateBackendType getBackendType() const {
    return backendType_;
  }

  /// Size of the key-group space: keyGroup = hash % maxParallelism. Stable
  /// across rescale, so key-group boundaries do not move.
  uint32_t getMaxParallelism() const {
    return maxParallelism_;
  }

  /// Inclusive start of this backend's key-group sub-range.
  uint32_t getStartKeyGroup() const {
    return startKeyGroup_;
  }

  /// Number of key groups in this backend's sub-range.
  uint32_t getNumKeyGroups() const {
    return numKeyGroups_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["jobId"] = jobId_;
    obj["operatorId"] = operatorId_;
    obj["stateBackendType"] = static_cast<int32_t>(backendType_);
    obj["maxParallelism"] = maxParallelism_;
    obj["startKeyGroup"] = startKeyGroup_;
    obj["numKeyGroups"] = numKeyGroups_;
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
    const uint32_t maxParallelism =
        obj.count("maxParallelism")
            ? static_cast<uint32_t>(obj["maxParallelism"].asInt())
            : kDefaultMaxParallelism;
    const uint32_t startKeyGroup =
        obj.count("startKeyGroup")
            ? static_cast<uint32_t>(obj["startKeyGroup"].asInt())
            : 0;
    const uint32_t numKeyGroups =
        obj.count("numKeyGroups")
            ? static_cast<uint32_t>(obj["numKeyGroups"].asInt())
            : kDefaultMaxParallelism;
    return std::make_shared<const KeyedStateBackendParameters>(
        backendType, jobId, operatorId, maxParallelism, startKeyGroup, numKeyGroups);
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("KeyedStateBackendParameters", create);
  }

 private:
  const StateBackendType backendType_;
  const std::string jobId_;
  const std::string operatorId_;
  const uint32_t maxParallelism_;
  const uint32_t startKeyGroup_;
  const uint32_t numKeyGroups_;
};

} // namespace facebook::velox::stateful
