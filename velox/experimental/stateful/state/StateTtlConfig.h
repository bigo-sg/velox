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

namespace facebook::velox::stateful {

// This class is relevent to flink org.apache.flink.api.common.State.
class StateTtlConfig : public ISerializable {
 public:
  StateTtlConfig(long retentionTime)
      : retentionTime_(retentionTime) {}

  folly::dynamic serialize() const override {
    return nullptr;
  }

 private:
  long retentionTime_;
};

} // namespace facebook::velox::stateful
