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

namespace facebook::velox::stateful {

// This class is relevent to flink org.apache.flink.api.common.StateDescriptor.
class StateDescriptor {
 public:
  StateDescriptor(const std::string& name) : name_(name) {}

  const std::string name() const {
    return name_;
  }

  int keyGroupNumber() const {
    return keyGroupNumber_;
  }

 protected:

 private:
  const std::string name_;
  int keyGroupNumber_ = 1024;
};

} // namespace facebook::velox::stateful
