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

#include "velox/vector/ComplexVector.h"
#include <climits>
#include <map>

namespace facebook::velox::stateful {

// This class is relevent to flink WindowBuffer.
class WindowKey {
 public:
  WindowKey(uint32_t key, int64_t window)
      : key_(key), window_(window) {}

  int64_t window() const {
    return window_;
  }

  const uint32_t key() const {
    return key_;
  }

  bool operator==(const WindowKey& other) const {
    return key_ == other.key() && window_ == other.window();
  }

 private:
  uint32_t key_;
  int64_t window_;
};

} // namespace facebook::velox::stateful

namespace std {
template<>
struct hash<facebook::velox::stateful::WindowKey> {
  size_t operator()(const facebook::velox::stateful::WindowKey& key) const {
    // TODO: RowVector should have a hash function.
    return std::hash<int64_t>()(key.window()) ^ std::hash<uint32_t>()(key.key());
  }
};
}
