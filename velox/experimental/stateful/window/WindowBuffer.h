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
#include <algorithm>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include "velox/experimental/stateful/window/WindowKey.h"
#include "velox/vector/ComplexVector.h"
#include <climits>

namespace facebook::velox::stateful {

/// This class is relevant to Flink WindowBuffer.
class WindowBuffer {
 public:
  // TODO: we use hash key of RowVector as key, but Flink uses RowVector as key.
  // This is not equal to Flink, should check it.
  virtual void
  addElement(uint32_t key, int64_t window, RowVectorPtr& element) = 0;

  virtual std::map<WindowKey, std::list<RowVectorPtr>>&
  advanceProgress(int64_t progress) = 0;

  virtual void clear() = 0;

  virtual void clear(int64_t window) = 0;

  virtual int size() = 0;
};

using WindowBufferPtr = std::shared_ptr<WindowBuffer>;

/// This class is relevant to Flink RecordsWindowBuffer.
class RecordsWindowBuffer : public WindowBuffer {
 public:
  void addElement(uint32_t key, int64_t sliceEnd, RowVectorPtr& element)
      override;

  std::map<WindowKey, std::list<RowVectorPtr>>& advanceProgress(
      int64_t progress) override;

  void clear() override {
    buffer_.clear();
    minSliceEnd_ = INT64_MAX;
  }

  void clear(int64_t window) override {
    // Do not erase during a range-for over the same map; erasure
    // invalidates iterators and causes undefined behavior / crashes.
    for (auto it = buffer_.begin(); it != buffer_.end();) {
      if (it->first.window() <= window) {
        it = buffer_.erase(it);
      } else {
        break;
      }
    }
    if (buffer_.empty()) {
      minSliceEnd_ = INT64_MAX;
    } else {
      minSliceEnd_ = INT64_MAX;
      for (const auto& entry : buffer_) {
        minSliceEnd_ = std::min(minSliceEnd_, entry.first.window());
      }
    }
  }

  int size() override {
    return buffer_.size();
  }

  private:
  /// Ordered by WindowKey (window end, then partition key) for stable iteration.
  std::map<WindowKey, std::list<RowVectorPtr>> buffer_;
  // This is used to return empty map when no window is fired.
  std::map<WindowKey, std::list<RowVectorPtr>> empty_;
  int64_t minSliceEnd_ = INT64_MAX;
  int shiftTimeZone_ = 0; // TODO: support time zone shift
};

} // namespace facebook::velox::stateful
