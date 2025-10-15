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

#include "velox/experimental/stateful/window/WindowKey.h"
#include "velox/vector/ComplexVector.h"
#include <climits>

namespace facebook::velox::stateful {

// This class is relevent to flink WindowBuffer.
class WindowBuffer {
 public:
  // TODO: we use hash key of RowVector as key, but flink use RowVector as key.
  // This is not equal to flink, should check it.
  virtual void addElement(uint32_t key, long window, RowVectorPtr& element) = 0;

  virtual std::unordered_map<WindowKey, std::list<RowVectorPtr>>& advanceProgress(long progress) = 0;

  virtual void clear() = 0;
};

using WindowBufferPtr = std::shared_ptr<WindowBuffer>;

// This class is relevent to flink RecordsWindowBuffer.
class RecordsWindowBuffer : public WindowBuffer {
 public:
  void addElement(uint32_t key, long sliceEnd, RowVectorPtr& element) override;

  std::unordered_map<WindowKey, std::list<RowVectorPtr>>& advanceProgress(long progress) override;

  void clear() override {
    buffer_.clear();
    minSliceEnd_ = LONG_MAX;
  }

 private:
  // TODO: use map to simplify.
  std::unordered_map<WindowKey, std::list<RowVectorPtr>> buffer_;
  // This is used to return empty map when no window is fired.
  std::unordered_map<WindowKey, std::list<RowVectorPtr>> empty_;

  long minSliceEnd_ = LONG_MAX;
  int shiftTimeZone_ = 0; // TODO: support time zone shift
};

} // namespace facebook::velox::stateful
