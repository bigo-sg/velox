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
#include "velox/experimental/stateful/window/WindowBuffer.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

#include <climits>

namespace facebook::velox::stateful {

void RecordsWindowBuffer::addElement(uint32_t key, int64_t sliceEnd, RowVectorPtr& element) {
  minSliceEnd_ = std::min(sliceEnd, minSliceEnd_);
  WindowKey windowKey(key, sliceEnd);
  std::lock_guard<std::mutex> lock(mtx);
  auto it = buffer_.find(windowKey);
  if (it != buffer_.end()) {
    // If the key already exists, we can append the element to the existing list.
    it->second.push_back(element);
  } else {
    // If the key does not exist, we create a new list and add the element.
    std::list<RowVectorPtr> newList;
    newList.push_back(element);
    buffer_[windowKey] = std::move(newList);
  }
}

std::unordered_map<WindowKey, std::list<RowVectorPtr>>& RecordsWindowBuffer::advanceProgress(int64_t progress) {
  if (TimeWindowUtil::isWindowFired(minSliceEnd_, progress, shiftTimeZone_)) {
    // there should be some window to be fired, flush buffer to state first
    return buffer_;
  }
  return empty_;
}

} // namespace facebook::velox::stateful
