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

#include "velox/connectors/fuzzer/DiscardDataSink.h"

#include <iostream>

namespace facebook::velox::connector::fuzzer {
namespace {

long getCurrentTime() {
  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}
} // namespace

void DiscardDataSink::appendData(RowVectorPtr input) {
  if (lastTime == 0) {
    lastTime = getCurrentTime();
  }
  LOG(INFO) << "input:" << input->toString(0);
  auto preNum = rowNums;
  rowNums += input->size();
  if (rowNums / 100000 != preNum / 100000) {
    long now = getCurrentTime();
    std::cout << "Discard  " << rowNums << " using " << (now - lastTime)  << std::endl;
    lastTime = now;
  }
}

folly::dynamic DiscardDataTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "DiscardTableHandle";
  return obj;
}

DiscardDataTableHandlePtr DiscardDataTableHandle::create(
    const folly::dynamic& obj) {
  return std::make_shared<DiscardDataTableHandle>();
}

void DiscardDataTableHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("DiscardDataTableHandle", DiscardDataTableHandle::create);
}

} // namespace facebook::velox::connector::fuzzer
