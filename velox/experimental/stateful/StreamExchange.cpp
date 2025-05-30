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
#include "velox/experimental/stateful/StreamExchange.h"

#include <iostream>

namespace facebook::velox::stateful {

StreamExchange::StreamExchange(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const StreamExchangeNode>& exchangeNode)
    : Operator(
          driverCtx,
          exchangeNode->outputType(),
          operatorId,
          exchangeNode->id(),
          "StreamExchange",
          std::nullopt),
      exchangeNode_(std::move(exchangeNode)) {
}

void StreamExchange::initialize() {
}
  
bool StreamExchange::isFinished() {
  return false;
}

void StreamExchange::traceInput(const RowVectorPtr& input) {
}

void StreamExchange::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

void StreamExchange::close() {
}

RowVectorPtr StreamExchange::getOutput() {
  // TODO: implement it
  return input_;
}

} // namespace facebook::velox::stateful
