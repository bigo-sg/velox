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

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::fuzzer {

class DiscardDataTableHandle;
using DiscardDataTableHandlePtr = std::shared_ptr<const DiscardDataTableHandle>;

class DiscardDataTableHandle : public ConnectorInsertTableHandle {
 public:
  DiscardDataTableHandle() {}

  virtual ~DiscardDataTableHandle() = default;

  folly::dynamic serialize() const override;

  static DiscardDataTableHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();

  std::string toString() const override {
    return "";
  }
};

class DiscardDataSink : public DataSink {
 public:
  DiscardDataSink() {}

  void appendData(RowVectorPtr input) override;

  bool finish() override {
    return true;
  }

  Stats stats() const override {
    Stats stats;
    return stats;
  }

  std::vector<std::string> close() override {
    std::vector<std::string> ignore;
    return ignore;
  }

  void abort() override {}

 private:
  long lastTime = 0;
  long rowNums = 0;
};

} // namespace facebook::velox::connector::fuzzer
