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
#include "velox/connectors/print/PrintSink.h"
#include "velox/connectors/utils/StringFormatter.h"
#include "velox/type/tz/TimeZoneMap.h"

#include <fmt/format.h>
#include <iostream>
#include <mutex>
#include <sstream>

namespace facebook::velox::connector::print {

// Process-wide mutex serializing stdout/stderr writes across subtasks.
namespace {
std::mutex& printSinkWriteMutex() {
  static std::mutex m;
  return m;
}
} // namespace

PrintSink::PrintSink(
    const RowTypePtr& inputType,
    const std::string& printIdentifier,
    bool isStdErr,
    const ConnectorQueryCtx* queryCtx)
    : inputType_(inputType),
      queryCtx_(queryCtx),
      formatter_(createFormatter(inputType_, tz::locateZone(queryCtx->sessionTimezone()))),
      prefix_([&] {
        const auto* props = queryCtx->sessionProperties();
        int parallelism = 1;
        int taskIndex = 0;
        if (props != nullptr) {
          parallelism = props->get<int>("parallelism", 1);
          taskIndex = props->get<int>("task_index", 0);
        }
        return computePrefix(printIdentifier, parallelism, taskIndex);
      }()),
      isStdErr_(isStdErr) {}

std::string PrintSink::computePrefix(
    const std::string& printIdentifier,
    int parallelism,
    int taskIndex) {
  std::string prefix = printIdentifier;
  if (parallelism > 1) {
    if (!prefix.empty()) {
      prefix += ":";
    }
    prefix += std::to_string(taskIndex + 1);
  }
  if (!prefix.empty()) {
    prefix += "> ";
  }
  return prefix;
}

void PrintSink::appendData(RowVectorPtr input) {
  VELOX_CHECK_NOT_NULL(formatter_);
  const auto& inputFields = inputType_->children();
  VELOX_CHECK_EQ(input->childrenSize(), inputFields.size());

  std::ostream& stream = isStdErr_ ? static_cast<std::ostream&>(std::cerr)
                                   : static_cast<std::ostream&>(std::cout);
  std::lock_guard<std::mutex> lock(printSinkWriteMutex());
  for (auto i = 0; i < input->size(); ++i) {
    std::stringstream ss;
    formatter_->toString(input, inputType_, i, ss);
    stream << prefix_ << ss.str() << std::endl;
  }
}

std::vector<std::string> PrintSink::close() {
  finished = true;
  return {};
}

bool PrintSink::finish() {
  finished = true;
  return true;
}

void PrintSink::abort() {}

connector::DataSink::Stats PrintSink::stats() const {
  return connector::DataSink::Stats{};
}

} // namespace facebook::velox::connector::print
