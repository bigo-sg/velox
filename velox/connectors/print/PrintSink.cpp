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
#include "velox/vector/SimpleVector.h"

#include <fmt/format.h>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string_view>

namespace facebook::velox::connector::print {

// Process-wide mutex serializing stdout/stderr writes across subtasks.
namespace {
std::mutex& printSinkWriteMutex() {
  static std::mutex m;
  return m;
}

// Name of the synthetic trailing TINYINT column carrying per-row RowKind.
// Mirrors velox/experimental/stateful/RowKind.h::kRowKindColumnName so the
// print connector stays decoupled from the stateful library.
constexpr std::string_view kRowKindColumnName = "$row_kind";

// Flink RowKind byte ordinals (org.apache.flink.types.RowKind.toByteValue()).
// Mirrors velox/experimental/stateful/RowKind.h::RowKind.
enum class RowKind : int8_t {
  INSERT = 0,
  UPDATE_BEFORE = 1,
  UPDATE_AFTER = 2,
  DELETE = 3,
};

// Matches Flink RowKind.shortString(): 0=INSERT -> "+I",
// 1=UPDATE_BEFORE -> "-U", 2=UPDATE_AFTER -> "+U", 3=DELETE -> "-D".
const char* rowKindPrefix(int8_t kind) {
  switch (static_cast<RowKind>(kind)) {
    case RowKind::INSERT:
      return "+I";
    case RowKind::UPDATE_BEFORE:
      return "-U";
    case RowKind::UPDATE_AFTER:
      return "+U";
    case RowKind::DELETE:
      return "-D";
  }
  VELOX_FAIL("Unknown RowKind byte: {}", static_cast<int>(kind));
}
} // namespace

PrintSink::PrintSink(
    const RowTypePtr& inputType,
    const std::string& printIdentifier,
    bool isStdErr,
    const ConnectorQueryCtx* queryCtx)
    : inputType_(inputType),
      dataColumnsType_([&] {
        // Detect trailing $row_kind TINYINT column (the planner augments
        // print-connector TableWriteNodes with this column so per-row RowKind
        // reaches this sink).
        if (inputType->size() > 0 &&
            inputType->nameOf(inputType->size() - 1) ==
                std::string(kRowKindColumnName)) {
          std::vector<std::string> names;
          std::vector<TypePtr> types;
          auto n = inputType->size() - 1;
          names.reserve(n);
          types.reserve(n);
          for (auto i = 0; i < n; ++i) {
            names.emplace_back(inputType->nameOf(i));
            types.emplace_back(inputType->childAt(i));
          }
          return ROW(std::move(names), std::move(types));
        }
        return inputType;
      }()),
      fieldFormatters_([&] {
        std::vector<FormatterPtr> formatters;
        auto tz = tz::locateZone(queryCtx->sessionTimezone());
        formatters.reserve(dataColumnsType_->size());
        for (auto i = 0; i < dataColumnsType_->size(); ++i) {
          formatters.emplace_back(
              createFormatter(dataColumnsType_->childAt(i), tz));
        }
        return formatters;
      }()),
      hasRowKind_(dataColumnsType_->size() != inputType_->size()),
      queryCtx_(queryCtx),
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
  const auto fieldCount = dataColumnsType_->size();
  VELOX_CHECK_EQ(input->childrenSize(), inputType_->size());
  VELOX_CHECK_EQ(fieldFormatters_.size(), fieldCount);

  std::shared_ptr<SimpleVector<int8_t>> rowKindVec;
  if (hasRowKind_) {
    auto rowKindIdx = inputType_->size() - 1;
    rowKindVec = std::dynamic_pointer_cast<SimpleVector<int8_t>>(
        input->childAt(rowKindIdx));
    VELOX_CHECK_NOT_NULL(
        rowKindVec,
        "$row_kind column must be a SimpleVector<int8_t>, got encoding: {}",
        input->childAt(rowKindIdx)->encoding());
  }

  std::ostream& stream = isStdErr_ ? static_cast<std::ostream&>(std::cerr)
                                   : static_cast<std::ostream&>(std::cout);
  std::lock_guard<std::mutex> lock(printSinkWriteMutex());
  for (auto i = 0; i < input->size(); ++i) {
    const char* rowPrefix =
        hasRowKind_ ? rowKindPrefix(rowKindVec->valueAt(i)) : "+I";
    std::stringstream ss;
    ss << rowPrefix << "[";
    for (auto j = 0; j < fieldCount; ++j) {
      const auto& field = input->childAt(j);
      if (field->isNullAt(i)) {
        ss << "null";
      } else {
        fieldFormatters_[j]->toString(
            field, dataColumnsType_->childAt(j), i, ss);
      }
      if (j != fieldCount - 1) {
        ss << ", ";
      }
    }
    ss << "]";
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
