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

#include "velox/connectors/utils/StringFormatter.h"
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <stack>

namespace facebook::velox::connector {
const FormatterPtr createFormatter(
    const TypePtr& type,
    const tz::TimeZone* timeZone) {
  TypeKind typeKind = type->kind();
  switch (typeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<DefaultFormatter<int32_t>>(timeZone);
    case TypeKind::BIGINT:
      return std::make_shared<DefaultFormatter<int64_t>>(timeZone);
    case TypeKind::HUGEINT:
      return std::make_shared<DefaultFormatter<int128_t>>(timeZone);
    case TypeKind::SMALLINT:
      return std::make_shared<DefaultFormatter<int16_t>>(timeZone);
    case TypeKind::TINYINT:
      return std::make_shared<DefaultFormatter<int8_t>>(timeZone);
    case TypeKind::DOUBLE:
      return std::make_shared<DefaultFormatter<double>>(timeZone);
    case TypeKind::REAL:
      return std::make_shared<DefaultFormatter<float>>(timeZone);
    case TypeKind::BOOLEAN:
      return std::make_shared<DefaultFormatter<bool>>(timeZone);
    case TypeKind::VARCHAR:
      return std::make_shared<DefaultFormatter<StringView>>(timeZone);
    case TypeKind::TIMESTAMP:
      return std::make_shared<DefaultFormatter<Timestamp>>(timeZone);
    case TypeKind::ROW: {
      const RowTypePtr rowType = std::dynamic_pointer_cast<const RowType>(type);
      std::vector<FormatterPtr> formatters;
      for (size_t i = 0; i < rowType->children().size(); ++i) {
        const auto formatter = createFormatter(rowType->childAt(i), timeZone);
        formatters.emplace_back(formatter);
      }
      return std::make_shared<RowFormatter>(formatters);
    }
    case TypeKind::ARRAY: {
      const std::shared_ptr<const ArrayType> arrayType =
          std::dynamic_pointer_cast<const ArrayType>(type);
      const TypePtr& elementType = arrayType->elementType();
      auto elemmentFormatter = createFormatter(elementType, timeZone);
      return std::make_shared<ArrayFormatter>(elemmentFormatter);
    }
    case TypeKind::MAP: {
      const std::shared_ptr<const MapType> mapType =
          std::dynamic_pointer_cast<const MapType>(type);
      const TypePtr& keyType = mapType->keyType();
      const TypePtr& valueType = mapType->valueType();
      auto keyFormatter = createFormatter(keyType, timeZone);
      auto valueFormatter = createFormatter(valueType, timeZone);
      return std::make_shared<MapFormatter>(keyFormatter, valueFormatter);
    }
    default:
      VELOX_FAIL("Unsupported type: {}", type->name());
  }
}

const std::vector<std::string> StringFormatter::split(
    const std::string& input,
    const std::string& delimiter) {
  std::vector<std::string> tokens;
  if (input.empty() || delimiter.empty()) {
    tokens.emplace_back(input);
    return tokens;
  }
  size_t delimLen = delimiter.size();
  size_t i = 0, start = 0, end = 0;
  while (i < input.size()) {
    const std::string subs = input.substr(i, delimLen);
    if (subs == delimiter) {
      int len = end - start;
      const std::string token = input.substr(start, len);
      tokens.emplace_back(token);
      i += delimLen;
      start = i;
    } else if (
        input.substr(i, 3) == "+I[" || input[i] == '[' || input[i] == '{') {
      char ch = input[i] == '{' ? '}' : ']';
      std::stack<std::string> stack;
      size_t j = i;
      while (j < input.size()) {
        if (input[j] == ']' || input[j] == '}') {
          auto sMatched = [&](char c) -> bool {
            bool matched = false;
            std::string s = stack.top();
            if (c == ']') {
              matched = s == "[" || s == "+I[";
            } else if (c == '}') {
              matched = s == "{";
            }
            if (matched) {
              stack.pop();
            }
            return matched;
          };
          VELOX_CHECK_EQ(
              sMatched(input[j]),
              true,
              "Character {} not matched with {}, input {} is illegal string",
              stack.top(),
              ch,
              input);
          if (input[j] == ch && stack.empty()) {
            start = i;
            i = j;
            break;
          } else {
            j++;
          }
        } else if (input.substr(j, 3) == "+I[") {
          stack.push(input.substr(j, 3));
          j += 3;
        } else if (input[j] == '{' || input[j] == '[') {
          stack.push(input.substr(j, 1));
          j++;
        } else {
          j++;
        }
      }
    } else {
      i++;
      end = i;
    }
  }
  if (end > start) {
    const std::string token = input.substr(start, end - start);
    tokens.emplace_back(token);
  }
  return tokens;
}

const void StringFormatter::normalize(std::string& input, const TypeKind kind) {
  switch (kind) {
    case TypeKind::ROW: {
      boost::replace_all(input, ", ", ",");
      boost::erase_first(input, "+I[");
      boost::erase_last(input, "]");
      return;
    }
    case TypeKind::ARRAY: {
      boost::replace_all(input, ", ", ",");
      boost::erase_first(input, "[");
      boost::erase_last(input, "]");
      return;
    }
    case TypeKind::MAP: {
      boost::replace_all(input, ", ", ",");
      boost::erase_first(input, "{");
      boost::erase_last(input, "}");
      return;
    }
    default:
      return;
  }
}
} // namespace facebook::velox::connector
