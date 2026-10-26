/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/common/base/Exceptions.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"

namespace facebook::velox::functions::flinksql {

/// Handler encodings for ON EMPTY / ON ERROR (aligned with Flink's
/// JsonValueOnEmptyOrError): 0=NULL, 1=ERROR, 2=DEFAULT.
inline constexpr int32_t kJsonValueNull = 0;
inline constexpr int32_t kJsonValueError = 1;
inline constexpr int32_t kJsonValueDefault = 2;

/// Flink-flavored JSON_VALUE:
///   JSON_VALUE(json, path [RETURNING T] [h ON EMPTY] [h ON ERROR])
///
/// Returns the scalar value referenced by the SQL/JSON path as a VARCHAR.
///
/// Measured against native Flink 1.19.2:
///  - If the path carries a "lax " prefix, then any case that does not yield a
///    scalar (missing field, null value, object/array match, invalid JSON)
///    is routed to the ON EMPTY handler.
///  - If the path carries a "strict " prefix (or no prefix, Flink defaults to
///    strict), then any such case is routed to the ON ERROR handler.
///  - A successful scalar extraction always returns the value.
template <typename T>
struct JsonValueFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& json,
      const arg_type<Varchar>& jsonPath,
      const arg_type<int32_t>& onEmpty,
      const arg_type<Varchar>& onEmptyDefault,
      const arg_type<int32_t>& onError,
      const arg_type<Varchar>& onErrorDefault) {
    if (extractScalar(result, json, jsonPath)) {
      return true;
    }
    // No scalar produced. Pick the handler by the lax/strict mode.
    return applyHandler(
        result,
        lax_ ? onEmpty : onError,
        lax_ ? onEmptyDefault : onErrorDefault);
  }

 private:
  // Returns true if a scalar was extracted into result; also resolves and
  // strips the lax/strict prefix on jsonPath into lax_.
  FOLLY_ALWAYS_INLINE bool extractScalar(
      out_type<Varchar>& result,
      const arg_type<Varchar>& json,
      const arg_type<Varchar>& jsonPath) {
    // SQL/JSON path mode prefix.
    std::string_view pathView = std::string_view(jsonPath.data(), jsonPath.size());
    lax_ = false;
    if (pathView.size() >= 4 && pathView.substr(0, 4) == "lax ") {
      lax_ = true;
      pathView.remove_prefix(4);
    } else if (
        pathView.size() >= 7 && pathView.substr(0, 7) == "strict ") {
      lax_ = false;
      pathView.remove_prefix(7);
    }

    bool resultPopulated = false;
    std::optional<std::string> resultStr;

    auto consumer = [&resultStr, &resultPopulated](auto& v) {
      if (resultPopulated) {
        // We should only get a single value; multiple values is an error.
        resultStr = std::nullopt;
        return simdjson::SUCCESS;
      }
      resultPopulated = true;
      SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
      switch (vtype) {
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(bool vbool, v.get_bool());
          resultStr = vbool ? "true" : "false";
          break;
        }
        case simdjson::ondemand::json_type::string: {
          SIMDJSON_ASSIGN_OR_RAISE(resultStr, v.get_string());
          break;
        }
        // object/array/null produce no scalar.
        case simdjson::ondemand::json_type::object:
        case simdjson::ondemand::json_type::array:
        case simdjson::ondemand::json_type::null:
          break;
        default: {
          SIMDJSON_ASSIGN_OR_RAISE(resultStr, simdjson::to_json_string(v));
        }
      }
      return simdjson::SUCCESS;
    };

    // Reject invalid JSON up front.
    simdjson::padded_string paddedJson(json.data(), json.size());
    {
      simdjson::ondemand::document parsedDoc;
      if (simdjsonParse(paddedJson).get(parsedDoc)) {
        return false;
      }
    }

    auto& extractor = SIMDJsonExtractor::getInstance(pathView);
    bool isDefinitePath = true;
    if (extractor.extract(paddedJson, consumer, isDefinitePath)) {
      return false;
    }

    if (resultStr.has_value()) {
      result.copy_from(*resultStr);
      return true;
    }
    return false;
  }

  FOLLY_ALWAYS_INLINE bool applyHandler(
      out_type<Varchar>& result,
      const arg_type<int32_t>& handler,
      const arg_type<Varchar>& defaultVal) {
    switch (handler) {
      case kJsonValueNull:
        return false;
      case kJsonValueError:
        VELOX_USER_FAIL(
            "JSON_VALUE: path does not resolve to a scalar value or JSON is "
            "invalid.");
      case kJsonValueDefault:
      default:
        result = defaultVal;
        return true;
    }
  }

  bool lax_{false};
};
} // namespace facebook::velox::functions::flinksql
