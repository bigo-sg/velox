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

#include <cctype>
#include <charconv>
#include <cmath>
#include <string>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/StringWriter.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"

namespace facebook::velox::functions::flinksql {

/// Handler encodings for ON EMPTY / ON ERROR (aligned with Flink's
/// JsonValueOnEmptyOrError): 0=NULL, 1=ERROR, 2=DEFAULT.
inline constexpr int32_t kJsonValueNull = 0;
inline constexpr int32_t kJsonValueError = 1;
inline constexpr int32_t kJsonValueDefault = 2;

namespace {
// Renders a double the way Flink does via Java's String.valueOf(double)
// (Double.toString): shortest round-trip digits, a trailing ".0" for integral
// values, and scientific notation only when |v| < 1e-3 or |v| >= 1e7, using an
// uppercase 'E' and no leading zeros or plus sign in the exponent.
inline std::string javaDoubleToString(double v) {
  if (std::isnan(v)) {
    return "NaN";
  }
  if (std::isinf(v)) {
    return v > 0 ? "Infinity" : "-Infinity";
  }
  if (v == 0.0) {
    return std::signbit(v) ? "-0.0" : "0.0";
  }

  const bool scientific = (std::abs(v) >= 1e7) || (std::abs(v) < 1e-3);

  char buf[64];
  const std::to_chars_result toChars = scientific
      ? std::to_chars(buf, buf + sizeof(buf), v)
      : std::to_chars(buf, buf + sizeof(buf), v, std::chars_format::fixed);
  VELOX_CHECK_EQ(
      toChars.ec, std::errc{}, "std::to_chars failed for double {}", v);
  std::string s(buf, toChars.ptr - buf);

  if (!scientific) {
    if (s.find('.') == std::string::npos) {
      s += ".0";
    }
    return s;
  }

  const auto epos = s.find('e');
  VELOX_CHECK_NE(epos, std::string::npos, "expected exponent in {}", s);
  std::string mantissa = s.substr(0, epos);
  std::string exponent = s.substr(epos + 1);
  if (mantissa.find('.') == std::string::npos) {
    mantissa += ".0";
  }
  const bool neg = !exponent.empty() && exponent[0] == '-';
  if (!exponent.empty() && (exponent[0] == '+' || exponent[0] == '-')) {
    exponent.erase(0, 1);
  }
  while (exponent.size() > 1 && exponent[0] == '0') {
    exponent.erase(0, 1);
  }
  return mantissa + "E" + (neg ? "-" : "") + exponent;
}

enum class JsonScalarKind { kBoolean, kString, kInteger, kFloat, kNone };

struct JsonScalar {
  JsonScalarKind kind{JsonScalarKind::kNone};
  // Normalized scalar text: "true"/"false" for booleans, the raw string for
  // strings, decimal digits for integers and the java-rendered double for
  // floats. Empty when kind is kNone.
  std::string value;
};

enum class JsonExtractResult { kScalar, kEmpty, kMalformed };

// Strips the SQL/JSON path mode prefix per Flink's rule
// "^\s*(strict|lax)\s+(.+)$" (case-insensitive, arbitrary whitespace). When no
// mode prefix is present the path stays untouched and lax is left false (strict
// is the default).
inline void stripJsonPathMode(std::string_view& pathView, bool& lax) {
  lax = false;
  size_t pos = 0;
  auto isWs = [](char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' ||
        c == '\v';
  };
  while (pos < pathView.size() && isWs(pathView[pos])) {
    ++pos;
  }

  auto ieq = [&](const char* word) {
    size_t i = 0;
    for (; word[i] != '\0' && pos + i < pathView.size(); ++i) {
      if (std::tolower(static_cast<unsigned char>(pathView[pos + i])) !=
          word[i]) {
        return false;
      }
    }
    return word[i] == '\0';
  };

  size_t wordLen = 0;
  bool modeLax = false;
  if (ieq("lax")) {
    wordLen = 3;
    modeLax = true;
  } else if (ieq("strict")) {
    wordLen = 6;
    modeLax = false;
  } else {
    return; // no mode prefix -> strict, path untouched.
  }

  // The mode word must be followed by at least one whitespace.
  if (pos + wordLen >= pathView.size() || !isWs(pathView[pos + wordLen])) {
    lax = false;
    return;
  }
  pos += wordLen;
  while (pos < pathView.size() && isWs(pathView[pos])) {
    ++pos;
  }
  pathView.remove_prefix(pos);
  lax = modeLax;
}

// Extracts the scalar referenced by jsonPath. Returns kScalar (scalar filled),
// kEmpty for structural non-scalar cases (missing field, null/object/array
// match, multiple results), or kMalformed when the input JSON fails to parse.
// The lax/strict mode prefix is stripped into lax.
inline JsonExtractResult extractJsonScalar(
    JsonScalar& scalar,
    const StringView& json,
    const StringView& jsonPath,
    bool& lax) {
  std::string_view pathView =
      std::string_view(jsonPath.data(), jsonPath.size());
  stripJsonPathMode(pathView, lax);

  bool resultPopulated = false;
  bool multiple = false;

  auto consumer = [&scalar, &resultPopulated, &multiple](auto& v) {
    if (resultPopulated) {
      multiple = true;
      return simdjson::SUCCESS;
    }
    resultPopulated = true;
    SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
    switch (vtype) {
      case simdjson::ondemand::json_type::boolean: {
        SIMDJSON_ASSIGN_OR_RAISE(bool vbool, v.get_bool());
        scalar.kind = JsonScalarKind::kBoolean;
        scalar.value = vbool ? "true" : "false";
        break;
      }
      case simdjson::ondemand::json_type::string: {
        SIMDJSON_ASSIGN_OR_RAISE(scalar.value, v.get_string());
        scalar.kind = JsonScalarKind::kString;
        break;
      }
      // object/array/null produce no scalar.
      case simdjson::ondemand::json_type::object:
      case simdjson::ondemand::json_type::array:
      case simdjson::ondemand::json_type::null:
        scalar.kind = JsonScalarKind::kNone;
        break;
      default: {
        std::string_view raw = v.raw_json_token();
        const bool isFloat = raw.find('.') != std::string_view::npos ||
            raw.find('e') != std::string_view::npos ||
            raw.find('E') != std::string_view::npos;
        if (isFloat) {
          double vdouble = 0.0;
          SIMDJSON_ASSIGN_OR_RAISE(vdouble, v.get_double());
          scalar.kind = JsonScalarKind::kFloat;
          scalar.value = javaDoubleToString(vdouble);
        } else {
          int64_t vint = 0;
          if (v.get_int64().get(vint) == simdjson::SUCCESS) {
            scalar.kind = JsonScalarKind::kInteger;
            scalar.value = std::to_string(vint);
          } else {
            uint64_t vuint = 0;
            if (v.get_uint64().get(vuint) == simdjson::SUCCESS) {
              scalar.kind = JsonScalarKind::kInteger;
              scalar.value = std::to_string(vuint);
            } else {
              // Integer beyond uint64 (BigInteger semantics): keep raw.
              scalar.kind = JsonScalarKind::kInteger;
              scalar.value = std::string(raw);
            }
          }
        }
        break;
      }
    }
    return simdjson::SUCCESS;
  };

  simdjson::padded_string paddedJson(json.data(), json.size());
  // Strictly validate the input: the on-demand parser is lazy and treats some
  // malformed inputs (e.g. "not-json") as a scalar, so use the DOM parser to
  // reject them up front as a malformed-JSON error. BIGINT_ERROR is allowed:
  // an integer beyond 64 bits is valid JSON and is kept as raw digits.
  {
    static thread_local simdjson::dom::parser domParser;
    simdjson::dom::element elem;
    const auto parseError = domParser.parse(paddedJson).get(elem);
    if (parseError != simdjson::SUCCESS &&
        parseError != simdjson::BIGINT_ERROR) {
      return JsonExtractResult::kMalformed;
    }
  }

  auto& extractor = SIMDJsonExtractor::getInstance(pathView);
  bool isDefinitePath = true;
  // A non-SUCCESS extract means the path could not be walked, which is a
  // malformed-input error, not an empty result.
  if (extractor.extract(paddedJson, consumer, isDefinitePath)) {
    return JsonExtractResult::kMalformed;
  }

  if (multiple || scalar.kind == JsonScalarKind::kNone) {
    return JsonExtractResult::kEmpty;
  }
  return JsonExtractResult::kScalar;
}

// Casts a normalized scalar into the writer for the requested RETURNING type.
// Returns false when the scalar kind does not agree with the target type (a
// conversion error). The caller fails the query in this case, matching Flink.
template <typename TResultWriter>
inline bool castJsonScalar(TResultWriter& result, const JsonScalar& scalar) {
  if constexpr (std::is_same_v<TResultWriter, exec::StringWriter>) {
    result.copy_from(scalar.value);
    return true;
  } else if constexpr (std::is_same_v<TResultWriter, int32_t>) {
    if (scalar.kind != JsonScalarKind::kInteger) {
      return false;
    }
    int32_t v = 0;
    auto [ptr, ec] = std::from_chars(
        scalar.value.data(), scalar.value.data() + scalar.value.size(), v);
    if (ec != std::errc{} || ptr != scalar.value.data() + scalar.value.size()) {
      return false;
    }
    result = v;
    return true;
  } else if constexpr (std::is_same_v<TResultWriter, bool>) {
    if (scalar.kind != JsonScalarKind::kBoolean) {
      return false;
    }
    result = scalar.value == "true";
    return true;
  } else if constexpr (std::is_same_v<TResultWriter, double>) {
    if (scalar.kind != JsonScalarKind::kFloat) {
      return false;
    }
    double v = 0.0;
    auto [ptr, ec] = std::from_chars(
        scalar.value.data(), scalar.value.data() + scalar.value.size(), v);
    if (ec != std::errc{} || ptr != scalar.value.data() + scalar.value.size()) {
      return false;
    }
    result = v;
    return true;
  } else {
    VELOX_UNREACHABLE();
  }
}

template <typename TResultWriter, typename TDefaultArg>
inline bool applyJsonHandler(
    TResultWriter& result,
    const int32_t& handler,
    const TDefaultArg* defaultVal) {
  switch (handler) {
    case kJsonValueNull:
      return false;
    case kJsonValueError:
      VELOX_USER_FAIL(
          "JSON_VALUE: path does not resolve to a scalar value or JSON is "
          "invalid.");
    case kJsonValueDefault:
      if (defaultVal == nullptr) {
        // DEFAULT NULL -> SQL NULL.
        return false;
      }
      result = *defaultVal;
      return true;
    default:
      VELOX_USER_FAIL(
          "JSON_VALUE: unknown handler encoding {}", (int32_t)handler);
  }
}
} // namespace

/// Flink-flavored JSON_VALUE:
///   JSON_VALUE(json, path [RETURNING T] [h ON EMPTY] [h ON ERROR])
///
/// One struct per supported RETURNING type is generated below (VARCHAR, INT,
/// BOOLEAN, DOUBLE); in each the default values carry that same type.
#define FLINK_JSON_VALUE_FUNCTION(STRUCT_NAME, RESULT_TYPE)               \
  template <typename TExec>                                               \
  struct STRUCT_NAME {                                                    \
    VELOX_DEFINE_FUNCTION_TYPES(TExec);                                   \
                                                                          \
    FOLLY_ALWAYS_INLINE bool callNullable(                                \
        out_type<RESULT_TYPE>& result,                                    \
        const arg_type<Varchar>* json,                                    \
        const arg_type<Varchar>* jsonPath,                                \
        const arg_type<int32_t>* onEmpty,                                 \
        const arg_type<RESULT_TYPE>* onEmptyDefault,                      \
        const arg_type<int32_t>* onError,                                 \
        const arg_type<RESULT_TYPE>* onErrorDefault) {                    \
      if (json == nullptr || jsonPath == nullptr) {                       \
        return false;                                                     \
      }                                                                   \
      JsonScalar scalar;                                                  \
      const JsonExtractResult extractResult =                             \
          extractJsonScalar(scalar, *json, *jsonPath, lax_);              \
      if (extractResult == JsonExtractResult::kScalar) {                  \
        if (castJsonScalar(result, scalar)) {                             \
          return true;                                                    \
        }                                                                 \
        /* A RETURNING-type mismatch fails the query in Flink (an         \
         * uncaught ClassCastException outside the extraction try/catch), \
         * regardless of ON ERROR, so fail the query here too. */         \
        VELOX_USER_FAIL(                                                  \
            "JSON_VALUE: value does not match the RETURNING type.");      \
      }                                                                   \
      VELOX_USER_CHECK(onEmpty != nullptr && onError != nullptr);         \
      /* Both non-scalar paths (kEmpty) and malformed JSON (kMalformed)   \
       * follow Flink's lax/strict routing: lax -> ON EMPTY, strict (or   \
       * default) -> ON ERROR. */                                         \
      return applyJsonHandler(                                            \
          result,                                                         \
          lax_ ? *onEmpty : *onError,                                     \
          lax_ ? onEmptyDefault : onErrorDefault);                        \
    }                                                                     \
                                                                          \
   private:                                                               \
    bool lax_{false};                                                     \
  };

FLINK_JSON_VALUE_FUNCTION(JsonValueVarcharFunction, Varchar)
FLINK_JSON_VALUE_FUNCTION(JsonValueIntegerFunction, int32_t)
FLINK_JSON_VALUE_FUNCTION(JsonValueBooleanFunction, bool)
FLINK_JSON_VALUE_FUNCTION(JsonValueDoubleFunction, double)

#undef FLINK_JSON_VALUE_FUNCTION

} // namespace facebook::velox::functions::flinksql
