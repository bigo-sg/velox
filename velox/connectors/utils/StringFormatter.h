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

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <folly/Try.h>
#include <charconv>
#include <cmath>
#include <ctime>
#include <memory>
#include <string>
#include <type_traits>
#include "velox/expression/CastExpr.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"

namespace facebook::velox::connector {

/// Renders a floating-point value the way Java's Double.toString /
/// Float.toString do, so the print connector matches native Flink output in
/// dual-run comparisons: the shortest decimal that round-trips (via
/// std::to_chars), plain notation within [1e-3, 1e7), and Java-style
/// scientific notation ("d.dddEx", one mandatory fraction digit, no '+'
/// sign or leading zeros in the exponent) outside that range. The default
/// `ostream << double` only keeps 6 significant digits, which silently
/// corrupts values like 647568.82 -> 647569.
template <typename T>
inline std::string javaFloatingPointToString(T t) {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
  if (std::isnan(t)) {
    return "NaN";
  }
  if (std::isinf(t)) {
    return t > 0 ? "Infinity" : "-Infinity";
  }
  const double abs = std::fabs(static_cast<double>(t));
  char buf[64];
  if (abs == 0.0 || (abs >= 1e-3 && abs < 1e7)) {
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), t);
    VELOX_CHECK(ec == std::errc());
    std::string s(buf, ptr - buf);
    if (s.find('.') == std::string::npos) {
      s += ".0";
    }
    return s;
  }
  auto [ptr, ec] =
      std::to_chars(buf, buf + sizeof(buf), t, std::chars_format::scientific);
  VELOX_CHECK(ec == std::errc());
  const std::string s(buf, ptr - buf);
  const auto epos = s.find('e');
  std::string mantissa = s.substr(0, epos);
  if (mantissa.find('.') == std::string::npos) {
    mantissa += ".0";
  }
  const int exp = std::stoi(s.substr(epos + 1));
  return mantissa + "E" + std::to_string(exp);
}

struct StringFormatter {
 public:
  StringFormatter(const tz::TimeZone* timeZone) : timeZone_(timeZone) {
    VELOX_CHECK(timeZone_ != nullptr);
  }
  StringFormatter() : timeZone_(tz::locateZone("UTC")) {}

  /// Renders the value at `index` into `ss`. `rowKindPrefix` is the change-kind
  /// prefix (+I/-U/+U/-D) rendered by RowFormatter before the opening bracket;
  /// the other formatters ignore it. Nested rows always render the "+I"
  /// default: Velox rows do not carry a nested change kind, matching Flink
  /// where nested RowData defaults to INSERT.
  virtual const void toString(
      const VectorPtr& input,
      const TypePtr&,
      const size_t index,
      std::stringstream& ss,
      const char* rowKindPrefix = "+I") = 0;

  virtual const void fromString(
      const std::string& s,
      const TypePtr& type,
      const size_t index,
      VectorPtr& vec) = 0;

  /// Split the input string of flink-style by given delimiter, the sub-string
  /// represents complex field would be taken as a whole and not be splitted.
  /// e.g. split('a,+I[1,2,3]', ',') -> [a,+I[1,2,3]].
  static const std::vector<std::string> split(
      const std::string& s,
      const std::string& delimiter);
  /// Normalize the input string of flink-style to make it easy to be splitted.
  /// e.g. +I[1,2,3] -> 1,2,3.
  static const void normalize(std::string& s, const TypeKind kind);

 protected:
  const tz::TimeZone* timeZone_;
};

using FormatterPtr = std::shared_ptr<StringFormatter>;

template <typename T>
struct DefaultFormatter : public StringFormatter {
 public:
  DefaultFormatter(const tz::TimeZone* timeZone) : StringFormatter(timeZone) {}
  DefaultFormatter() : StringFormatter() {}

  const void toString(
      const VectorPtr& input,
      const TypePtr& type,
      const size_t index,
      std::stringstream& ss,
      const char* /*rowKindPrefix*/ = "+I") override {
    if (input->isNullAt(index)) {
      ss << "null";
    } else {
      T val;
      switch (input->encoding()) {
        case VectorEncoding::Simple::FLAT: {
          auto flat = std::dynamic_pointer_cast<FlatVector<T>>(input);
          val = flat->valueAt(index);
          break;
        }
        case VectorEncoding::Simple::DICTIONARY: {
          auto dict = std::dynamic_pointer_cast<DictionaryVector<T>>(input);
          val = dict->valueAt(index);
          break;
        }
        case VectorEncoding::Simple::CONSTANT: {
          auto constant = std::dynamic_pointer_cast<ConstantVector<T>>(input);
          val = constant->valueAt(index);
          break;
        }
        default:
          VELOX_FAIL("Encoding {} currently not supported", input->encoding());
      }
      if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>) {
        if (type->isDecimal()) {
          ss << DecimalUtil::toString(val, type);
        } else if (std::is_same_v<T, int64_t>) {
          ss << static_cast<int64_t>(val);
        }
      } else {
        toString(val, ss);
      }
    }
  }

  const void fromString(
      const std::string& s,
      const TypePtr& type,
      const size_t index,
      VectorPtr& vec) override {
    std::optional<std::string> errMsg;
    if (s == "null") {
      vec->setNull(index, true);
    } else {
      if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>) {
        if (type->isDecimal()) {
          int precision = 0;
          int scale = 0;
          if (type->isShortDecimal()) {
            std::shared_ptr<const ShortDecimalType> decimalType =
                std::dynamic_pointer_cast<const ShortDecimalType>(type);
            precision = decimalType->precision();
            scale = decimalType->scale();
          } else {
            std::shared_ptr<const LongDecimalType> decimalType =
                std::dynamic_pointer_cast<const LongDecimalType>(type);
            precision = decimalType->precision();
            scale = decimalType->scale();
          }
          T t;
          auto status = exec::detail::toDecimalValue(
              StringView(s.data(), s.size()), precision, scale, t);
          VELOX_CHECK(
              status.ok(),
              "Failed to format {} to decimal, with scale {}, precision {}",
              s,
              scale,
              precision);
          vec->asFlatVector<T>()->set(index, t);
          return;
        }
      }
      vec->asFlatVector<T>()->set(index, fromString(s));
    }
  }

 protected:
  inline const void toString(const T& t, std::stringstream& ss) {
    if constexpr (
        std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
        std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
      ss << t;
    } else if constexpr (
        std::is_same_v<T, float> || std::is_same_v<T, double>) {
      ss << javaFloatingPointToString(t);
    } else if constexpr (std::is_same_v<T, bool>) {
      std::string s = t ? "true" : "false";
      ss << s;
    } else if constexpr (std::is_same_v<T, StringView>) {
      ss << t.str();
    } else if constexpr (std::is_same_v<T, Timestamp>) {
      TimestampToStringOptions options{
          .precision = TimestampPrecision::kMilliseconds};
      ss << t.toString(options);
    } else {
      VELOX_FAIL("Not supported type: {}", typeid(T).name());
    }
  }

  template <bool throwException>
  inline const T fromString(const std::string& s, T defaultValue) {
    auto result = folly::tryTo<T>(s);
    if (result.hasValue()) {
      return result.value();
    } else {
      if constexpr (throwException) {
        VELOX_FAIL("Failed to format {} to type:{}", s, typeid(T).name());
      } else {
        return defaultValue;
      }
    }
  }

  inline const T fromString(const std::string& s) {
    if constexpr (
        std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
        std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
      return fromString<false>(s, 0);
    } else if constexpr (
        std::is_same_v<T, float> || std::is_same_v<T, double>) {
      return fromString<false>(s, 0.0);
    } else if constexpr (std::is_same_v<T, bool>) {
      return fromString<false>(s, false);
    } else if constexpr (std::is_same_v<T, StringView>) {
      StringView sv(s.data(), s.size());
      return sv;
    } else if constexpr (std::is_same_v<T, Timestamp>) {
      const auto parsed =
          util::fromTimestampWithTimezoneString(
              s.data(), s.size(), util::TimestampParseMode::kLegacyCast)
              .thenOrThrow(folly::identity, [&](const Status& status) {
                VELOX_FAIL("error while parse timestamp: {}", status.message());
              });
      return util::fromParsedTimestampWithTimeZone(parsed, timeZone_);
    } else {
      VELOX_FAIL("Not supported type: {}", typeid(T).name());
    }
  }
};

struct RowFormatter : public StringFormatter {
 public:
  RowFormatter(const std::vector<FormatterPtr>& formatters)
      : formatters_(formatters) {}

  const void toString(
      const VectorPtr& input,
      const TypePtr& type,
      const size_t index,
      std::stringstream& ss,
      const char* rowKindPrefix = "+I") override {
    auto rowInput = std::dynamic_pointer_cast<const RowVector>(input);
    auto rowType = std::dynamic_pointer_cast<const RowType>(type);
    VELOX_CHECK_EQ(rowInput->childrenSize(), rowType->children().size());
    VELOX_CHECK_EQ(rowInput->childrenSize(), formatters_.size());
    ss << rowKindPrefix << "[";
    for (size_t i = 0; i < rowInput->childrenSize(); ++i) {
      const VectorPtr& field = rowInput->childAt(i);
      const TypePtr& fieldType = rowType->childAt(i);
      if (field->isNullAt(index)) {
        ss << "null";
      } else {
        formatters_[i]->toString(field, fieldType, index, ss);
      }
      if (i != rowInput->childrenSize() - 1) {
        ss << ", ";
      }
    }
    ss << "]";
  }

  const void fromString(
      const std::string& s,
      const TypePtr& type,
      const size_t index,
      VectorPtr& vec) override {
    if (s == "null") {
      vec->setNull(index, true);
      return;
    }
    auto rowVector = std::dynamic_pointer_cast<RowVector>(vec);
    auto rowType = std::dynamic_pointer_cast<const RowType>(type);
    VELOX_CHECK_EQ(rowVector->childrenSize(), rowType->children().size());
    VELOX_CHECK_EQ(rowVector->childrenSize(), formatters_.size());
    std::string data = s;
    StringFormatter::normalize(data, TypeKind::ROW);
    std::vector<std::string> datas = StringFormatter::split(data, ",");
    VELOX_CHECK_EQ(datas.size(), rowVector->childrenSize());
    std::vector<VectorPtr>& rowFields = rowVector->children();
    const std::vector<TypePtr>& fieldTypes = rowType->children();
    for (size_t i = 0; i < datas.size(); ++i) {
      formatters_[i]->fromString(datas[i], fieldTypes[i], index, rowFields[i]);
    }
  }

 private:
  std::vector<FormatterPtr> formatters_;
};

struct ArrayFormatter : public StringFormatter {
 public:
  ArrayFormatter(const FormatterPtr& elementFormatter)
      : elementFormatter_(elementFormatter) {}

  const void toString(
      const VectorPtr& input,
      const TypePtr& type,
      const size_t index,
      std::stringstream& ss,
      const char* /*rowKindPrefix*/ = "+I") override {
    auto arrayInput = std::dynamic_pointer_cast<const ArrayVector>(input);
    auto arrayType = std::dynamic_pointer_cast<const ArrayType>(type);
    ss << "[";
    auto offset = arrayInput->offsetAt(index);
    auto size = arrayInput->sizeAt(index);
    auto elements = arrayInput->elements();
    for (size_t j = 0; j < size; ++j) {
      auto elementIndex = offset + j;
      if (elements->isNullAt(elementIndex)) {
        ss << "null";
      } else {
        elementFormatter_->toString(
            elements, arrayType->elementType(), elementIndex, ss);
      }
      if (j != size - 1) {
        ss << ", ";
      }
    }
    ss << "]";
  }

  const void fromString(
      const std::string& s,
      const TypePtr& type,
      const size_t index,
      VectorPtr& vec) override {
    if (s == "null") {
      vec->setNull(index, true);
      return;
    }
    auto arrayVector = std::dynamic_pointer_cast<ArrayVector>(vec);
    auto arrayType = std::dynamic_pointer_cast<const ArrayType>(type);
    std::string data = s;
    StringFormatter::normalize(data, TypeKind::ARRAY);
    std::vector<std::string> elements = StringFormatter::split(data, ",");
    VectorPtr& elementsVector = arrayVector->elements();
    auto offset = elementsVector->size();
    elementsVector->resize(offset + elements.size());
    for (size_t i = 0; i < elements.size(); ++i) {
      elementFormatter_->fromString(
          elements[i], arrayType->elementType(), offset + i, elementsVector);
    }
    arrayVector->setOffsetAndSize(index, offset, elements.size());
  }

 private:
  FormatterPtr elementFormatter_;
};

struct MapFormatter : public StringFormatter {
 public:
  MapFormatter(
      const FormatterPtr& keyFormatter,
      const FormatterPtr& valueFormatter)
      : keyFormatter_(keyFormatter), valueFormatter_(valueFormatter) {}

  const void toString(
      const VectorPtr& input,
      const TypePtr& type,
      const size_t index,
      std::stringstream& ss,
      const char* /*rowKindPrefix*/ = "+I") override {
    auto mapInput = std::dynamic_pointer_cast<const MapVector>(input);
    auto mapType = std::dynamic_pointer_cast<const MapType>(type);
    ss << "{";
    auto offset = mapInput->offsetAt(index);
    auto size = mapInput->sizeAt(index);

    auto keys = mapInput->mapKeys();
    auto values = mapInput->mapValues();
    auto keyType = mapType->keyType();
    auto valueType = mapType->valueType();

    for (size_t j = 0; j < size; ++j) {
      auto entryIndex = offset + j;

      if (keys->isNullAt(entryIndex)) {
        ss << "null";
      } else {
        keyFormatter_->toString(keys, keyType, entryIndex, ss);
      }

      ss << "=";

      if (values->isNullAt(entryIndex)) {
        ss << "null";
      } else {
        valueFormatter_->toString(values, valueType, entryIndex, ss);
      }

      if (j != size - 1) {
        ss << ", ";
      }
    }
    ss << "}";
  }

  const void fromString(
      const std::string& s,
      const TypePtr& type,
      const size_t index,
      VectorPtr& vec) override {
    if (s == "null") {
      vec->setNull(index, true);
      return;
    }
    auto mapVector = std::dynamic_pointer_cast<MapVector>(vec);
    auto mapType = std::dynamic_pointer_cast<const MapType>(type);
    std::string data = s;
    StringFormatter::normalize(data, TypeKind::MAP);
    std::vector<std::string> kvs = StringFormatter::split(data, ",");
    VectorPtr& keys = mapVector->mapKeys();
    VectorPtr& values = mapVector->mapValues();
    auto offset = keys->size();
    keys->resize(offset + kvs.size());
    values->resize(offset + kvs.size());
    const TypePtr& keyType = mapType->keyType();
    const TypePtr& valueType = mapType->valueType();
    for (size_t i = 0; i < kvs.size(); ++i) {
      std::vector<std::string> kv;
      boost::algorithm::split(kv, kvs[i], boost::is_any_of("="));
      VELOX_CHECK_EQ(kv.size(), 2);
      keyFormatter_->fromString(kv[0], keyType, i + offset, keys);
      valueFormatter_->fromString(kv[1], valueType, i + offset, values);
    }
    mapVector->setOffsetAndSize(index, offset, kvs.size());
  }

 private:
  FormatterPtr keyFormatter_;
  FormatterPtr valueFormatter_;
};

const FormatterPtr createFormatter(
    const TypePtr& type,
    const tz::TimeZone* timeZone);

} // namespace facebook::velox::connector
