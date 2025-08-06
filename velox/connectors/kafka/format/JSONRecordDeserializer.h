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

#include "velox/functions/prestosql/json/SIMDJsonWrapper.h"
#include "velox/connectors/kafka/KafkaRecordDeserializer.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/Type.h"
#include <type_traits>
#include <typeinfo>

namespace facebook::velox::connector::kafka {

using Element = simdjson::dom::element;
using Elements = std::vector<simdjson::dom::element>;

struct JSONDeserializer {
 public:
  virtual inline const void
  deserialize(const Element& e, const size_t index, VectorPtr& vec) = 0;

  static const std::shared_ptr<JSONDeserializer> create(const TypePtr& type);
};

template <typename T>
struct BaseDeserializer : public JSONDeserializer {
 public:
  inline const void
  deserialize(const Element& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<T>>(vec);
    if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
      flat->set(index, static_cast<T>(e.get_double().value()));
    } else if constexpr (std::is_same_v<T, bool>) {
      flat->set(index, e.get_bool().value());
    } else if constexpr (std::is_same_v<T, facebook::velox::StringView>) {
      const auto s = e.get_string().value();
      flat->set(index, StringView(s.data(), s.size()));
    } else if constexpr (std::is_same_v<T, facebook::velox::Timestamp>) {
      const auto s = e.get_string().value();
      const auto timestamp =
          util::fromTimestampString(
              s.data(), s.size(), util::TimestampParseMode::kLegacyCast)
              .thenOrThrow(folly::identity, [&](const Status& status) {
                VELOX_FAIL("error while parse timestamp: {}", status.message());
              });
      flat->set(index, timestamp);
    } else if constexpr (
        std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
        std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
      flat->set(index, static_cast<T>(e.get_int64().value()));
    } else {
      VELOX_FAIL("The type {} is not supported", typeid(T).name());
    }
  }
};

struct RowDeserializer : public JSONDeserializer {
 public:
  RowDeserializer(
      const std::vector<std::string>& fieldNames,
      const std::vector<TypePtr>& fieldTypes,
      const std::vector<std::shared_ptr<JSONDeserializer>>& deserializers)
      : fieldNames_(fieldNames),
        fieldTypes_(fieldTypes),
        deserializers_(deserializers) {}

  inline const void
  deserialize(const Element& e, const size_t index, VectorPtr& vec) override {
    RowVectorPtr rowVector = std::dynamic_pointer_cast<RowVector>(vec);
    std::vector<VectorPtr>& rowFields = rowVector->children();
    for (size_t i = 0; i < fieldNames_.size(); ++i) {
      if (e[fieldNames_[i]].is_null()) {
        rowFields[i]->setNull(index, true);
      } else {
        deserializers_[i]->deserialize(e[fieldNames_[i]], index, rowFields[i]);
      }
    }
  }

 private:
  std::vector<std::string> fieldNames_;
  std::vector<TypePtr> fieldTypes_;
  std::vector<std::shared_ptr<JSONDeserializer>> deserializers_;
};

struct ArrayDeserializer : public JSONDeserializer {
 public:
  ArrayDeserializer(
      const TypePtr& elementType,
      const std::shared_ptr<JSONDeserializer>& deserializer)
      : elementType_(elementType), elementDeserializer_(deserializer) {}

  inline const void
  deserialize(const Element& e, const size_t index, VectorPtr& vec) override {}

 private:
  TypePtr elementType_;
  std::shared_ptr<JSONDeserializer> elementDeserializer_;
};

struct MapDeserializer : public JSONDeserializer {
 public:
  MapDeserializer(
      const TypePtr& keyType,
      const TypePtr& valueType,
      const std::shared_ptr<JSONDeserializer>& keyDeserializer,
      const std::shared_ptr<JSONDeserializer>& valueDeserializer)
      : keyType_(keyType),
        valueType_(valueType),
        keyDeserializer_(keyDeserializer),
        valueDeserializer_(valueDeserializer) {}

  inline const void
  deserialize(const Element& e, const size_t index, VectorPtr& vec) override {}
  
 private:
  TypePtr keyType_;
  TypePtr valueType_;
  std::shared_ptr<JSONDeserializer> keyDeserializer_;
  std::shared_ptr<JSONDeserializer> valueDeserializer_;
};

/// Class for kafka record deserialization of json format.
class KafkaJSONRecordDeserializer : public KafkaRecordDeserializer {
 public:
  KafkaJSONRecordDeserializer(
      const RowTypePtr& outputType,
      memory::MemoryPool* memoryPool)
      : KafkaRecordDeserializer(outputType, memoryPool),
        deserializer_(std::dynamic_pointer_cast<RowDeserializer>(
            JSONDeserializer::create(outputType))),
        parser_(std::make_shared<simdjson::dom::parser>()) {}

  const void deserialize(
      const std::string& message,
      const size_t index,
      VectorPtr& vec) override;

 private:
  std::shared_ptr<RowDeserializer> deserializer_;
  std::shared_ptr<simdjson::dom::parser> parser_;
};

} // namespace facebook::velox::connector::kafka
