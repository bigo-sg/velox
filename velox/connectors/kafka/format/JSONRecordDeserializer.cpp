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

#include "velox/connectors/kafka/format/JSONRecordDeserializer.h"

namespace facebook::velox::connector::kafka {

using IntDeserializer = BaseDeserializer<int32_t>;
using BigIntDeseralizer = BaseDeserializer<int64_t>;
using SmallIntDeserializer = BaseDeserializer<int16_t>;
using TinyIntDeserializer = BaseDeserializer<int8_t>;
using BoolDeserializer = BaseDeserializer<bool>;
using FloatDeserializer = BaseDeserializer<float>;
using DoubleDeserializer = BaseDeserializer<double>;
using StringDeserializer = BaseDeserializer<facebook::velox::StringView>;
using TimestampDeserializer = BaseDeserializer<facebook::velox::Timestamp>;

const std::shared_ptr<JSONDeserializer> JSONDeserializer::create(
    const TypePtr& type) {
  const TypeKind kind = type->kind();
  if (kind == TypeKind::INTEGER) {
    return std::make_shared<IntDeserializer>();
  } else if (kind == TypeKind::BIGINT) {
    return std::make_shared<BigIntDeseralizer>();
  } else if (kind == TypeKind::SMALLINT) {
    return std::make_shared<SmallIntDeserializer>();
  } else if (kind == TypeKind::TINYINT) {
    return std::make_shared<TinyIntDeserializer>();
  } else if (kind == TypeKind::BOOLEAN) {
    return std::make_shared<BoolDeserializer>();
  } else if (kind == TypeKind::VARCHAR) {
    return std::make_shared<StringDeserializer>();
  } else if (kind == TypeKind::REAL) {
    return std::make_shared<FloatDeserializer>();
  } else if (kind == TypeKind::DOUBLE) {
    return std::make_shared<DoubleDeserializer>();
  } else if (kind == TypeKind::TIMESTAMP) {
    return std::make_shared<TimestampDeserializer>();
  } else if (kind == TypeKind::ROW) {
    const RowTypePtr rowType = std::dynamic_pointer_cast<const RowType>(type);
    const std::vector<std::string> fieldNames = rowType->names();
    const std::vector<TypePtr> fieldTypes = rowType->children();
    std::vector<std::shared_ptr<JSONDeserializer>> deserializers;
    for (const auto& fieldType : fieldTypes) {
      deserializers.emplace_back(create(fieldType));
    }
    return std::make_shared<RowDeserializer>(
        fieldNames, fieldTypes, deserializers);
  } else if (kind == TypeKind::ARRAY) {
    const std::shared_ptr<const ArrayType> arrayType =
        std::dynamic_pointer_cast<const ArrayType>(type);
    const TypePtr& elementType = arrayType->elementType();
    const std::shared_ptr<JSONDeserializer> elementDeserializer =
        create(elementType);
    return std::make_shared<ArrayDeserializer>(
        elementType, elementDeserializer);
  } else if (kind == TypeKind::MAP) {
    const std::shared_ptr<const MapType> mapType =
        std::dynamic_pointer_cast<const MapType>(type);
    const TypePtr& keyType = mapType->keyType();
    const TypePtr& valueType = mapType->valueType();
    const std::shared_ptr<JSONDeserializer> keyDeserializer = create(keyType);
    const std::shared_ptr<JSONDeserializer> valueDeserializer =
        create(valueType);
    return std::make_shared<MapDeserializer>(
        keyType, valueType, keyDeserializer, valueDeserializer);
  } else {
    VELOX_FAIL("The type is not supported: {}", type);
  }
}

const void KafkaJSONRecordDeserializer::deserialize(
    const std::string& message,
    const size_t index,
    VectorPtr& vec) {
  try {
    Element element;
    parser_->parse(message.data(), message.size()).get(element);
    if (element.is_null()) {
      vec->setNull(true, index);
    } else {
      deserializer_->deserialize(std::move(element), index, vec);
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to deserialize record: " << message
                 << " , error: " << e.what();
  }
}

} // namespace facebook::velox::connector::kafka
