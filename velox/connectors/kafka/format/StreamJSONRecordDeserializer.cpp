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

#include "velox/connectors/kafka/format/StreamJSONRecordDeserializer.h"

namespace facebook::velox::connector::kafka {

const std::shared_ptr<StreamJSONDeserializer> StreamJSONDeserializer::create(
    const TypePtr& type) {
  const TypeKind kind = type->kind();
  if (kind == TypeKind::INTEGER) {
    return std::make_shared<StreamIntDeserializer>();
  } else if (kind == TypeKind::BIGINT) {
    return std::make_shared<StreamBigIntDeserializer>();
  } else if (kind == TypeKind::HUGEINT) {
    return std::make_shared<StreamHugeIntDeserializer>();
  } else if (kind == TypeKind::VARCHAR) {
    return std::make_shared<StreamStringDeserializer>();
  } else if (kind == TypeKind::TIMESTAMP) {
    return std::make_shared<StreamTimestampDeserializer>();
  } else if (kind == TypeKind::ROW) {
    const RowTypePtr rowType = std::dynamic_pointer_cast<const RowType>(type);
    const std::vector<std::string> fieldNames = rowType->names();
    const std::vector<TypePtr> fieldTypes = rowType->children();
    std::vector<std::shared_ptr<StreamJSONDeserializer>> deserializers;
    for (const auto& fieldType : fieldTypes) {
      deserializers.emplace_back(create(fieldType));
    }
    return std::make_shared<StreamRowDeserializer>(
        fieldNames, fieldTypes, deserializers);
  } else {
    VELOX_FAIL("The type is not supported: {}", type);
  }
}

const void KafkaStreamJSONRecordDeserializer::deserialize(
    const std::string& message,
    const size_t index,
    VectorPtr& vec) {
  try {
    simdjson::padded_string_view json_padded(
        message.data(),
        message.size(),
        message.size() + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::document doc = parser_->iterate(json_padded);
    JSONValue value = doc.get_value();
    if (value.is_null()) {
      vec->setNull(index, true);
    } else {
      deserializer_->deserialize(value, index, vec);
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to deserialize record: " << message
                 << " , error: " << e.what();
  }
}

} // namespace facebook::velox::connector::kafka