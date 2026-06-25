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

#include "velox/connectors/kafka/format/KafkaRecordDeserializer.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::kafka {
/// Class for kafka record deserialization of raw format.
class KafkaRawRecordDeserializer : public KafkaRecordDeserializer {
 public:
  KafkaRawRecordDeserializer(
      const RowTypePtr& outputType,
      memory::MemoryPool* memoryPool)
      : KafkaRecordDeserializer(outputType, memoryPool) {
    VELOX_CHECK_EQ(
        outputType_->size(),
        1,
        "Output type size of raw deserializer must be 1.");
    const TypePtr& childType = outputType_->childAt(0);
    VELOX_CHECK_EQ(
        childType->kind(),
        TypeKind::VARCHAR,
        "Output type must be Row(String).");
  }

  const void deserialize(
      const std::string& message,
      const size_t index,
      VectorPtr& vec) override {
    RowVectorPtr rowVector = std::dynamic_pointer_cast<RowVector>(vec);
    VELOX_CHECK_EQ(
        rowVector->childrenSize(),
        1,
        "The raw record vector children size {} is not 1",
        rowVector->childrenSize());
    VectorPtr& childVector = rowVector->children()[0];
    auto flat =
        std::dynamic_pointer_cast<FlatVector<velox::StringView>>(childVector);
    flat->set(index, StringView(message.data(), message.size()));
  }
};

} // namespace facebook::velox::connector::kafka
