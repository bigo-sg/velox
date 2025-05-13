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

#include "velox/connectors/kafka/KafkaRecordDeserializer.h"

namespace facebook::velox::connector::kafka {
/// Class for kafka record deserialization of csv format.
class KafkaCSVRecordDeserializer : public KafkaRecordDeserializer {
 public:
  KafkaCSVRecordDeserializer(
      const RowTypePtr& outputType,
      memory::MemoryPool* memoryPool)
      : KafkaRecordDeserializer(outputType, memoryPool) {}

  const void deserialize(
      const std::string & message,
      const size_t index,
      VectorPtr& vec) override {
        VELOX_NYI("Not implemented.");   
    }
};

} // namespace facebook::velox::connector::kafka
