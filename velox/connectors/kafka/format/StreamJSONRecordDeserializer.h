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

#include "velox/connectors/kafka/format/StreamJSONRecordDeserializer.h"
#include "velox/connectors/kafka/format/KafkaRecordDeserializer.h"

#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "simdjson.h"
#include <type_traits>
#include <typeinfo>

namespace facebook::velox::connector::kafka {

using JSONDoc = simdjson::ondemand::document;
using JSONValue = simdjson::ondemand::value;
using JSONRow = simdjson::ondemand::object;
using JSONArray = simdjson::ondemand::array;

struct StreamJSONDeserializer {
 public:
  virtual ~StreamJSONDeserializer() = default;

  static const std::shared_ptr<StreamJSONDeserializer> create(
      const TypePtr& type);

  virtual const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) = 0;
};

struct StreamIntDeserializer : public StreamJSONDeserializer {
 public:
  StreamIntDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<int32_t>>(vec);
    flat->set(index, static_cast<int32_t>(e.get_int64()));
  }
};

struct StreamBigIntDeserializer : public StreamJSONDeserializer {
 public:
  StreamBigIntDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec);
    flat->set(index, e.get_int64());
  }
};

struct StreamHugeIntDeserializer : public StreamJSONDeserializer {
 public:
  StreamHugeIntDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<int128_t>>(vec);
    flat->set(index, static_cast<int128_t>(e.get_int64()));
  }
};

struct StreamStringDeserializer : public StreamJSONDeserializer {
 public:
  StreamStringDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat =
        std::dynamic_pointer_cast<FlatVector<facebook::velox::StringView>>(vec);
    std::string_view s = e.get_string();
    flat->set(index, facebook::velox::StringView(s.data(), s.size()));
  }
};

struct StreamTimestampDeserializer : public StreamJSONDeserializer {
 public:
  StreamTimestampDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat =
        std::dynamic_pointer_cast<FlatVector<facebook::velox::Timestamp>>(vec);
    std::string_view s = e.get_string();
    const auto timestamp =
        util::fromTimestampString(
            s.data(), s.size(), util::TimestampParseMode::kLegacyCast)
            .thenOrThrow(folly::identity, [&](const Status& status) {
              VELOX_FAIL("error while parse timestamp: {}", status.message());
            });
    flat->set(index, timestamp);
  }
};

struct StreamRowDeserializer : public StreamJSONDeserializer {
 public:
  StreamRowDeserializer(
      const std::vector<std::string>& fieldNames,
      const std::vector<TypePtr>& fieldTypes,
      const std::vector<std::shared_ptr<StreamJSONDeserializer>>& deserializers)
      : fieldNames_(fieldNames),
        fieldTypes_(fieldTypes),
        deserializers_(deserializers) {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    RowVectorPtr rowVector = std::dynamic_pointer_cast<RowVector>(vec);
    std::vector<VectorPtr>& rowFields = rowVector->children();
    JSONRow row = e.get_object();
    for (size_t i = 0; i < fieldNames_.size(); ++i) {
      JSONValue v;
      auto err = row[fieldNames_[i]].get(v);
      if (err != simdjson::error_code::SUCCESS || v.is_null()) {
        rowFields[i]->setNull(index, true);
      } else {
        deserializers_[i]->deserialize(v, index, rowFields[i]);
      }
    }
  }

 private:
  std::vector<std::string> fieldNames_;
  std::vector<TypePtr> fieldTypes_;
  std::vector<std::shared_ptr<StreamJSONDeserializer>> deserializers_;
};

/// Class for kafka record deserialization of json format, using the streaming
/// interface of simdjson
class KafkaStreamJSONRecordDeserializer : public KafkaRecordDeserializer {
 public:
  KafkaStreamJSONRecordDeserializer(
      const RowTypePtr& outputType,
      memory::MemoryPool* memoryPool)
      : KafkaRecordDeserializer(outputType, memoryPool),
        deserializer_(std::dynamic_pointer_cast<StreamRowDeserializer>(
            StreamJSONDeserializer::create(outputType))),
        parser_(std::make_shared<simdjson::ondemand::parser>()) {}

  const void deserialize(
      const std::string& message,
      const size_t index,
      VectorPtr& vec) override;

 private:
  std::shared_ptr<StreamRowDeserializer> deserializer_;
  std::shared_ptr<simdjson::ondemand::parser> parser_;
};

} // namespace facebook::velox::connector::kafka
