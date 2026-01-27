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

#include <cstdint>
#include <memory>
#include <sstream>
#include <type_traits>
#include <typeinfo>
#include <vector>
#include "velox/common/memory/MemoryPool.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/HugeInt.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

class TypeBaseSerializer {
 public:
  TypeBaseSerializer() {}
  virtual ~TypeBaseSerializer() = default;
};

using TypeSerializerPtr = std::shared_ptr<TypeBaseSerializer>;

template <typename D>
class TypeSerializer : public TypeBaseSerializer {
 public:
  TypeSerializer() : TypeBaseSerializer() {}
  ~TypeSerializer() override = default;
  /// Serialize the given value to char array.
  virtual const std::string serialize(const D& data) = 0;

  /// Deserialize the give char array to value.
  virtual const D deserialize(const std::string& str) = 0;

 protected:
  std::unique_ptr<ByteInputStream> toByteStream(
      const char* data,
      const size_t len) {
    ByteRange byteRange{
        reinterpret_cast<uint8_t*>(const_cast<char*>(data)),
        static_cast<int64_t>(len),
        0};
    return std::make_unique<BufferInputStream>(
        std::vector<ByteRange>{{byteRange}});
  }
};

template <typename D>
class ValueSerializer : public TypeSerializer<D> {
 public:
  ValueSerializer() : TypeSerializer<D>() {}

  const std::string serialize(const D& t) override {
    if constexpr (
        std::is_same_v<D, int8_t> || std::is_same_v<D, int16_t> ||
        std::is_same_v<D, int32_t> || std::is_same_v<D, int64_t> ||
        std::is_same_v<D, int128_t> || std::is_same_v<D, uint8_t> ||
        std::is_same_v<D, uint16_t> || std::is_same_v<D, uint32_t> ||
        std::is_same_v<D, uint64_t> || std::is_same_v<D, uint128_t> ||
        std::is_same_v<D, float> || std::is_same_v<D, double>) {
      std::vector<char> buffer(sizeof(D));
      std::memcpy(buffer.data(), &t, sizeof(D));
      return std::string(buffer.data(), buffer.size());
    } else if constexpr (std::is_same_v<D, bool>) {
      uint8_t byteValue = t ? 1 : 0;
      std::vector<char> buffer(sizeof(uint8_t));
      std::memcpy(buffer.data(), &byteValue, sizeof(uint8_t));
      return std::string(buffer.data(), buffer.size());
    } else if constexpr (std::is_same_v<D, StringView>) {
      return std::string(t.data(), t.size());
    } else if constexpr (std::is_same_v<D, Timestamp>) {
      int64_t mills = t.toMillis();
      std::vector<char> buffer(sizeof(int64_t));
      std::memcpy(buffer.data(), &mills, sizeof(int64_t));
      return std::string(buffer.data(), buffer.size());
    } else {
      VELOX_FAIL("Type {} is not supported", typeid(D).name());
    }
  }

  const D deserialize(const std::string& str) override {
    if constexpr (
        std::is_same_v<D, int8_t> || std::is_same_v<D, int16_t> ||
        std::is_same_v<D, int32_t> || std::is_same_v<D, int64_t> ||
        std::is_same_v<D, int128_t> || std::is_same_v<D, uint8_t> ||
        std::is_same_v<D, uint16_t> || std::is_same_v<D, uint32_t> ||
        std::is_same_v<D, uint64_t> || std::is_same_v<D, uint128_t> ||
        std::is_same_v<D, float> || std::is_same_v<D, double>) {
      D t;
      std::memcpy(&t, str.data(), str.size());
      return t;
    } else if constexpr (std::is_same_v<D, bool>) {
      uint8_t byteValue;
      std::memcpy(&byteValue, str.data(), str.size());
      return byteValue > 0 ? true : false;
    } else if constexpr (std::is_same_v<D, StringView>) {
      return StringView(str.data(), str.size());
    } else if constexpr (std::is_same_v<D, Timestamp>) {
      int64_t mills;
      std::memcpy(&mills, str.data(), str.size());
      return Timestamp::fromMillis(mills);
    } else {
      VELOX_FAIL("Type {} is not supported", typeid(D).name());
    }
  }
};

template <typename D>
class ComplexVectorSerializer : public TypeSerializer<D> {
 public:
  ComplexVectorSerializer(const TypePtr& dataType, memory::MemoryPool* pool)
      : dataType_(dataType),
        pool_(pool),
        serde_(std::make_shared<serializer::presto::PrestoVectorSerde>()) {
    checkTypes();
  }

  const std::string serialize(const D& t) override {
    std::ostringstream output;
    serde_->serializeSingleColumn(t, nullptr, pool_, &output);
    return output.str();
  }

  const D deserialize(const std::string& str) override {
    auto byteStream = TypeSerializer<D>::toByteStream(str.data(), str.size());
    VectorPtr vec;
    serde_->deserializeSingleColumn(
        byteStream.get(), pool_, dataType_, &vec, nullptr);
    if constexpr (std::is_same_v<D, RowVectorPtr>) {
      return std::dynamic_pointer_cast<RowVector>(vec);
    } else if constexpr (std::is_same_v<D, ArrayVectorPtr>) {
      return std::dynamic_pointer_cast<ArrayVector>(vec);
    } else if constexpr (std::is_same_v<D, MapVectorPtr>) {
      return std::dynamic_pointer_cast<MapVector>(vec);
    } else {
      VELOX_FAIL(
          "Vector type not valid, this complex vector seralizer can only suupport rowvector/arrayvector/mapvector.");
    }
  }

  const TypePtr getDataType() const {
    return dataType_;
  }

 private:
  const TypePtr dataType_;
  memory::MemoryPool* pool_;
  const std::shared_ptr<serializer::presto::PrestoVectorSerde> serde_;

  void checkTypes() {
    if (!std::is_same_v<D, RowVectorPtr> &&
        !std::is_same_v<D, ArrayVectorPtr> &&
        !std::is_same_v<D, MapVectorPtr>) {
      VELOX_FAIL(
          "Vector type not valid, this complex vector seralizer can only suupport rowvector/arrayvector/mapvector.");
    }
  }
};

inline TypeSerializerPtr createSerializer(
    const TypePtr& type,
    const bool isUnsigned = false,
    memory::MemoryPool* pool = nullptr) {
  const TypeKind kind = type->kind();
  if (kind == TypeKind::INTEGER) {
    if (isUnsigned) {
      return std::make_shared<ValueSerializer<uint32_t>>();
    } else {
      return std::make_shared<ValueSerializer<int32_t>>();
    }
  } else if (kind == TypeKind::BIGINT) {
    if (isUnsigned) {
      return std::make_shared<ValueSerializer<uint64_t>>();
    } else {
      return std::make_shared<ValueSerializer<int64_t>>();
    }
  } else if (kind == TypeKind::HUGEINT) {
    if (isUnsigned) {
      return std::make_shared<ValueSerializer<uint128_t>>();
    } else {
      return std::make_shared<ValueSerializer<int128_t>>();
    }
  } else if (kind == TypeKind::SMALLINT) {
    if (isUnsigned) {
      return std::make_shared<ValueSerializer<uint16_t>>();
    } else {
      return std::make_shared<ValueSerializer<int16_t>>();
    }
  } else if (kind == TypeKind::TINYINT) {
    if (isUnsigned) {
      return std::make_shared<ValueSerializer<uint8_t>>();
    } else {
      return std::make_shared<ValueSerializer<int8_t>>();
    }
  } else if (kind == TypeKind::REAL) {
    using T5 = TypeTraits<TypeKind::REAL>::NativeType;
    return std::make_shared<ValueSerializer<T5>>();
  } else if (kind == TypeKind::DOUBLE) {
    using T6 = TypeTraits<TypeKind::DOUBLE>::NativeType;
    return std::make_shared<ValueSerializer<T6>>();
  } else if (kind == TypeKind::BOOLEAN) {
    using T7 = TypeTraits<TypeKind::BOOLEAN>::NativeType;
    return std::make_shared<ValueSerializer<T7>>();
  } else if (kind == TypeKind::VARCHAR) {
    using T8 = TypeTraits<TypeKind::VARCHAR>::NativeType;
    return std::make_shared<ValueSerializer<T8>>();
  } else if (kind == TypeKind::TIMESTAMP) {
    using T9 = TypeTraits<TypeKind::TIMESTAMP>::NativeType;
    return std::make_shared<ValueSerializer<T9>>();
  } else if (kind == TypeKind::ROW) {
    const std::shared_ptr<const RowType> rowType =
        std::dynamic_pointer_cast<const RowType>(type);
    return std::make_shared<ComplexVectorSerializer<RowVectorPtr>>(
        rowType, pool);
  } else if (kind == TypeKind::ARRAY) {
    const std::shared_ptr<const ArrayType> arrayType =
        std::dynamic_pointer_cast<const ArrayType>(type);
    return std::make_shared<ComplexVectorSerializer<ArrayVectorPtr>>(
        arrayType, pool);
  } else if (kind == TypeKind::MAP) {
    const std::shared_ptr<const MapType> mapType =
        std::dynamic_pointer_cast<const MapType>(type);
    return std::make_shared<ComplexVectorSerializer<MapVectorPtr>>(
        mapType, pool);
  } else {
    VELOX_FAIL("Type {} not supported", type->name());
  }
}
} // namespace facebook::velox::stateful
