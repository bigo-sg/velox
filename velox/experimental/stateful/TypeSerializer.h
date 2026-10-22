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
#include "velox/experimental/stateful/state/CheckpointStream.h"
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

  /// Identity of the serialized layout: a canonical, host-independent
  /// string naming the type and its encoding (for instance
  /// "timestamp(raw:i64,i32)"). The checkpoint header carries it and
  /// restore cross-checks it against the registered states' serializers.
  /// Serializers without a declared id fail here rather than silently
  /// matching every other undeclared serializer.
  virtual std::string schemaId() const {
    VELOX_NYI("This serializer does not declare a schema id");
  }
};

using TypeSerializerPtr = std::shared_ptr<TypeBaseSerializer>;

template <typename D>
class TypeSerializer : public TypeBaseSerializer {
 public:
  TypeSerializer() : TypeBaseSerializer() {}
  ~TypeSerializer() override = default;
  /// Serialize the given value to char array.
  virtual std::string serialize(const D& data) = 0;

  /// Deserialize the given bytes to a value. 'str' is a zero-copy view into
  /// the checkpoint buffer; implementations must not keep the view (or
  /// pointers into it) beyond the returned value's own construction.
  virtual D deserialize(std::string_view str) = 0;

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

  std::string schemaId() const override {
    if constexpr (std::is_same_v<D, int8_t>) {
      return "i8";
    } else if constexpr (std::is_same_v<D, uint8_t>) {
      return "u8";
    } else if constexpr (std::is_same_v<D, int16_t>) {
      return "i16";
    } else if constexpr (std::is_same_v<D, uint16_t>) {
      return "u16";
    } else if constexpr (std::is_same_v<D, int32_t>) {
      return "i32";
    } else if constexpr (std::is_same_v<D, uint32_t>) {
      return "u32";
    } else if constexpr (std::is_same_v<D, int64_t>) {
      return "i64";
    } else if constexpr (std::is_same_v<D, uint64_t>) {
      return "u64";
    } else if constexpr (std::is_same_v<D, int128_t>) {
      return "i128";
    } else if constexpr (std::is_same_v<D, uint128_t>) {
      return "u128";
    } else if constexpr (std::is_same_v<D, float>) {
      return "f32";
    } else if constexpr (std::is_same_v<D, double>) {
      return "f64";
    } else if constexpr (std::is_same_v<D, bool>) {
      return "bool";
    } else if constexpr (std::is_same_v<D, StringView>) {
      return "varchar";
    } else if constexpr (std::is_same_v<D, Timestamp>) {
      return "timestamp(raw:i64,i32)";
    } else {
      VELOX_FAIL("Type {} is not supported", typeid(D).name());
    }
  }

  std::string serialize(const D& t) override {
    if constexpr (
        std::is_same_v<D, int8_t> || std::is_same_v<D, int16_t> ||
        std::is_same_v<D, int32_t> || std::is_same_v<D, int64_t> ||
        std::is_same_v<D, int128_t> || std::is_same_v<D, uint8_t> ||
        std::is_same_v<D, uint16_t> || std::is_same_v<D, uint32_t> ||
        std::is_same_v<D, uint64_t> || std::is_same_v<D, uint128_t> ||
        std::is_same_v<D, float> || std::is_same_v<D, double>) {
      return littleEndianBytes(t);
    } else if constexpr (std::is_same_v<D, bool>) {
      return std::string(1, t ? '\x01' : '\x00');
    } else if constexpr (std::is_same_v<D, StringView>) {
      return std::string(t.data(), t.size());
    } else if constexpr (std::is_same_v<D, Timestamp>) {
      // The raw (seconds, nanos) pair, little-endian: the full nanosecond
      // precision round-trips, the same raw pair the key path carries.
      return littleEndianBytes(t.getSeconds()) +
          littleEndianBytes(static_cast<uint32_t>(t.getNanos()));
    } else {
      VELOX_FAIL("Type {} is not supported", typeid(D).name());
    }
  }

  D deserialize(std::string_view str) override {
    if constexpr (
        std::is_same_v<D, int8_t> || std::is_same_v<D, int16_t> ||
        std::is_same_v<D, int32_t> || std::is_same_v<D, int64_t> ||
        std::is_same_v<D, int128_t> || std::is_same_v<D, uint8_t> ||
        std::is_same_v<D, uint16_t> || std::is_same_v<D, uint32_t> ||
        std::is_same_v<D, uint64_t> || std::is_same_v<D, uint128_t> ||
        std::is_same_v<D, float> || std::is_same_v<D, double>) {
      return littleEndianValue<D>(str);
    } else if constexpr (std::is_same_v<D, bool>) {
      VELOX_CHECK_EQ(
          str.size(), 1, "Corrupt checkpoint: truncated boolean value");
      return str[0] != '\x00';
    } else if constexpr (std::is_same_v<D, StringView>) {
      // Borrowed result: the StringView points into 'str'; the caller must
      // copy it (as FlatVector::set does) if it outlives the checkpoint
      // buffer.
      return StringView(str.data(), str.size());
    } else if constexpr (std::is_same_v<D, Timestamp>) {
      VELOX_CHECK_EQ(
          str.size(),
          sizeof(int64_t) + sizeof(uint32_t),
          "Corrupt checkpoint: truncated timestamp value");
      return Timestamp(
          littleEndianValue<int64_t>(str.substr(0, sizeof(int64_t))),
          littleEndianValue<uint32_t>(str.substr(sizeof(int64_t))));
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

  std::string schemaId() const override {
    return "presto:" + dataType_->toString();
  }

  std::string serialize(const D& t) override {
    std::ostringstream output;
    serde_->serializeSingleColumn(t, nullptr, pool_, &output);
    return output.str();
  }

  D deserialize(std::string_view str) override {
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

  const TypePtr& getDataType() const {
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

/// Serializes a shared value as a null flag byte followed by the inner
/// serializer's payload, so a null pointee round-trips as nullptr. Used for
/// value-state values, which are nullable pointer types.
template <typename T>
class SharedPtrSerializer : public TypeSerializer<std::shared_ptr<T>> {
 public:
  explicit SharedPtrSerializer(std::shared_ptr<TypeSerializer<T>> inner)
      : inner_(std::move(inner)) {}

  std::string schemaId() const override {
    return "shared_ptr<" + inner_->schemaId() + ">";
  }

  std::string serialize(const std::shared_ptr<T>& value) override {
    if (value == nullptr) {
      return std::string(1, static_cast<char>(0));
    }
    return std::string(1, static_cast<char>(1)) + inner_->serialize(*value);
  }

  std::shared_ptr<T> deserialize(std::string_view str) override {
    VELOX_CHECK(!str.empty(), "Missing null flag in the shared value payload");
    const char flag = str[0];
    VELOX_CHECK(
        flag == 0 || flag == 1,
        "Corrupt checkpoint: shared value null flag must be 0 or 1, got {}",
        static_cast<int>(flag));
    if (flag == 0) {
      return nullptr;
    }
    // string_view::substr is O(1): the inner serializer reads the payload
    // straight out of the checkpoint buffer.
    return std::make_shared<T>(inner_->deserialize(str.substr(1)));
  }

 private:
  const std::shared_ptr<TypeSerializer<T>> inner_;
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
  } else if (kind == TypeKind::VARBINARY) {
    using T10 = TypeTraits<TypeKind::VARBINARY>::NativeType;
    return std::make_shared<ValueSerializer<T10>>();
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
