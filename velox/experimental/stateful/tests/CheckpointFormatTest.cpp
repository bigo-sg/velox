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
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "velox/experimental/stateful/state/CheckpointStream.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::stateful {
namespace {

using namespace facebook::velox;

constexpr uint32_t kMaxParallelism = 128;

// Wire-format tests of the keyed-state checkpoint stream: the fixed header,
// the little-endian integer encoding, the entry framing, the CRC-32 trailer
// and the restore-time cross-checks (schema ids, maxParallelism, group
// order). The cross-producer test hand-builds a stream byte by byte, so a
// format change that stays internal to the writer still fails here.
class CheckpointFormatTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::vector<RowContainerStateKey> probeKeys(
      KeySelector& selector,
      const std::vector<std::optional<int64_t>>& values) {
    selector.probe(makeRowVector({makeNullableFlatVector<int64_t>(values)}));
    auto keys = selector.keys();
    return std::vector<RowContainerStateKey>(keys.begin(), keys.end());
  }

  TypeSerializerPtr int64Serializer() {
    return std::make_shared<ValueSerializer<int64_t>>();
  }

  // --- Hand-building helpers: append stream components without the writer
  // so the layout, not the writer's calls, is what the test pins. ---

  void appendU8(std::string& out, uint8_t value) {
    out.push_back(static_cast<char>(value));
  }

  void appendU32(std::string& out, uint32_t value) {
    out += littleEndianBytes(value);
  }

  void appendI32(std::string& out, int32_t value) {
    out += littleEndianBytes(static_cast<uint32_t>(value));
  }

  void appendBytes(std::string& out, std::string_view value) {
    appendU32(out, static_cast<uint32_t>(value.size()));
    out.append(value.data(), value.size());
  }

  // The schema table of a one-state stream over a BIGINT key with a
  // list-of-int64 state named "list".
  void appendBigIntListSchemaTable(std::string& out) {
    appendBytes(out, "(BIGINT)");
    appendU32(out, 1);
    appendBytes(out, "list");
    appendBytes(out, "void");
    appendBytes(out, "list<i64>");
  }
};

// The fixed header: size on the wire, magic and version leading the stream,
// fields in order, round-trip through the read side.
TEST_F(CheckpointFormatTest, headerRoundTrip) {
  std::string bytes;
  CheckpointWriter writer(bytes);
  CheckpointHeader header;
  header.flags = kCheckpointFlagLittleEndian | kCheckpointFlagCrcTrailer;
  header.maxParallelism = 128;
  header.startKeyGroup = 64;
  header.numKeyGroups = 64;
  writeCheckpointHeader(writer, header);
  // Magic (4) + version (1) + flags (1) + three labels (4 each).
  ASSERT_EQ(18, kCheckpointHeaderSize);
  ASSERT_EQ(kCheckpointHeaderSize, bytes.size());
  EXPECT_EQ(0, bytes.compare(0, 4, "VSKV"));
  EXPECT_EQ(kCheckpointFormatVersion, static_cast<uint8_t>(bytes[4]));
  EXPECT_EQ(header.flags, static_cast<uint8_t>(bytes[5]));

  CheckpointReader reader(bytes.data(), bytes.size());
  const auto restored = readCheckpointHeader(reader);
  EXPECT_EQ(header.flags, restored.flags);
  EXPECT_EQ(header.maxParallelism, restored.maxParallelism);
  EXPECT_EQ(header.startKeyGroup, restored.startKeyGroup);
  EXPECT_EQ(header.numKeyGroups, restored.numKeyGroups);
  EXPECT_TRUE(reader.atEnd());
}

// The integer encoding and the CRC are pinned to independent reference
// values, not to the encoder's own output.
TEST_F(CheckpointFormatTest, littleEndianAndCrcAnchors) {
  EXPECT_EQ(
      std::string("\x04\x03\x02\x01", 4),
      littleEndianBytes<uint32_t>(0x01020304));
  EXPECT_EQ(
      std::string("\x01\x00\x00\x00\x00\x00\x00\x00", 8),
      littleEndianBytes<int64_t>(1));
  EXPECT_EQ(std::string(1, '\x2a'), littleEndianBytes<uint8_t>(0x2a));
  EXPECT_EQ(
      0x01020304u,
      littleEndianValue<uint32_t>(std::string("\x04\x03\x02\x01", 4)));
  // The zlib-polynomial check value of the reference string.
  EXPECT_EQ(0xCBF43926u, checkpointCrc32("123456789"));
}

// One entry is three length-prefixed frames; the reader hands out
// zero-copy views into the stream, positioned right after each prefix.
TEST_F(CheckpointFormatTest, entryFramesRoundTrip) {
  std::string bytes;
  CheckpointWriter writer(bytes);
  writeEntryFrames(writer, "ns", "key", "payload");
  ASSERT_EQ(24, bytes.size()); // 4+2, 4+3, 4+7

  CheckpointReader reader(bytes.data(), bytes.size());
  const auto frames = readEntryFrames(reader);
  EXPECT_EQ("ns", frames.ns);
  EXPECT_EQ("key", frames.key);
  EXPECT_EQ("payload", frames.payload);
  EXPECT_EQ(bytes.data() + 4, frames.ns.data());
  EXPECT_EQ(bytes.data() + 10, frames.key.data());
  EXPECT_EQ(bytes.data() + 17, frames.payload.data());
  EXPECT_TRUE(reader.atEnd());
}

// A timestamp value travels as its raw (seconds, nanos) pair, 12
// little-endian bytes, so nanosecond precision round-trips.
TEST_F(CheckpointFormatTest, timestampValueEncoding) {
  ValueSerializer<Timestamp> serializer;
  EXPECT_EQ("timestamp(raw:i64,i32)", serializer.schemaId());

  const Timestamp value(-2, 123456789);
  const auto bytes = serializer.serialize(value);
  ASSERT_EQ(12, bytes.size());
  EXPECT_EQ(
      littleEndianBytes<int64_t>(-2) + littleEndianBytes<uint32_t>(123456789),
      bytes);

  const auto restored = serializer.deserialize(bytes);
  EXPECT_EQ(value.getSeconds(), restored.getSeconds());
  EXPECT_EQ(123456789u, restored.getNanos());
}

// A full round trip of nanosecond timestamps through a list state and a
// nullable shared value: precision survives on both sides of the checkpoint
// and a null shared value round-trips through its flag byte.
TEST_F(CheckpointFormatTest, roundTripTimestampNanosAndSharedNull) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysA = probeKeys(selectorA, {17, 34, 51});
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto tsList = backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<Timestamp>(
          "ts-list", std::make_shared<ValueSerializer<Timestamp>>(), pool()));
  auto tsValue = backendA.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<Timestamp>>(
          "ts-value",
          std::make_shared<SharedPtrSerializer<Timestamp>>(
              std::make_shared<ValueSerializer<Timestamp>>()),
          pool()));

  const VoidNamespace ns;
  tsList->add(keysA[0], ns, Timestamp(1700000000, 123456789));
  tsList->add(keysA[0], ns, Timestamp(-1, 999999999));
  tsList->add(keysA[1], ns, Timestamp(0, 1));
  tsValue->update(keysA[0], ns, std::make_shared<Timestamp>(1700000000, 42));
  tsValue->update(keysA[2], ns, nullptr);

  const auto bytes = backendA.snapshot();

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysB = probeKeys(selectorB, {17, 34, 51});
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto tsListB = backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<Timestamp>(
          "ts-list", std::make_shared<ValueSerializer<Timestamp>>(), pool()));
  auto tsValueB = backendB.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<Timestamp>>(
          "ts-value",
          std::make_shared<SharedPtrSerializer<Timestamp>>(
              std::make_shared<ValueSerializer<Timestamp>>()),
          pool()));
  backendB.restore(bytes);

  const auto restoredList = tsListB->get(keysB[0], ns);
  ASSERT_EQ(2, restoredList.size());
  EXPECT_EQ(1700000000, restoredList[0].getSeconds());
  EXPECT_EQ(123456789u, restoredList[0].getNanos());
  EXPECT_EQ(-1, restoredList[1].getSeconds());
  EXPECT_EQ(999999999u, restoredList[1].getNanos());
  ASSERT_EQ(1, tsListB->get(keysB[1], ns).size());
  EXPECT_EQ(1u, tsListB->get(keysB[1], ns)[0].getNanos());
  EXPECT_TRUE(tsListB->get(keysB[2], ns).empty());

  const auto restoredValue = tsValueB->value(keysB[0], ns);
  ASSERT_NE(nullptr, restoredValue);
  EXPECT_EQ(1700000000, restoredValue->getSeconds());
  EXPECT_EQ(42u, restoredValue->getNanos());
  EXPECT_EQ(nullptr, tsValueB->value(keysB[1], ns));
  EXPECT_EQ(nullptr, tsValueB->value(keysB[2], ns));
}

// A stream written by hand, byte by byte, as another implementation of the
// format would write it: header without a CRC trailer, the schema table,
// and one group block carrying one list entry {7, 9} under the BIGINT key
// 42. Restoring it proves the layout — not just the writer's own calls — is
// what the reader consumes.
TEST_F(CheckpointFormatTest, crossProducerStreamRestores) {
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  auto key = probeKeys(selector, {42})[0];
  const auto group = key.keyGroup();

  HeapKeyedStateBackend<RowContainerStateKey> backend(
      selector.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto list = backend.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));

  std::string bytes;
  bytes.append("VSKV");
  appendU8(bytes, kCheckpointFormatVersion);
  appendU8(bytes, kCheckpointFlagLittleEndian);
  appendU32(bytes, kMaxParallelism);
  appendU32(bytes, group);
  appendU32(bytes, 1);
  appendBigIntListSchemaTable(bytes);
  appendU32(bytes, group);
  appendBytes(bytes, "list");
  appendI32(bytes, 1); // entry count
  // Entry frames: the empty namespace, the key (a column component: null
  // flag + raw value), the payload (element count + length-prefixed
  // elements).
  appendBytes(bytes, "");
  std::string keyBytes;
  appendBytes(
      keyBytes, std::string(1, '\x01') + littleEndianBytes<int64_t>(42));
  appendBytes(bytes, keyBytes);
  std::string payload;
  appendI32(payload, 2);
  appendBytes(payload, littleEndianBytes<int64_t>(7));
  appendBytes(payload, littleEndianBytes<int64_t>(9));
  appendBytes(bytes, payload);

  backend.restore(bytes);
  EXPECT_EQ((std::vector<int64_t>{7, 9}), list->get(key, VoidNamespace()));
}

// A flipped byte anywhere before the trailer, or in the trailer itself,
// fails the CRC check before any state is restored.
TEST_F(CheckpointFormatTest, restoreRejectsCrcMismatch) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysA = probeKeys(selectorA, {42});
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto listA = backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  listA->add(keysA[0], VoidNamespace(), 7);
  const auto bytes = backendA.snapshot();
  ASSERT_GT(bytes.size(), kCheckpointHeaderSize + 4);

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  auto makeBackendB = [&] {
    HeapKeyedStateBackend<RowContainerStateKey> backend(
        selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
    backend.getOrCreateListState<VoidNamespace>(
        ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
    return backend;
  };

  std::string flippedBody = bytes;
  flippedBody[flippedBody.size() / 2] ^= '\x40';
  EXPECT_THROW(makeBackendB().restore(flippedBody), VeloxException);

  std::string flippedTrailer = bytes;
  flippedTrailer.back() ^= '\x01';
  EXPECT_THROW(makeBackendB().restore(flippedTrailer), VeloxException);
}

// A stream whose flags announce no trailer restores without one: a storage
// layer that verifies integrity itself may strip both the flag and the
// four trailer bytes.
TEST_F(CheckpointFormatTest, restoreWorksWithoutCrcTrailer) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysA = probeKeys(selectorA, {42});
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto listA = backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  listA->add(keysA[0], VoidNamespace(), 7);
  const auto withTrailer = backendA.snapshot();
  ASSERT_EQ(
      kCheckpointFlagLittleEndian | kCheckpointFlagCrcTrailer,
      static_cast<uint8_t>(withTrailer[5]));

  auto stripped = withTrailer.substr(0, withTrailer.size() - 4);
  stripped[5] = static_cast<char>(kCheckpointFlagLittleEndian);

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  auto keysB = probeKeys(selectorB, {42});
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto listB = backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  backendB.restore(stripped);
  EXPECT_EQ((std::vector<int64_t>{7}), listB->get(keysB[0], VoidNamespace()));
}

// Registering the same state id under a different value schema fails the
// schema-table cross-check: the ids are the checkpoint's contract.
TEST_F(CheckpointFormatTest, restoreRejectsSchemaChange) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  const auto bytes = backendA.snapshot();

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backendB.getOrCreateValueState<VoidNamespace>(
      ValueStateDescriptor<std::shared_ptr<int64_t>>(
          "list",
          std::make_shared<SharedPtrSerializer<int64_t>>(
              std::make_shared<ValueSerializer<int64_t>>()),
          pool()));
  EXPECT_THROW(backendB.restore(bytes), VeloxException);
}

// maxParallelism defines the key-group space and is stable across rescale:
// a checkpoint from a different space is rejected by equality, not by the
// range overlap check.
TEST_F(CheckpointFormatTest, restoreRejectsMaxParallelismChange) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  const auto bytes = backendA.snapshot();

  KeySelector selectorB({0}, {BIGINT()}, 64, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), 64, 0, 64);
  backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  EXPECT_THROW(backendB.restore(bytes), VeloxException);
}

// The body's group ids must ascend from the header's startKeyGroup; a
// stream that starts elsewhere or goes back is corruption.
TEST_F(CheckpointFormatTest, restoreRejectsNonAscendingGroups) {
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      selector.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backend.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));

  // Header range [0, 2) but the body leads with group 1: the reader
  // expects group 0 first and fails.
  std::string bytes;
  bytes.append("VSKV");
  appendU8(bytes, kCheckpointFormatVersion);
  appendU8(bytes, kCheckpointFlagLittleEndian);
  appendU32(bytes, kMaxParallelism);
  appendU32(bytes, 0);
  appendU32(bytes, 2);
  appendBigIntListSchemaTable(bytes);
  appendU32(bytes, 1); // first group id: 1, not 0
  appendBytes(bytes, "list");
  appendI32(bytes, 0);
  appendU32(bytes, 0);
  appendBytes(bytes, "list");
  appendI32(bytes, 0);
  EXPECT_THROW(backend.restore(bytes), VeloxException);
}

// A backend with no registered states checkpoints to a bare stream —
// header, empty schema table, empty group blocks — and restores into
// another empty backend byte-identically.
TEST_F(CheckpointFormatTest, emptyBackendRoundTrip) {
  KeySelector selectorA({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  const auto bytes = backendA.snapshot();
  // Header, the "(BIGINT)" key schema id, a zero state count, one group id
  // per key group, the trailer.
  ASSERT_EQ(
      kCheckpointHeaderSize + 4 + std::string("(BIGINT)").size() + 4 +
          4 * kMaxParallelism + 4,
      bytes.size());
  const std::string expectedTrailer = littleEndianBytes(
      checkpointCrc32(bytes.substr(0, bytes.size() - sizeof(uint32_t))));
  EXPECT_EQ(0, bytes.compare(bytes.size() - 4, 4, expectedTrailer));

  KeySelector selectorB({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backendB.restore(bytes);
  EXPECT_EQ(bytes, backendB.snapshot());
}

// Streams that are not keyed-state checkpoints — wrong magic, wrong
// version — fail at the header, before anything is read.
TEST_F(CheckpointFormatTest, restoreRejectsForeignMagicAndVersion) {
  KeySelector selector({0}, {BIGINT()}, kMaxParallelism, pool());
  HeapKeyedStateBackend<RowContainerStateKey> backend(
      selector.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  backend.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  const auto bytes = backend.snapshot();

  std::string badMagic = bytes;
  badMagic[0] = 'X';
  EXPECT_THROW(backend.restore(badMagic), VeloxException);

  std::string badVersion = bytes;
  badVersion[4] = static_cast<char>(kCheckpointFormatVersion + 1);
  EXPECT_THROW(backend.restore(badVersion), VeloxException);
}

// A TIMESTAMP key column round-trips with its nanoseconds intact, through
// the same raw little-endian (seconds, nanos) encoding the value path
// carries: one portable Timestamp encoding on both paths.
TEST_F(CheckpointFormatTest, timestampKeyColumnRoundTrip) {
  KeySelector selectorA({0}, {TIMESTAMP()}, kMaxParallelism, pool());
  selectorA.probe(makeRowVector(
      {makeNullableFlatVector<Timestamp>({Timestamp(1700000000, 123456789)})}));
  auto keysA = std::vector<RowContainerStateKey>(
      selectorA.keys().begin(), selectorA.keys().end());
  ASSERT_EQ(1, keysA.size());
  HeapKeyedStateBackend<RowContainerStateKey> backendA(
      selectorA.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto listA = backendA.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  listA->add(keysA[0], VoidNamespace(), 7);
  const auto bytes = backendA.snapshot();

  KeySelector selectorB({0}, {TIMESTAMP()}, kMaxParallelism, pool());
  selectorB.probe(makeRowVector(
      {makeNullableFlatVector<Timestamp>({Timestamp(1700000000, 123456789)})}));
  auto keysB = std::vector<RowContainerStateKey>(
      selectorB.keys().begin(), selectorB.keys().end());
  HeapKeyedStateBackend<RowContainerStateKey> backendB(
      selectorB.keySerializer(), kMaxParallelism, 0, kMaxParallelism);
  auto listB = backendB.getOrCreateListState<VoidNamespace>(
      ListStateDescriptor<int64_t>("list", int64Serializer(), pool()));
  backendB.restore(bytes);

  // The restored key resolves to the row of the freshly probed equal key:
  // the nanosecond-precise identity survived the checkpoint.
  EXPECT_EQ((std::vector<int64_t>{7}), listB->get(keysB[0], VoidNamespace()));
}

} // namespace
} // namespace facebook::velox::stateful
