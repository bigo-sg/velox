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

#include "velox/experimental/stateful/StreamElement.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/type/Type.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/VectorEncoding.h"

namespace facebook::velox::stateful::test {
namespace {

class StreamRecordTest : public exec::test::OperatorTestBase {
 protected:
  RowVectorPtr plainBatch() {
    return makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  }

  RowVectorPtr mergedBatch() {
    auto value = makeFlatVector<int64_t>({10, 20, 30, 40});
    auto rowKind = makeFlatVector<int8_t>({0, 1, 2, 3});
    return makeRowVector({"c", "$row_kind"}, {value, rowKind});
  }

  RowVectorPtr makeMergedWithNulls(BufferPtr nulls) {
    auto value = makeFlatVector<int64_t>({10, 20, 30, 40});
    auto rowKind = makeFlatVector<int8_t>({0, 1, 2, 3});
    return std::make_shared<RowVector>(
        pool(),
        ROW({"c", "$row_kind"}, {BIGINT(), TINYINT()}),
        nulls,
        4,
        std::vector<VectorPtr>{value, rowKind});
  }

  BufferPtr makeNulls(std::vector<vector_size_t> nullRows) {
    constexpr vector_size_t kSize = 4;
    auto nulls = AlignedBuffer::allocate<uint64_t>(
        bits::nwords(kSize), pool(), bits::kNotNull64);
    for (auto idx : nullRows) {
      bits::setNull(nulls->asMutable<uint64_t>(), idx);
    }
    return nulls;
  }
};

TEST_F(StreamRecordTest, toMergedRowVectorAppendOnlyReturnsRecordAsIs) {
  auto batch = plainBatch();
  auto record = std::make_shared<StreamRecord>("source", batch);
  auto merged = record->toMergedRowVector(false);
  ASSERT_EQ(merged->type()->size(), 1);
  EXPECT_EQ(merged.get(), batch.get());
}

TEST_F(
    StreamRecordTest,
    toMergedRowVectorAlwaysAppendSynthesizesConstantInsert) {
  auto batch = plainBatch();
  auto record = std::make_shared<StreamRecord>("source", batch);
  auto merged = record->toMergedRowVector(true);
  auto mergedRowType = asRowType(merged->type());
  ASSERT_EQ(mergedRowType->size(), 2);
  EXPECT_EQ(mergedRowType->nameOf(1), "$row_kind");
  auto kindVec = merged->childAt(1);
  EXPECT_EQ(kindVec->type()->kind(), TypeKind::TINYINT);
  ASSERT_EQ(kindVec->encoding(), VectorEncoding::Simple::CONSTANT);
  auto constantKind =
      std::dynamic_pointer_cast<ConstantVector<int8_t>>(kindVec);
  ASSERT_NE(constantKind, nullptr);
  EXPECT_EQ(constantKind->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(constantKind->size(), batch->size());
}

TEST_F(StreamRecordTest, toMergedRowVectorChangelogAppendsRowKindColumn) {
  auto record = StreamRecord::create("source", mergedBatch());
  ASSERT_FALSE(record->appendOnly());
  ASSERT_NE(record->rowKind(), nullptr);

  auto merged = record->toMergedRowVector();
  auto mergedRowType = asRowType(merged->type());
  ASSERT_EQ(mergedRowType->size(), 2);
  EXPECT_EQ(mergedRowType->nameOf(1), "$row_kind");
  auto kindVec =
      std::dynamic_pointer_cast<SimpleVector<int8_t>>(merged->childAt(1));
  ASSERT_NE(kindVec, nullptr);
  EXPECT_EQ(kindVec->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(kindVec->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(kindVec->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  EXPECT_EQ(kindVec->valueAt(3), static_cast<int8_t>(RowKind::DELETE));
}

TEST_F(StreamRecordTest, createNormalizesConstantInsertToAppendOnly) {
  auto value = makeFlatVector<int64_t>({1, 2, 3});
  auto constantInsert =
      makeConstant<int8_t>(static_cast<int8_t>(RowKind::INSERT), 3, TINYINT());
  auto merged = makeRowVector({"c", "$row_kind"}, {value, constantInsert});

  auto record = StreamRecord::create("source", merged);
  EXPECT_TRUE(record->appendOnly());
  EXPECT_EQ(record->rowKind(), nullptr);
  EXPECT_EQ(record->record()->type()->size(), 1);
}

TEST_F(StreamRecordTest, createRejectsRowKindSizeMismatch) {
  auto value = makeFlatVector<int64_t>({1, 2, 3});
  auto rowKind = makeFlatVector<int8_t>({0, 1, 2, 3});
  auto merged = makeRowVector({"c", "$row_kind"}, {value, rowKind});

  VELOX_ASSERT_USER_THROW(
      StreamRecord::create("source", merged), "$row_kind column length");
}

TEST_F(StreamRecordTest, createRejectsNonTinyIntRowKindColumn) {
  auto value = makeFlatVector<int64_t>({1, 2, 3});
  auto rowKind = makeFlatVector<int32_t>({0, 1, 2});
  auto merged = makeRowVector({"c", "$row_kind"}, {value, rowKind});

  VELOX_ASSERT_USER_THROW(
      StreamRecord::create("source", merged),
      "$row_kind column must be TINYINT");
}

TEST_F(StreamRecordTest, createRejectsNullRowKindColumn) {
  auto value = makeFlatVector<int64_t>({1, 2, 3});
  auto rowKind = makeNullableFlatVector<int8_t>({0, std::nullopt, 2});
  auto merged = makeRowVector({"c", "$row_kind"}, {value, rowKind});

  VELOX_ASSERT_USER_THROW(
      StreamRecord::create("source", merged),
      "$row_kind column must not contain nulls");
}

TEST_F(StreamRecordTest, createPreservesTopLevelNulls) {
  auto nulls = makeNulls({1, 3});
  auto merged = makeMergedWithNulls(nulls);

  auto record = StreamRecord::create("source", merged);
  EXPECT_EQ(record->record()->nulls(), nulls);
  EXPECT_FALSE(record->record()->isNullAt(0));
  EXPECT_TRUE(record->record()->isNullAt(1));
  EXPECT_FALSE(record->record()->isNullAt(2));
  EXPECT_TRUE(record->record()->isNullAt(3));
}

TEST_F(StreamRecordTest, toMergedRowVectorPreservesTopLevelNulls) {
  auto nulls = makeNulls({0, 2});
  auto record = StreamRecord::create("source", makeMergedWithNulls(nulls));
  ASSERT_FALSE(record->appendOnly());

  auto merged = record->toMergedRowVector();
  EXPECT_EQ(merged->nulls(), nulls);
  EXPECT_TRUE(merged->isNullAt(0));
  EXPECT_FALSE(merged->isNullAt(1));
  EXPECT_TRUE(merged->isNullAt(2));
  EXPECT_FALSE(merged->isNullAt(3));
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
