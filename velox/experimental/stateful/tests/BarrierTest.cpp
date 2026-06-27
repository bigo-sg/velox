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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/StreamElement.h"

namespace facebook::velox::stateful::test {
namespace {

using facebook::velox::exec::test::OperatorTestBase;

class BarrierElementTest : public OperatorTestBase {};

// Verifies that Barrier carries the correct checkpointId and reports
// isBarrier() == true while isRecord()/isWatermark() are false.
TEST_F(BarrierElementTest, basicProperties) {
  Barrier barrier("node-1", 42);
  ASSERT_TRUE(barrier.isBarrier());
  ASSERT_FALSE(barrier.isRecord());
  ASSERT_FALSE(barrier.isWatermark());
  ASSERT_EQ(42, barrier.checkpointId());
  ASSERT_EQ("node-1", barrier.nodeId());
}

// Verifies that StreamRecord reports isBarrier() == false.
TEST_F(BarrierElementTest, streamRecordIsNotBarrier) {
  auto pool = memory::memoryManager()->addRootPool();
  auto rv = std::make_shared<RowVector>(
      pool.get(), ROW({"x"}, {BIGINT()}), nullptr, 1, std::vector<VectorPtr>{});
  StreamRecord record("node-1", rv);
  ASSERT_FALSE(record.isBarrier());
  ASSERT_TRUE(record.isRecord());
}

// Verifies that Watermark reports isBarrier() == false.
TEST_F(BarrierElementTest, watermarkIsNotBarrier) {
  Watermark wm("node-1", 1000);
  ASSERT_FALSE(wm.isBarrier());
  ASSERT_TRUE(wm.isWatermark());
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
