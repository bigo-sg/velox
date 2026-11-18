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
#include "velox/connectors/from_elements/FromElementsSource.h"

#include <gtest/gtest.h>
#include "velox/common/config/Config.h"
#include "velox/connectors/Connector.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::connector::from_elements::test {

class FromElementsSourceTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("from_elements_test");
  }

  std::unique_ptr<ConnectorQueryCtx> makeQueryCtx(
      const std::string& sessionTimezone) {
    sessionProperties_ = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    return std::make_unique<ConnectorQueryCtx>(
        pool_.get(),
        pool_.get(),
        sessionProperties_.get(),
        nullptr /* spillConfig */,
        common::PrefixSortConfig{},
        nullptr /* expressionEvaluator */,
        nullptr /* cache */,
        "query",
        "task",
        "plan-node",
        0,
        sessionTimezone);
  }

  std::shared_ptr<RowVector> readAll(
      const RowTypePtr& outputType,
      const ConnectorQueryCtx* ctx,
      const std::vector<std::string>& rows) {
    FromElementsSource source(outputType, ctx, rows);
    velox::ContinueFuture future;
    auto batch = source.next(100, future);
    VELOX_CHECK(batch.has_value());
    return batch.value();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<const config::ConfigBase> sessionProperties_;
};

// Zone-free TIMESTAMP strings parse as wall clock: Flink's values-table
// storage convention is wall-as-UTC (TimestampData.fromLocalDateTime), so
// the parse must NOT fold wall time through the session timezone.
TEST_F(FromElementsSourceTest, ZoneFreeTimestampParsesAsWallClock) {
  const auto ctx = makeQueryCtx("Asia/Shanghai");
  auto data = readAll(
      ROW({"ts"}, {TIMESTAMP()}), ctx.get(), {"+I[2024-01-15T03:04:05]"});
  const auto& ts = data->childAt(0)->as<SimpleVector<Timestamp>>()->valueAt(0);
  // Wall clock 2024-01-15 03:04:05, not the Shanghai-folded instant
  // (2024-01-14T19:04:05Z).
  ASSERT_EQ(Timestamp::fromMillis(1705287845000), ts);
}

// Strings with an explicit offset keep their own offset regardless of the
// session timezone.
TEST_F(FromElementsSourceTest, ExplicitOffsetStringKeepsItsOffset) {
  const auto ctx = makeQueryCtx("Asia/Shanghai");
  auto data = readAll(
      ROW({"ts"}, {TIMESTAMP()}), ctx.get(), {"+I[2024-01-15T03:04:05+08:00]"});
  const auto& ts = data->childAt(0)->as<SimpleVector<Timestamp>>()->valueAt(0);
  ASSERT_EQ(Timestamp::fromMillis(1705259045000), ts);
}

// Non-timestamp columns are unaffected by the zone choice.
TEST_F(FromElementsSourceTest, VarcharAndIntParseUnaffected) {
  const auto ctx = makeQueryCtx("Asia/Shanghai");
  auto data = readAll(
      ROW({"s", "i"}, {VARCHAR(), INTEGER()}), ctx.get(), {"+I[hello, 42]"});
  ASSERT_EQ(
      "hello", data->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0));
  ASSERT_EQ(42, data->childAt(1)->as<SimpleVector<int32_t>>()->valueAt(0));
}

} // namespace facebook::velox::connector::from_elements::test
