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

#include "velox/experimental/stateful/CombinedWatermarkStatus.h"

#include <gtest/gtest.h>

namespace facebook::velox::stateful::test {
namespace {

TEST(CombinedWatermarkStatusTest, activeInputsUseMinimumWatermark) {
  CombinedWatermarkStatus status(2);

  EXPECT_FALSE(status.isIdle());

  EXPECT_FALSE(status.updateWatermark(0, 100));
  EXPECT_FALSE(status.isIdle());
  EXPECT_EQ(status.getCombinedWatermark(), INT64_MIN);

  EXPECT_TRUE(status.updateWatermark(1, 90));
  EXPECT_EQ(status.getCombinedWatermark(), 90);

  EXPECT_TRUE(status.updateWatermark(1, 110));
  EXPECT_EQ(status.getCombinedWatermark(), 100);

  EXPECT_TRUE(status.updateWatermark(0, 120));
  EXPECT_EQ(status.getCombinedWatermark(), 110);
}

TEST(CombinedWatermarkStatusTest, idleInputsAreExcludedFromMinimumWatermark) {
  CombinedWatermarkStatus status(2);

  EXPECT_FALSE(status.updateWatermark(0, 100));
  EXPECT_TRUE(status.updateWatermark(1, 50));
  EXPECT_EQ(status.getCombinedWatermark(), 50);

  EXPECT_TRUE(status.updateStatus(1, true));
  EXPECT_FALSE(status.isIdle());
  EXPECT_EQ(status.getCombinedWatermark(), 100);

  EXPECT_FALSE(status.updateStatus(0, true));
  EXPECT_TRUE(status.isIdle());
  EXPECT_EQ(status.getCombinedWatermark(), 100);
}

TEST(
    CombinedWatermarkStatusTest,
    watermarkReactivatesIdleInputWithoutRollback) {
  CombinedWatermarkStatus status(1);

  EXPECT_TRUE(status.updateWatermark(0, 100));
  EXPECT_EQ(status.getCombinedWatermark(), 100);

  EXPECT_FALSE(status.updateStatus(0, true));
  EXPECT_TRUE(status.isIdle());

  EXPECT_FALSE(status.updateWatermark(0, 100));
  EXPECT_FALSE(status.isIdle());
  EXPECT_EQ(status.getCombinedWatermark(), 100);

  EXPECT_FALSE(status.updateStatus(0, true));
  EXPECT_TRUE(status.isIdle());

  EXPECT_FALSE(status.updateWatermark(0, 90));
  EXPECT_FALSE(status.isIdle());
  EXPECT_EQ(status.getCombinedWatermark(), 100);
}

TEST(CombinedWatermarkStatusTest, activeStatusRejoinsWithPreviousWatermark) {
  CombinedWatermarkStatus status(2);

  EXPECT_FALSE(status.updateWatermark(0, 100));
  EXPECT_TRUE(status.updateWatermark(1, 50));
  EXPECT_EQ(status.getCombinedWatermark(), 50);

  EXPECT_TRUE(status.updateStatus(1, true));
  EXPECT_EQ(status.getCombinedWatermark(), 100);

  EXPECT_TRUE(status.updateWatermark(0, 120));
  EXPECT_EQ(status.getCombinedWatermark(), 120);

  EXPECT_FALSE(status.updateStatus(1, false));
  EXPECT_FALSE(status.isIdle());
  EXPECT_EQ(status.getCombinedWatermark(), 120);

  EXPECT_FALSE(status.updateWatermark(1, 130));
  EXPECT_EQ(status.getCombinedWatermark(), 120);

  EXPECT_TRUE(status.updateWatermark(0, 140));
  EXPECT_EQ(status.getCombinedWatermark(), 130);
}

} // namespace
} // namespace facebook::velox::stateful::test
