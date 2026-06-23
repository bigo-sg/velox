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

#include <cstdint>

#include <gtest/gtest.h>

#include "velox/connectors/nexmark/NexmarkUtils.h"

namespace facebook::velox::connector::nexmark::test {

TEST(NexmarkUtilsTest, FormatDateTimeZeroPadding) {
  // Base: 1749513600000 ms = 2025-06-10T00:00:00.000Z
  EXPECT_EQ(formatDateTime(1749513600007), "2025-06-10T00:00:00.007Z");
  EXPECT_EQ(formatDateTime(1749513600077), "2025-06-10T00:00:00.077Z");
  EXPECT_EQ(formatDateTime(1749513600526), "2025-06-10T00:00:00.526Z");
  EXPECT_EQ(formatDateTime(1749513600000), "2025-06-10T00:00:00.000Z");
}

TEST(NexmarkUtilsTest, ComputeNexmarkSeedIsDeterministic) {
  EXPECT_EQ(
      computeNexmarkSeed(/*firstEventId=*/1, /*maxEvents=*/1000, /*firstEventNumber=*/0),
      computeNexmarkSeed(/*firstEventId=*/1, /*maxEvents=*/1000, /*firstEventNumber=*/0));
}

TEST(NexmarkUtilsTest, ComputeNexmarkSeedIsSensitiveToFirstEventId) {
  EXPECT_NE(
      computeNexmarkSeed(/*firstEventId=*/1, /*maxEvents=*/1000, /*firstEventNumber=*/0),
      computeNexmarkSeed(/*firstEventId=*/2, /*maxEvents=*/1000, /*firstEventNumber=*/0));
}

TEST(NexmarkUtilsTest, ComputeNexmarkSeedIsSensitiveToMaxEvents) {
  EXPECT_NE(
      computeNexmarkSeed(/*firstEventId=*/1, /*maxEvents=*/1000, /*firstEventNumber=*/0),
      computeNexmarkSeed(/*firstEventId=*/1, /*maxEvents=*/2000, /*firstEventNumber=*/0));
}

TEST(NexmarkUtilsTest, ComputeNexmarkSeedIsSensitiveToFirstEventNumber) {
  EXPECT_NE(
      computeNexmarkSeed(/*firstEventId=*/1, /*maxEvents=*/1000, /*firstEventNumber=*/0),
      computeNexmarkSeed(/*firstEventId=*/1, /*maxEvents=*/1000, /*firstEventNumber=*/1));
}

} // namespace facebook::velox::connector::nexmark::test
