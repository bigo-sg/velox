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

#include "NexmarkConnectorTestBase.h"

namespace facebook::velox::connector::nexmark::test {

class NexmarkGeneratorRandomTest : public NexmarkConnectorTestBase {};

TEST_F(NexmarkGeneratorRandomTest, IndependentStreamsForSameConfig) {
  constexpr int64_t kMaxEvents = 1000;
  auto gen1 = makeNexmarkGenerator(kMaxEvents);
  auto gen2 = makeNexmarkGenerator(kMaxEvents);

  int checked = 0;
  int differing = 0;
  constexpr int kSamples = 100;
  while (gen1->hasNext() && gen2->hasNext() && checked < kSamples) {
    auto e1 = gen1->next().getEvent().toString();
    auto e2 = gen2->next().getEvent().toString();
    if (e1 != e2) {
      ++differing;
    }
    ++checked;
  }
  ASSERT_EQ(checked, kSamples);
  ASSERT_GT(differing, 0);
}

} // namespace facebook::velox::connector::nexmark::test
