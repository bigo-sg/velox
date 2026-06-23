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

#include "velox/connectors/print/PrintSink.h"

namespace facebook::velox::connector::print::test {

TEST(PrintSinkTest, PrefixNoIdentifierSingleParallelism) {
  EXPECT_EQ("", PrintSink::computePrefix("", 1, 0));
}

TEST(PrintSinkTest, PrefixNoIdentifierMultiParallelism) {
  EXPECT_EQ("1> ", PrintSink::computePrefix("", 2, 0));
  EXPECT_EQ("2> ", PrintSink::computePrefix("", 2, 1));
}

TEST(PrintSinkTest, PrefixWithIdentifierSingleParallelism) {
  EXPECT_EQ("foo> ", PrintSink::computePrefix("foo", 1, 0));
}

TEST(PrintSinkTest, PrefixWithIdentifierMultiParallelism) {
  EXPECT_EQ("foo:1> ", PrintSink::computePrefix("foo", 2, 0));
  EXPECT_EQ("foo:2> ", PrintSink::computePrefix("foo", 2, 1));
}

} // namespace facebook::velox::connector::print::test
