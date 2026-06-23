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

#include "velox/connectors/print/PrintTableHandle.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::print::test {

class PrintTableHandleTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Type::registerSerDe();
    PrintTableHandle::registerSerDe();
  }

  static RowTypePtr sampleRowType() {
    return ROW({{"a", BIGINT()}, {"b", VARCHAR()}});
  }
};

// Discriminator key is required by velox4j's PolymorphicDeserializer.
TEST_F(PrintTableHandleTest, SerializeIncludesDiscriminator) {
  PrintTableHandle handle("t", sampleRowType(), "foo", true);
  auto obj = handle.serialize();
  ASSERT_EQ(obj["name"].asString(), "PrintTableHandle");
  ASSERT_EQ(obj["tableName"].asString(), "t");
  ASSERT_EQ(obj["printIdentifier"].asString(), "foo");
  ASSERT_TRUE(obj["isStdErr"].asBool());
  ASSERT_TRUE(obj.count("dataColumns") > 0);
}

TEST_F(PrintTableHandleTest, RoundTripWithAllFields) {
  PrintTableHandle handle("t", sampleRowType(), "foo", true);
  auto obj = handle.serialize();
  auto clone = ISerializable::deserialize<PrintTableHandle>(obj);
  ASSERT_EQ(clone->tableName(), "t");
  ASSERT_EQ(clone->printIdentifier(), "foo");
  ASSERT_TRUE(clone->isStdErr());
  ASSERT_EQ(clone->dataColumns()->toString(), sampleRowType()->toString());
  ASSERT_EQ(clone->toString(), handle.toString());
}

// Optional fields fall back to defaults when JSON omits them.
TEST_F(PrintTableHandleTest, RoundTripWithMissingOptionalFields) {
  PrintTableHandle handle("t", sampleRowType(), "", false);
  auto obj = handle.serialize();
  obj.erase("printIdentifier");
  obj.erase("isStdErr");
  auto clone = ISerializable::deserialize<PrintTableHandle>(obj);
  ASSERT_TRUE(clone->printIdentifier().empty());
  ASSERT_FALSE(clone->isStdErr());
}

} // namespace facebook::velox::connector::print::test
