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
#include "velox/core/Expressions.h"
#include "velox/experimental/stateful/udf/Register.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::udf::test {

class UDFTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    // stateful::udf::registerFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }

  // Build expression tree directly since 'extract' is a SQL keyword
  // and cannot be parsed by DuckDB as a function call.
  core::TypedExprPtr makeExtractExpr(
      const std::string& field,
      const TypePtr& timestampType) {
    return std::make_shared<core::CallTypedExpr>(
        BIGINT(),
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::ConstantTypedExpr>(
                VARCHAR(), variant(field)),
            std::make_shared<core::FieldAccessTypedExpr>(timestampType, "c0")},
        "extract");
  }

  std::optional<int64_t> extractField(
      const std::string& field,
      std::optional<Timestamp> timestamp) {
    auto rowVector = makeRowVector({
        makeNullableFlatVector<Timestamp>({timestamp}, TIMESTAMP()),
    });
    auto expr = makeExtractExpr(field, TIMESTAMP());
    auto result = evaluate(expr, rowVector);
    auto* flatResult = result->asFlatVector<int64_t>();
    if (flatResult->isNullAt(0)) {
      return std::nullopt;
    }
    return flatResult->valueAt(0);
  }
};

// 2026-06-08 09:40:30 UTC (Monday)
static constexpr int64_t kMondayTs = 1780911630;

// 2026-06-07 00:00:00 UTC (Sunday)
static constexpr int64_t kSundayTs = 1780790400;

TEST_F(UDFTest, extractAllFields) {
  auto ts = Timestamp(kMondayTs, 0);
  stateful::udf::registerFunctions("");

  struct Case {
    std::string lower;
    std::string upper;
    std::string mixed;
    int64_t expected;
  };

  const std::vector<Case> fields = {
      {"year", "YEAR", "Year", 2026},
      {"month", "MONTH", "Month", 6},
      {"day", "DAY", "Day", 8},
      {"day_of_month", "DAY_OF_MONTH", "Day_Of_Month", 8},
      {"hour", "HOUR", "Hour", 9},
      {"minute", "MINUTE", "Minute", 40},
      {"second", "SECOND", "Second", 30},
      {"day_of_week", "DAY_OF_WEEK", "Day_Of_Week", 1},
      {"dow", "DOW", "Dow", 1},
      {"day_of_year", "DAY_OF_YEAR", "Day_Of_Year", 159},
      {"doy", "DOY", "Doy", 159},
  };

  for (const auto& f : fields) {
    EXPECT_EQ(f.expected, extractField(f.lower, ts).value())
        << "field: " << f.lower;
    EXPECT_EQ(f.expected, extractField(f.upper, ts).value())
        << "field: " << f.upper;
    EXPECT_EQ(f.expected, extractField(f.mixed, ts).value())
        << "field: " << f.mixed;
  }

  // Sunday: tm_wday=0 returns 7
  auto sunday = Timestamp(kSundayTs, 0);
  for (const auto& name :
       {"day_of_week", "DAY_OF_WEEK", "Day_Of_Week", "dow", "DOW", "Dow"}) {
    EXPECT_EQ(7, extractField(name, sunday).value()) << "field: " << name;
  }
}

TEST_F(UDFTest, extractUnknownFieldReturnsNull) {
  auto ts = Timestamp(kMondayTs, 0);
  stateful::udf::registerFunctions("");

  EXPECT_FALSE(extractField("unknown_field", ts).has_value());
  EXPECT_FALSE(extractField("UNKNOWN", ts).has_value());
}

TEST_F(UDFTest, splitIndex) {
  const auto splitIndex = [&](const std::optional<std::string>& a,
                              const std::optional<std::string>& d,
                              const std::optional<int64_t>& i) {
    return evaluateOnce<std::string>("split_index(c0, c1, c2)", a, d, i);
  };
  EXPECT_EQ(splitIndex("a/b/c", "/", 1), "a");
  EXPECT_EQ(splitIndex("a/b/c", "/", 2), "b");
  const std::optional<std::string> res0 = splitIndex("a/b/c", "/", 0);
  const std::optional<std::string> res1 = splitIndex("a/b/c", "/", -1);
  const std::optional<std::string> res2 = splitIndex("a/b/c", "/", 4);
  EXPECT_EQ(res0.has_value(), false);
  EXPECT_EQ(res1.has_value(), false);
  EXPECT_EQ(res2.has_value(), false);
}

} // namespace facebook::velox::udf::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true); // Parse gflags
  return RUN_ALL_TESTS();
}
