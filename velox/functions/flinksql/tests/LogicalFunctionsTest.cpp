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
#include <optional>
#include <string>
#include <vector>

#include "velox/core/Expressions.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::functions::flinksql::test {

// Tests for the logical function group (NOT / IS FALSE / IS NOT TRUE /
// IS NOT FALSE). "not" and "is_*" are constructed directly as CallTypedExpr
// trees ("not" is a SQL keyword; nested combinations are built by nesting
// calls).
class LogicalFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    // This test process registers no prestosql/sparksql/stateful baselines,
    // so the flinksql registrations are free of overwrite interference.
    memory::MemoryManager::testingSetInstance({});
  }

  static core::TypedExprPtr boolField() {
    return std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c0");
  }

  static core::TypedExprPtr makeCall(
      const std::string& name,
      std::vector<core::TypedExprPtr> inputs) {
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(), std::move(inputs), name);
  }

  // Truth-table input: {true, false, NULL}.
  RowVectorPtr truthTableData() {
    return makeRowVector({makeNullableFlatVector<bool>(
        std::vector<std::optional<bool>>{true, false, std::nullopt})});
  }

  // All-NULL input.
  RowVectorPtr allNullData(vector_size_t size = 3) {
    return makeRowVector(
        {makeNullableFlatVector<bool>(std::vector<std::optional<bool>>(size))});
  }

  // Evaluates and returns the result as a SimpleVector<bool>. The result
  // vector stays owned by 'owner' for the lifetime of the caller's scope.
  SimpleVector<bool>* evaluateBool(
      core::TypedExprPtr expr,
      const RowVectorPtr& data,
      VectorPtr& owner) {
    owner = evaluate(expr, data);
    return owner->as<SimpleVector<bool>>();
  }
};

// L1: NOT three-valued truth table — {true,false,NULL} ->
// {false,true,NULL}, with the NULL row propagating.
TEST_F(LogicalFunctionsTest, notTruthTable) {
  flinksql::registerFunctions("");
  VectorPtr owner;
  auto* result =
      evaluateBool(makeCall("not", {boolField()}), truthTableData(), owner);
  ASSERT_EQ(result->size(), 3);
  EXPECT_FALSE(result->valueAt(0));
  EXPECT_TRUE(result->valueAt(1));
  EXPECT_TRUE(result->isNullAt(2));
}

// L2: IS_FALSE truth table — {true,false,NULL} -> {false,true,false}, never
// NULL.
TEST_F(LogicalFunctionsTest, isFalseTruthTable) {
  flinksql::registerFunctions("");
  VectorPtr owner;
  auto* result = evaluateBool(
      makeCall("is_false", {boolField()}), truthTableData(), owner);
  ASSERT_EQ(result->size(), 3);
  EXPECT_FALSE(result->valueAt(0));
  EXPECT_TRUE(result->valueAt(1));
  EXPECT_FALSE(result->valueAt(2));
  EXPECT_EQ(0, BaseVector::countNulls(result->nulls(), result->size()));
}

// L3: IS_NOT_TRUE truth table — {true,false,NULL} -> {false,true,true},
// never NULL.
TEST_F(LogicalFunctionsTest, isNotTrueTruthTable) {
  flinksql::registerFunctions("");
  VectorPtr owner;
  auto* result = evaluateBool(
      makeCall("is_not_true", {boolField()}), truthTableData(), owner);
  ASSERT_EQ(result->size(), 3);
  EXPECT_FALSE(result->valueAt(0));
  EXPECT_TRUE(result->valueAt(1));
  EXPECT_TRUE(result->valueAt(2));
  EXPECT_EQ(0, BaseVector::countNulls(result->nulls(), result->size()));
}

// L4: IS_NOT_FALSE truth table — {true,false,NULL} -> {true,false,true},
// never NULL.
TEST_F(LogicalFunctionsTest, isNotFalseTruthTable) {
  flinksql::registerFunctions("");
  VectorPtr owner;
  auto* result = evaluateBool(
      makeCall("is_not_false", {boolField()}), truthTableData(), owner);
  ASSERT_EQ(result->size(), 3);
  EXPECT_TRUE(result->valueAt(0));
  EXPECT_FALSE(result->valueAt(1));
  EXPECT_TRUE(result->valueAt(2));
  EXPECT_EQ(0, BaseVector::countNulls(result->nulls(), result->size()));
}

// L5: NOT over a constant-encoded input takes the constant path of the
// vector function (NotFunction contract): all rows are false.
TEST_F(LogicalFunctionsTest, notConstantEncoding) {
  flinksql::registerFunctions("");
  constexpr vector_size_t kSize = 5;
  auto data = makeRowVector({makeConstant<bool>(true, kSize)});
  VectorPtr owner;
  auto* result = evaluateBool(makeCall("not", {boolField()}), data, owner);
  ASSERT_EQ(result->size(), kSize);
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_FALSE(result->valueAt(i)) << "row: " << i;
    EXPECT_FALSE(result->isNullAt(i)) << "row: " << i;
  }
}

// L6: NOT over an all-NULL flat input is all-NULL (framework
// defaultNullBehavior path of the vector function).
TEST_F(LogicalFunctionsTest, notAllNull) {
  flinksql::registerFunctions("");
  VectorPtr owner;
  auto* result =
      evaluateBool(makeCall("not", {boolField()}), allNullData(), owner);
  ASSERT_EQ(result->size(), 3);
  for (auto i = 0; i < result->size(); ++i) {
    EXPECT_TRUE(result->isNullAt(i)) << "row: " << i;
  }
}

// L7: NOT(NOT(x)) is the identity — {true,false,NULL} stays
// {true,false,NULL}.
TEST_F(LogicalFunctionsTest, doubleNotIsIdentity) {
  flinksql::registerFunctions("");
  VectorPtr owner;
  auto* result = evaluateBool(
      makeCall("not", {makeCall("not", {boolField()})}),
      truthTableData(),
      owner);
  ASSERT_EQ(result->size(), 3);
  EXPECT_TRUE(result->valueAt(0));
  EXPECT_FALSE(result->valueAt(1));
  EXPECT_TRUE(result->isNullAt(2));
}

// L8: IS_NOT_TRUE(NOT(x)) equals the IS_NOT_FALSE(x) truth table —
// {true,false,NULL} -> {true,false,true}.
TEST_F(LogicalFunctionsTest, isNotTrueOfNot) {
  flinksql::registerFunctions("");
  VectorPtr owner;
  auto* result = evaluateBool(
      makeCall("is_not_true", {makeCall("not", {boolField()})}),
      truthTableData(),
      owner);
  ASSERT_EQ(result->size(), 3);
  EXPECT_TRUE(result->valueAt(0));
  EXPECT_FALSE(result->valueAt(1));
  EXPECT_TRUE(result->valueAt(2));
  EXPECT_EQ(0, BaseVector::countNulls(result->nulls(), result->size()));
}

// L9: all three IS_* functions over an all-NULL input produce
// {F,T,T} respectively, with no NULLs in the output.
TEST_F(LogicalFunctionsTest, isFamilyAllNullMatrix) {
  flinksql::registerFunctions("");
  struct Case {
    const char* name;
    bool expected;
  };
  const std::vector<Case> cases = {
      {"is_false", false}, {"is_not_true", true}, {"is_not_false", true}};
  for (const auto& c : cases) {
    VectorPtr owner;
    auto* result =
        evaluateBool(makeCall(c.name, {boolField()}), allNullData(), owner);
    ASSERT_EQ(result->size(), 3);
    for (auto i = 0; i < result->size(); ++i) {
      EXPECT_FALSE(result->isNullAt(i)) << c.name << " row: " << i;
      EXPECT_EQ(c.expected, result->valueAt(i)) << c.name << " row: " << i;
    }
  }
}

// L10: the "not" name resolves through the vector function registry to the
// flinksql registration: only flinksql is registered in this process, and
// "not" is present in the vector signatures with (boolean) -> boolean.
TEST_F(LogicalFunctionsTest, notNameResolution) {
  flinksql::registerFunctions("");

  auto vectorSignatures = getVectorFunctionSignatures();
  ASSERT_TRUE(vectorSignatures.count("not") > 0);
  ASSERT_EQ(vectorSignatures["not"].size(), 1);
  EXPECT_EQ("(boolean) -> boolean", vectorSignatures["not"][0]->toString());

  // Evaluating by name matches the NOT truth table (L1).
  VectorPtr owner;
  auto* result =
      evaluateBool(makeCall("not", {boolField()}), truthTableData(), owner);
  ASSERT_EQ(result->size(), 3);
  EXPECT_FALSE(result->valueAt(0));
  EXPECT_TRUE(result->valueAt(1));
  EXPECT_TRUE(result->isNullAt(2));
}

} // namespace facebook::velox::functions::flinksql::test
