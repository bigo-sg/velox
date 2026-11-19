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
#pragma once

#include "velox/exec/Aggregate.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::flinksql {

/// Flink CountAggFunction (non-null values, or all rows for the no-argument
/// COUNT(*)): BIGINT, never null - zero for nothing accumulated. The insert-
/// only face of the P0 window supply; the retract variant arrives with the
/// P1 retract-to-window chain.
class CountAggregate final : public exec::Aggregate {
 public:
  CountAggregate() : Aggregate(BIGINT()) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  int32_t accumulatorAlignmentSize() const override {
    return 1;
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<FlatVector<int64_t>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);
    int64_t* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      // Count is never null (Flink reports 0 for empty input).
      clearNull(rawNulls, i);
      rawValues[i] = *exec::Aggregate::value<int64_t>(groups[i]);
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      rows.applyToSelected([&](vector_size_t i) {
        *exec::Aggregate::value<int64_t>(groups[i]) += 1;
      });
      return;
    }
    DecodedVector decoded;
    decoded.decode(*args[0], rows);
    updateRows(groups, rows, decoded);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      *exec::Aggregate::value<int64_t>(group) += rows.countSelected();
      return;
    }
    DecodedVector decoded;
    decoded.decode(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        *exec::Aggregate::value<int64_t>(group) += rows.countSelected();
      }
      return;
    }
    const bool mayHaveNulls = decoded.mayHaveNulls();
    rows.applyToSelected([&](vector_size_t i) {
      if (mayHaveNulls && decoded.isNullAt(i)) {
        return;
      }
      *exec::Aggregate::value<int64_t>(group) += 1;
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded;
    decoded.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t i) {
      if (!decoded.isNullAt(i)) {
        *exec::Aggregate::value<int64_t>(groups[i]) +=
            decoded.valueAt<int64_t>(i);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded;
    decoded.decode(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        *exec::Aggregate::value<int64_t>(group) +=
            decoded.valueAt<int64_t>(0) * rows.countSelected();
      }
      return;
    }
    rows.applyToSelected([&](vector_size_t i) {
      if (!decoded.isNullAt(i)) {
        *exec::Aggregate::value<int64_t>(group) +=
            decoded.valueAt<int64_t>(i);
      }
    });
  }

 private:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<int64_t>(groups[i]) = 0;
    }
  }

  void updateRows(
      char** groups,
      const SelectivityVector& rows,
      const DecodedVector& decoded) {
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        rows.applyToSelected([&](vector_size_t i) {
          *exec::Aggregate::value<int64_t>(groups[i]) += 1;
        });
      }
      return;
    }
    const bool mayHaveNulls = decoded.mayHaveNulls();
    rows.applyToSelected([&](vector_size_t i) {
      if (mayHaveNulls && decoded.isNullAt(i)) {
        return;
      }
      *exec::Aggregate::value<int64_t>(groups[i]) += 1;
    });
  }
};

} // namespace facebook::velox::aggregate::flinksql
