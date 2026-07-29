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
#include "velox/experimental/stateful/StreamElement.h"

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/type/Type.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::stateful {

RowVectorPtr StreamRecord::toMergedRowVector(bool alwaysAppendRowKind) const {
  if (appendOnly() && !alwaysAppendRowKind) {
    return record_;
  }
  auto rowType = asRowType(record_->type());
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> children;
  auto n = rowType->size();
  names.reserve(n + 1);
  types.reserve(n + 1);
  children.reserve(n + 1);
  for (size_t i = 0; i < n; ++i) {
    names.emplace_back(rowType->nameOf(i));
    types.emplace_back(rowType->childAt(i));
    children.emplace_back(record_->childAt(i));
  }
  names.emplace_back(kRowKindColumnName);
  types.emplace_back(TINYINT());
  if (appendOnly()) {
    children.emplace_back(std::make_shared<ConstantVector<int8_t>>(
        record_->pool(),
        size(),
        false /*isNull*/,
        TINYINT(),
        static_cast<int8_t>(RowKind::INSERT)));
  } else {
    children.emplace_back(rowKind_);
  }
  auto mergedType = ROW(std::move(names), std::move(types));
  return std::make_shared<RowVector>(
      record_->pool(),
      mergedType,
      record_->nulls(),
      size(),
      std::move(children));
}

std::shared_ptr<StreamRecord> StreamRecord::create(
    std::string nodeId,
    RowVectorPtr merged) {
  VELOX_USER_CHECK_NOT_NULL(merged, "merged RowVector must not be null");
  auto rowType = asRowType(merged->type());
  if (rowType->size() == 0 ||
      rowType->nameOf(rowType->size() - 1) != kRowKindColumnName) {
    return std::make_shared<StreamRecord>(std::move(nodeId), std::move(merged));
  }
  auto lastIdx = rowType->size() - 1;
  auto kindVector = merged->childAt(lastIdx);
  VELOX_USER_CHECK_EQ(
      kindVector->type()->kind(),
      TypeKind::TINYINT,
      "$row_kind column must be TINYINT, got {}",
      kindVector->type()->toString());
  auto simpleKind = std::dynamic_pointer_cast<SimpleVector<int8_t>>(kindVector);
  VELOX_USER_CHECK_NOT_NULL(
      simpleKind,
      "$row_kind column must be a SimpleVector<int8_t>, got encoding: {}",
      kindVector->encoding());
  VELOX_USER_CHECK_EQ(
      simpleKind->size(),
      merged->size(),
      "$row_kind column length {} does not match merged RowVector length {}",
      simpleKind->size(),
      merged->size());
  VELOX_USER_CHECK(
      !simpleKind->mayHaveNulls(), "$row_kind column must not contain nulls");
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> children;
  names.reserve(lastIdx);
  types.reserve(lastIdx);
  children.reserve(lastIdx);
  for (size_t i = 0; i < lastIdx; ++i) {
    names.emplace_back(rowType->nameOf(i));
    types.emplace_back(rowType->childAt(i));
    children.emplace_back(merged->childAt(i));
  }
  auto valueType = ROW(std::move(names), std::move(types));
  auto value = std::make_shared<RowVector>(
      merged->pool(),
      valueType,
      merged->nulls(),
      merged->size(),
      std::move(children));
  // Normalize a constant INSERT row kind back to appendOnly (rowKind=null) so
  // downstream code can rely on appendOnly() without re-checking the encoding.
  // Only ConstantVector is normalized here: its single shared value is O(1)
  // to inspect. Every other SimpleVector<int8_t> encoding (FlatVector,
  // DictionaryVector, SequenceVector, ...) would require an O(n) scan over
  // per-row kind bytes to detect all-INSERT, which is too expensive on the
  // create() hot path. Such records keep a non-null rowKind_ and appendOnly()
  // returns false; downstream consumers must walk per-row kinds themselves
  // (semantically correct, just on the slow path).
  if (simpleKind->isConstantEncoding() &&
      simpleKind->as<ConstantVector<int8_t>>()->valueAt(0) ==
          static_cast<int8_t>(RowKind::INSERT)) {
    return std::make_shared<StreamRecord>(std::move(nodeId), std::move(value));
  }
  return std::make_shared<StreamRecord>(
      std::move(nodeId), std::move(value), std::move(simpleKind));
}

} // namespace facebook::velox::stateful
