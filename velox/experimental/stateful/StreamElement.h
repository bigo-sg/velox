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
#include <cstdint>

#include "velox/core/PlanNode.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::stateful {

class StreamElement {
 public:
  StreamElement(std::string nodeId) : nodeId_(std::move(nodeId)) {}

  virtual bool isWatermark() {
    return false;
  }

  virtual bool isRecord() {
    return false;
  }

  virtual bool isWatermarkStatus() {
    return false;
  }

  const std::string nodeId() const {
    return nodeId_;
  }

 private:
  // Node ID of the operator that generates this element.
  const std::string nodeId_;
};

using StreamElementPtr = std::shared_ptr<StreamElement>;

class Watermark : public StreamElement {
 public:
  Watermark(std::string nodeId, int64_t timestamp)
      : StreamElement(nodeId), timestamp_(timestamp) {}

  int64_t timestamp() const {
    return timestamp_;
  }

  bool isWatermark() override {
    return true;
  }

 private:
  const int64_t timestamp_;
};

class WatermarkStatus : public StreamElement {
 public:
  // idle == true means WatermarkStatus.IDLE; false means ACTIVE.
  WatermarkStatus(std::string nodeId, bool idle)
      : StreamElement(nodeId), idle_(idle) {}

  bool idle() const {
    return idle_;
  }

  bool isWatermarkStatus() override {
    return true;
  }

 private:
  const bool idle_;
};

// StreamRecord carries a RowVector of user columns together with an optional
// per-row RowKind vector (TINYINT, byte ordinals matching Flink RowKind:
// 0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, 3=DELETE). When rowKind is null
// the record is appendOnly (all INSERT) and toMergedRowVector() returns the
// user RowVector without a trailing $row_kind column.
class StreamRecord : public StreamElement {
 public:
  // Primary constructors: user RowVector + optional per-row RowKind vector.
  // rowKind == nullptr means appendOnly.
  StreamRecord(
      std::string nodeId,
      RowVectorPtr record,
      SimpleVectorPtr<int8_t> rowKind = nullptr)
      : StreamElement(nodeId),
        record_(std::move(record)),
        rowKind_(std::move(rowKind)),
        timestamp_(-1),
        hasTimestamp_(false),
        key_(-1) {}

  StreamRecord(
      std::string nodeId,
      RowVectorPtr record,
      SimpleVectorPtr<int8_t> rowKind,
      int64_t timestamp)
      : StreamElement(nodeId),
        record_(std::move(record)),
        rowKind_(std::move(rowKind)),
        timestamp_(timestamp),
        hasTimestamp_(true),
        key_(-1) {}

  // Convenience constructor for appendOnly records carrying a timestamp.
  StreamRecord(std::string nodeId, RowVectorPtr record, int64_t timestamp)
      : StreamElement(nodeId),
        record_(std::move(record)),
        rowKind_(nullptr),
        timestamp_(timestamp),
        hasTimestamp_(true),
        key_(-1) {}

  StreamRecord(
      std::string nodeId,
      int key,
      RowVectorPtr record,
      SimpleVectorPtr<int8_t> rowKind = nullptr)
      : StreamElement(nodeId),
        record_(std::move(record)),
        rowKind_(std::move(rowKind)),
        timestamp_(-1),
        hasTimestamp_(false),
        key_(key) {}

  const RowVectorPtr& record() const {
    return record_;
  }

  // May be null when appendOnly.
  const SimpleVectorPtr<int8_t>& rowKind() const {
    return rowKind_;
  }

  bool appendOnly() const {
    return !rowKind_;
  }

  vector_size_t size() const {
    return record_->size();
  }

  int64_t timestamp() const {
    return timestamp_;
  }

  int key() const {
    return key_;
  }

  bool isRecord() override {
    return true;
  }

  bool hasTimestamp() const {
    return hasTimestamp_;
  }

  // Merges record_ + rowKind_ into a single RowVector. By default an appendOnly
  // record returns record_ as-is (no trailing $row_kind column). When
  // alwaysAppendRowKind is true, the trailing $row_kind column is always
  // present: rowKind_ for changelog records, or a ConstantVector<int8_t>
  // (INSERT) for appendOnly records. Used by callers (e.g. JNI boundary,
  // StatefulCalcOperator) that need a RowVector schema matching an
  // N+1-column output type regardless of appendOnly state.
  RowVectorPtr toMergedRowVector(bool alwaysAppendRowKind = false) const;

  // Splits a merged RowVector (user columns + optional trailing $row_kind)
  // back into a StreamRecord. If the trailing column is absent or is a
  // ConstantVector<int8_t>(INSERT), the result is appendOnly (rowKind=null).
  static std::shared_ptr<StreamRecord> create(
      std::string nodeId,
      RowVectorPtr merged);

 private:
  const RowVectorPtr record_;
  const SimpleVectorPtr<int8_t> rowKind_;
  const int64_t timestamp_;
  bool hasTimestamp_ = false;
  const int key_;
};

using StreamRecordPtr = std::shared_ptr<StreamRecord>;

} // namespace facebook::velox::stateful
