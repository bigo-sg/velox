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
#include "velox/experimental/stateful/window/WindowPartitionFunction.h"
#include <cstdint>
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/experimental/stateful/window/Window.h"

#include <numeric>

namespace facebook::velox::stateful {

WindowPartitionFunction::WindowPartitionFunction(
    const RowTypePtr& inputType,
    const column_index_t rowtimeIndex,
    int64_t size,
    int64_t step,
    int64_t offset,
    WindowType windowType)
    : inputType_(std::move(inputType)),
      rowtimeIndex_(rowtimeIndex),
      size_(size),
      step_(step),
      offset_(offset),
      windowType_(windowType) {
  sliceSize_ = std::gcd(size, step);
}

std::optional<int64_t> WindowPartitionFunction::partition(
    const RowVector& input,
    std::vector<int64_t>& partitions) {
  if (inputType_->childAt(rowtimeIndex_)->kind() == TypeKind::BIGINT) {
    // TODO: this is a optimization, as the RowVector may have be partitioned in
    // local aggregation, so need not to partition again in global agg, but need
    // to verify whether the judge condition is enough.
    auto child = input.childAt(rowtimeIndex_);
    auto ts = child->as<SimpleVector<int64_t>>()->valueAt(0);
    return ts;
  }
  const auto size = input.size();
  partitions.clear();
  partitions.resize(size);
  // TODO: support more window types. Support time zone.
  for (auto i = 0; i < size; ++i) {
    const auto& child = input.childAt(rowtimeIndex_);
    auto ts = child->as<SimpleVector<Timestamp>>()->valueAt(i);
    int64_t timestamp = ts.toMillis();
    if (windowType_ == WindowType::HOP) { // Hopping window
      int64_t start = TimeWindowUtil::getWindowStartWithOffset(
          timestamp, offset_, sliceSize_);
      partitions[i] = start + sliceSize_;
    } else if (windowType_ == WindowType::TUMBLE) { // Windowed Slice Assigner
      partitions[i] = timestamp;
    } else {
      VELOX_UNSUPPORTED(
          "Unsupported window type: {}", static_cast<int32_t>(windowType_));
    }
  }
  return std::nullopt;
}

std::optional<uint32_t> WindowPartitionFunction::partition(
    const RowVector& /* input */,
    std::vector<uint32_t>& /* partitions */) {
  VELOX_NYI();
}

int64_t getTimestamp(
    const RowVectorPtr& input,
    RowTypePtr inputType,
    const column_index_t rowtimeIndex) {
  return 0;
}

std::unique_ptr<core::PartitionFunction>
StreamWindowPartitionFunctionSpec::create(int numPartitions, bool localExchange)
    const {
  return std::make_unique<WindowPartitionFunction>(
      inputType_, rowtimeIndex_, size_, step_, offset_, windowType_);
}

std::string StreamWindowPartitionFunctionSpec::toString() const {
  return fmt::format("FIELD()", rowtimeIndex_);
}

folly::dynamic StreamWindowPartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "StreamWindowPartitionFunctionSpec";
  obj["inputType"] = inputType_->serialize();
  obj["rowtimeIndex"] = rowtimeIndex_;
  obj["size"] = size_;
  obj["step"] = step_;
  obj["offset"] = offset_;
  obj["windowType"] = static_cast<int32_t>(windowType_);
  return obj;
}

// static
core::PartitionFunctionSpecPtr StreamWindowPartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* /* context */) {
  auto rowtimeIndex = obj["rowtimeIndex"].asInt();
  auto size = obj["size"].asInt();
  auto step = obj["step"].asInt();
  auto offset = obj["offset"].asInt();
  auto windowType = Window::getType(obj["windowType"].asInt());

  return std::make_shared<StreamWindowPartitionFunctionSpec>(
      ISerializable::deserialize<RowType>(obj["inputType"]),
      rowtimeIndex,
      size,
      step,
      offset,
      windowType);
}

void registerPartitionFunctionSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register(
      "StreamWindowPartitionFunctionSpec",
      StreamWindowPartitionFunctionSpec::deserialize);
}

} // namespace facebook::velox::stateful
