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

#include "velox/connectors/nexmark/NexmarkConnector.h"
#include <iostream>

namespace facebook::velox::connector::nexmark {

folly::dynamic NexmarkTableHandle::serialize() const {
  folly::dynamic obj = ConnectorTableHandle::serializeBase("NexmarkTableHandle");
  obj["config"] = config_.serialize();
  return obj;
}

ConnectorTableHandlePtr NexmarkTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  auto connectorId = obj["connectorId"].asString();
  auto config = GeneratorConfig::deserialize(obj["config"]);

  return std::make_shared<const NexmarkTableHandle>(
      connectorId, std::move(config));
}

void NexmarkTableHandle::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("NexmarkTableHandle", create);
}

NexmarkDataSource::NexmarkDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    velox::memory::MemoryPool* pool)
    : outputType_(outputType), pool_(pool) {
  auto nexmarkTableHandle =
      std::dynamic_pointer_cast<NexmarkTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      nexmarkTableHandle,
      "TableHandle must be an instance of NexmarkTableHandle");

  nexmarkGenerator_ =
      std::make_unique<NexmarkGenerator>(nexmarkTableHandle->config_, 0, -1);
}

void NexmarkDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_EQ(
      currentSplit_,
      nullptr,
      "NexmarkDataSource only accept one split.");
  currentSplit_ = std::dynamic_pointer_cast<NexmarkConnectorSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for NexmarkDataSource.");

  splitOffset_ = 0;
  splitEnd_ = currentSplit_->numRows;
}

std::optional<RowVectorPtr> NexmarkDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(
      currentSplit_, "No split to process. Call addSplit() first.");

  // Split exhausted.
  if (splitOffset_ >= splitEnd_ || !nexmarkGenerator_->hasNext()) {
    return nullptr;
  }

  const size_t outputRows = std::min(size, (splitEnd_ - splitOffset_));
  auto outputVector = Event::createVector(outputRows, pool_);

  size_t i = 0;
  int64_t maxWallclockTimestamp = 0;
  for (; i < outputRows && nexmarkGenerator_->hasNext(); ++i) {
    auto nextEvent = nexmarkGenerator_->next();
    Event::fillVector(outputVector.get(), i, nextEvent.getEvent());

    maxWallclockTimestamp = std::max(
        maxWallclockTimestamp, nextEvent.getWallclockTimestamp());
    // std::cerr << nextEvent.getEvent().toString() << std::endl;
  }
  outputVector->resize(i);

  splitOffset_ += outputVector->size();
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();

  /// Wait until reach the max wallclock timestamp.
  auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch())
      .count();
  if (maxWallclockTimestamp > nowMs) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(maxWallclockTimestamp - nowMs));
  }
  return outputVector;
}

} // namespace facebook::velox::connector::nexmark
