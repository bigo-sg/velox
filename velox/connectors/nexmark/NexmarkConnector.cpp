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
#include "velox/vector/VectorPrinter.h"

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
  auto config = NexmarkConfiguration::deserialize(obj["config"]);

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

  GeneratorConfig config(
      nexmarkTableHandle->config_,
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count(),
      1,
      nexmarkTableHandle->config_.numEvents,
      1);
  nexmarkGenerator_ = std::make_unique<NexmarkGenerator>(
      config, 0, -1, pool_);
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
  auto pair = nexmarkGenerator_->nextBatch(outputRows);
  auto [outputVector, maxWallclockTimestamp] = std::move(pair);
  splitOffset_ += outputVector->size();
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();

  // Wait until reach the max wallclock timestamp.
  auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch())
      .count();
  if (maxWallclockTimestamp > nowMs) {
    // std::cout << "maxWallclockTimestamp:" << maxWallclockTimestamp
    //           << ",nowMs:" << nowMs << ",wait:" << maxWallclockTimestamp - nowMs
    //           << std::endl;
    std::this_thread::sleep_for(
        std::chrono::milliseconds(maxWallclockTimestamp - nowMs));
  }

  // std::cout << facebook::velox::printVector(*outputVector) << std::endl;
  return outputVector;
}

} // namespace facebook::velox::connector::nexmark
