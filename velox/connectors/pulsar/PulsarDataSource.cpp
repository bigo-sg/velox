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

#include "velox/connectors/pulsar/PulsarDataSource.h"
#include <curl/curl.h>
#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/FileUtil.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/json.h>
#include <glog/logging.h>
#include <algorithm>
#include <cctype>
#include <limits>
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/connectors/kafka/format/CSVRecordDeserializer.h"
#include "velox/connectors/kafka/format/RawRecordDeserializer.h"
#include "velox/connectors/kafka/format/StreamJSONRecordDeserializer.h"
#include "velox/connectors/pulsar/PulsarConnectorSplit.h"
#include "velox/connectors/pulsar/PulsarPartitionUtils.h"
#include "velox/connectors/pulsar/PulsarTableHandle.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::connector::pulsar {

namespace {

constexpr const char* kTaskIndex = "task_index";
constexpr const char* kTaskParallelism = "parallelism";
constexpr uint32_t kReceiveTimeoutBackoffMillis = 1;

size_t
writeCurlResponse(char* contents, size_t size, size_t nmemb, void* userp) {
  const auto bytes = size * nmemb;
  auto* response = static_cast<std::string*>(userp);
  response->append(contents, bytes);
  return bytes;
}

std::string trimTrailingSlash(std::string value) {
  while (!value.empty() && value.back() == '/') {
    value.pop_back();
  }
  return value;
}

std::string urlEncode(CURL* curl, const std::string& value) {
  char* encoded =
      curl_easy_escape(curl, value.c_str(), static_cast<int>(value.size()));
  VELOX_CHECK_NOT_NULL(
      encoded, "Failed to URL encode Pulsar admin path segment.");
  std::string result(encoded);
  curl_free(encoded);
  return result;
}

std::string getResolvedAuthToken(const ConnectionConfig& config) {
  auto token = config.getAuthToken();
  if (token.empty() && !config.getAuthTokenFile().empty()) {
    VELOX_CHECK(
        folly::readFile(config.getAuthTokenFile().c_str(), token),
        "Failed to read Pulsar token file: {}",
        config.getAuthTokenFile());
    token = folly::trimWhitespace(token).str();
  }
  return token;
}

std::vector<std::string> parseTopicForAdminPath(const std::string& topic) {
  const auto schemeEnd = topic.find("://");
  VELOX_CHECK(
      schemeEnd != std::string::npos,
      "Invalid Pulsar topic {}. Expected persistent://tenant/namespace/topic.",
      topic);
  const auto domain = topic.substr(0, schemeEnd);
  VELOX_CHECK(
      domain == "persistent" || domain == "non-persistent",
      "Unsupported Pulsar topic domain {} in topic {}.",
      domain,
      topic);
  const auto rest = topic.substr(schemeEnd + 3);
  std::vector<std::string> parts;
  folly::split('/', rest, parts);
  VELOX_CHECK_GE(
      parts.size(),
      3,
      "Invalid Pulsar topic {}. Expected persistent://tenant/namespace/topic.",
      topic);
  std::string localName = parts[2];
  for (size_t i = 3; i < parts.size(); ++i) {
    localName += "/" + parts[i];
  }
  return {domain, parts[0], parts[1], localName};
}

bool isNonNegativeMessagePosition(const std::string& messageId) {
  std::vector<std::string> parts;
  folly::split(':', messageId, parts);
  if (parts.size() < 2) {
    return false;
  }
  try {
    return folly::to<int64_t>(parts[0]) >= 0 &&
        folly::to<int64_t>(parts[1]) >= 0;
  } catch (const std::exception&) {
    return false;
  }
}

std::optional<std::string> getCommittedSubscriptionCursor(
    const ConnectionConfig& config,
    const std::string& partitionedTopic) {
  const auto adminUrl = trimTrailingSlash(config.getAdminUrl());
  VELOX_CHECK(!adminUrl.empty(), "Pulsar admin URL is empty.");

  curl_global_init(CURL_GLOBAL_DEFAULT);
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(
      curl_easy_init(), curl_easy_cleanup);
  VELOX_CHECK_NOT_NULL(curl.get(), "Failed to initialize CURL.");

  const auto topicParts = parseTopicForAdminPath(partitionedTopic);
  const auto url = fmt::format(
      "{}/admin/v2/{}/{}/{}/{}/internalStats?metadata=false",
      adminUrl,
      urlEncode(curl.get(), topicParts[0]),
      urlEncode(curl.get(), topicParts[1]),
      urlEncode(curl.get(), topicParts[2]),
      urlEncode(curl.get(), topicParts[3]));

  std::string response;
  curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, writeCurlResponse);
  curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response);
  curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(
      curl.get(),
      CURLOPT_TIMEOUT_MS,
      static_cast<long>(config.getReceiveTimeoutMills()));

  struct curl_slist* headers = nullptr;
  const auto token = getResolvedAuthToken(config);
  if (!token.empty()) {
    headers = curl_slist_append(
        headers, fmt::format("Authorization: Bearer {}", token).c_str());
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers);
  }

  const auto cleanupHeaders = folly::makeGuard([&]() {
    if (headers != nullptr) {
      curl_slist_free_all(headers);
    }
  });

  const auto result = curl_easy_perform(curl.get());
  VELOX_CHECK(
      result == CURLE_OK,
      "Failed to query Pulsar subscription cursor from {}: {}",
      url,
      curl_easy_strerror(result));

  long responseCode = 0;
  curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &responseCode);
  if (responseCode == 404) {
    LOG(INFO) << fmt::format(
        "No Pulsar subscription cursor found for topic {} subscription {}. Using initial position {}.",
        partitionedTopic,
        config.getSubscriptionName(),
        config.getInitialPosition());
    return std::nullopt;
  }
  VELOX_CHECK(
      responseCode >= 200 && responseCode < 300,
      "Failed to query Pulsar subscription cursor from {}. HTTP status {}, body: {}",
      url,
      responseCode,
      response);

  folly::dynamic obj;
  try {
    obj = folly::parseJson(response);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to parse Pulsar internal stats response from {}: {}",
        url,
        e.what());
  }
  if (!obj.isObject() || !obj.count("cursors") || !obj["cursors"].isObject()) {
    return std::nullopt;
  }
  const auto& cursors = obj["cursors"];
  const auto subscriptionName = config.getSubscriptionName();
  if (!cursors.count(subscriptionName) ||
      !cursors[subscriptionName].isObject()) {
    LOG(INFO) << fmt::format(
        "No Pulsar cursor found for topic {} subscription {}. Using initial position {}.",
        partitionedTopic,
        subscriptionName,
        config.getInitialPosition());
    return std::nullopt;
  }
  const auto& cursor = cursors[subscriptionName];
  if (!cursor.count("markDeletePosition") ||
      !cursor["markDeletePosition"].isString()) {
    return std::nullopt;
  }
  const auto markDeletePosition = cursor["markDeletePosition"].asString();
  if (!isNonNegativeMessagePosition(markDeletePosition)) {
    LOG(INFO) << fmt::format(
        "Pulsar cursor for topic {} subscription {} has no committed message position: {}. Using initial position {}.",
        partitionedTopic,
        subscriptionName,
        markDeletePosition,
        config.getInitialPosition());
    return std::nullopt;
  }
  return markDeletePosition;
}

uint32_t javaStringHashCode(const std::string& value) {
  uint32_t hash = 0;
  for (const unsigned char c : value) {
    hash = 31 * hash + c;
  }
  return hash;
}

int32_t getSplitOwner(
    const std::string& topic,
    int32_t partitionIndex,
    int32_t taskParallelism) {
  const uint32_t startIndex =
      ((javaStringHashCode(topic) * 31) & 0x7fffffff) % taskParallelism;
  return (startIndex + std::max(partitionIndex, 0)) % taskParallelism;
}

int32_t partitionIndexFromTopicName(
    const std::string& baseTopic,
    const std::string& topic) {
  if (topic == baseTopic) {
    return -1;
  }
  const auto prefix = fmt::format("{}-partition-", baseTopic);
  if (topic.rfind(prefix, 0) != 0) {
    return -1;
  }
  return folly::to<int32_t>(topic.substr(prefix.size()));
}

std::string explicitInitialMessageId(const ConnectionConfig& config) {
  if (!config.exists(ConnectionConfig::kInitialPosition)) {
    return "";
  }
  const auto initialPosition = config.getInitialPosition();
  if (initialPosition == "earliest" || initialPosition == "latest") {
    return initialPosition;
  }
  VELOX_FAIL("Unsupported Pulsar initial position: {}", initialPosition);
}

} // namespace

PulsarDataSource::PulsarDataSource(
    const RowTypePtr& outputType,
    const TableHandlePtr& tableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    const ConnectionConfigPtr& config)
    : queryCtx_(connectorQueryCtx), config_(config), outputType_(outputType) {
  scheduler_.start();
  const std::shared_ptr<PulsarTableHandle> pulsarTableHandle =
      std::dynamic_pointer_cast<PulsarTableHandle>(tableHandle);
  if (pulsarTableHandle) {
    connectorId_ = pulsarTableHandle->connectorId();
    config_ = config_->updateAndGetAllConfigs<ConnectionConfig>(
        pulsarTableHandle->tableParameters());
  } else {
    VELOX_FAIL(
        "The table handle {} is not supported for pulsar data source.",
        tableHandle->connectorId());
  }
  VELOX_CHECK(
      config_->getDataBatchSize() > 0,
      "Batch size config value must greater than 0.");
  createCachedQueue(config_->getDataBatchSize());
  createRecordDeserializer(config_->getFormat(), outputType_);
}

PulsarDataSource::~PulsarDataSource() {
  scheduler_.shutdown();
  completeBlockingFuture();
}

bool PulsarDataSource::consumerCanbeCreated() const {
  return config_->exists(ConnectionConfig::kServiceUrl) &&
      config_->exists(ConnectionConfig::kTopic) &&
      config_->exists(ConnectionConfig::kSubscriptionName) &&
      config_->exists(ConnectionConfig::kFormat) && !consumer_.get();
}

void PulsarDataSource::createConsumerForPartitions() {
  VELOX_CHECK_NULL(
      consumer_.get(),
      "Failed to create pulsar consumer as the consumer is not null");
  VELOX_CHECK_GT(
      topicPartitionOffsets_.size(),
      0,
      "Pulsar split has no topic partition offsets to consume.");

  std::unordered_map<std::string, TopicPartitionOffset>
      resolvedTopicPartitionOffsetMap;
  std::vector<TopicPartitionOffset> resolvedTopicPartitionOffsets;
  const auto initialMessageId = explicitInitialMessageId(*config_);
  resolvedTopicPartitionOffsetMap.reserve(topicPartitionOffsets_.size());
  resolvedTopicPartitionOffsets.reserve(topicPartitionOffsets_.size());
  for (const auto& [partitionedTopic, topicPartitionOffset] :
       topicPartitionOffsets_) {
    TopicPartitionOffset resolvedTopicPartitionOffset = topicPartitionOffset;
    if (resolvedTopicPartitionOffset.messageId.empty()) {
      if (!initialMessageId.empty()) {
        resolvedTopicPartitionOffset.messageId = initialMessageId;
        resolvedTopicPartitionOffset.startMessageIdInclusive =
            initialMessageId == "earliest";
      } else {
        const auto committedCursor = getCommittedSubscriptionCursor(
            *config_, resolvedTopicPartitionOffset.partitionedTopic);
        if (committedCursor.has_value()) {
          resolvedTopicPartitionOffset.messageId = committedCursor.value();
          resolvedTopicPartitionOffset.startMessageIdInclusive = false;
        }
      }
    }
    LOG(INFO) << fmt::format(
        "Creating Pulsar consumer for partitioned topic {}, subscription {}, start offset {}",
        resolvedTopicPartitionOffset.partitionedTopic,
        config_->getSubscriptionName(),
        resolvedTopicPartitionOffset.messageId.empty()
            ? fmt::format(
                  "<initial-position:{}>", config_->getInitialPosition())
            : resolvedTopicPartitionOffset.messageId);
    resolvedTopicPartitionOffsets.push_back(resolvedTopicPartitionOffset);
    resolvedTopicPartitionOffsetMap.insert_or_assign(
        resolvedTopicPartitionOffset.partitionedTopic,
        std::move(resolvedTopicPartitionOffset));
  }

  topicPartitionOffsets_ = std::move(resolvedTopicPartitionOffsetMap);
  consumer_ = std::make_shared<PulsarConsumer>(
      config_, std::move(resolvedTopicPartitionOffsets));
}

void PulsarDataSource::resetSplitState() {
  consumer_.reset();
  queue_.clear();
  consumePos_ = 0;
  checkpointStateToCommit_.clear();
  checkpointAckOffsets_.clear();
  topicPartitionOffsets_.clear();
  cancelled_ = false;
  completeBlockingFuture();
}

void PulsarDataSource::cancel() {
  cancelled_ = true;
  if (consumer_) {
    consumer_->close();
  }
  completeBlockingFuture();
}

void PulsarDataSource::completeBlockingFuture() {
  if (blockingPromise_.has_value()) {
    blockingPromise_->setValue();
    blockingPromise_.reset();
  }
}

std::optional<RowVectorPtr> PulsarDataSource::blockOnReceiveTimeout(
    velox::ContinueFuture& future) {
  auto [promise, blockedFuture] =
      makeVeloxContinuePromiseContract("PulsarDataSource::next");
  blockingPromise_ = std::move(promise);
  scheduler_.addFunctionOnce(
      [this]() { completeBlockingFuture(); },
      fmt::format("PulsarDataSource::next.{}", ++blockingSequence_),
      std::chrono::milliseconds(kReceiveTimeoutBackoffMillis));
  future = std::move(blockedFuture);
  return std::nullopt;
}

void PulsarDataSource::createCachedQueue(uint32_t size) {
  VELOX_CHECK_GT(
      size, 0, "Pulsar consume message queue size must greater than 0");
  queue_.reserve(size);
}

void PulsarDataSource::createRecordDeserializer(
    const std::string& format,
    const RowTypePtr& outputType) {
  if (format == "json") {
    deserializer_ = std::make_shared<kafka::KafkaStreamJSONRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else if (format == "csv") {
    deserializer_ = std::make_shared<kafka::KafkaCSVRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else if (format == "raw") {
    deserializer_ = std::make_shared<kafka::KafkaRawRecordDeserializer>(
        outputType, queryCtx_->memoryPool());
  } else {
    VELOX_FAIL_UNSUPPORTED_INPUT_UNCATCHABLE(
        "The data format {} is not supported for pulsar.", format);
  }
  outRow_ = RowVector::createEmpty(outputType_, queryCtx_->memoryPool());
  outRow_->resize(1);
}

void PulsarDataSource::addSplit(ConnectorSplitPtr split) {
  VELOX_CHECK(
      !consumer_ && queue_.empty(),
      "Cannot add Pulsar split after Pulsar consumer has been created.");
  auto pulsarConnectorSplit =
      std::dynamic_pointer_cast<PulsarConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(
      pulsarConnectorSplit,
      "Failed to add split, because the pulsar connector split is null.");
  VELOX_CHECK_EQ(
      pulsarConnectorSplit->serviceUrl_,
      config_->getServiceUrl(),
      "Pulsar split service url differs from data source config.");
  VELOX_CHECK_EQ(
      pulsarConnectorSplit->topic_,
      config_->getTopic(),
      "Pulsar split topic differs from data source config.");
  VELOX_CHECK_EQ(
      pulsarConnectorSplit->subscriptionName_,
      config_->getSubscriptionName(),
      "Pulsar split subscription name differs from data source config.");
  VELOX_CHECK_EQ(
      pulsarConnectorSplit->format_,
      config_->getFormat(),
      "Pulsar split format differs from data source config.");

  resetSplitState();
  topicPartitionOffsets_ = getSplitPartitions(*pulsarConnectorSplit);
  const auto restoredOffsets =
      offsetsFromCheckpointRecords(restoredCheckpointRecords_);
  for (const auto& [partitionedTopic, restoredOffset] : restoredOffsets) {
    if (topicPartitionOffsets_.count(partitionedTopic) > 0) {
      topicPartitionOffsets_.insert_or_assign(partitionedTopic, restoredOffset);
    }
  }
  if (consumerCanbeCreated()) {
    createConsumerForPartitions();
  }
}

std::unordered_map<std::string, TopicPartitionOffset>
PulsarDataSource::getSplitPartitions(const PulsarConnectorSplit& split) const {
  if (!split.topicPartitions_.empty()) {
    std::unordered_map<std::string, TopicPartitionOffset> topicPartitionOffsets;
    topicPartitionOffsets.reserve(split.topicPartitions_.size());
    for (const auto& topicPartition : split.topicPartitions_) {
      VELOX_CHECK(
          topicPartition.partitionedTopic == split.topic_ ||
              partitionIndexFromTopicName(
                  split.topic_, topicPartition.partitionedTopic) >= 0,
          "Pulsar split partitioned topic {} does not belong to split topic {}.",
          topicPartition.partitionedTopic,
          split.topic_);
      auto [_, inserted] = topicPartitionOffsets.emplace(
          topicPartition.partitionedTopic,
          TopicPartitionOffset{
              topicPartition.partitionedTopic,
              topicPartition.messageId,
              topicPartition.startMessageIdInclusive});
      VELOX_CHECK(
          inserted,
          "Duplicate Pulsar split partitioned topic {}.",
          topicPartition.partitionedTopic);
    }
    return topicPartitionOffsets;
  }

  ::pulsar::Client client(
      config_->getServiceUrl(), config_->getPulsarClientConfiguration());
  std::vector<std::string> partitions;
  const auto result =
      client.getPartitionsForTopic(config_->getTopic(), partitions);
  client.close();
  VELOX_CHECK(
      result == ::pulsar::ResultOk,
      "Failed to get Pulsar partitions for topic {}: {}",
      config_->getTopic(),
      ::pulsar::strResult(result));
  VELOX_CHECK_GT(
      partitions.size(),
      0,
      "Failed to get partitions of Pulsar topic: {}",
      config_->getTopic());

  std::vector<int32_t> partitionIndexes;
  partitionIndexes.reserve(partitions.size());
  for (const auto& partition : partitions) {
    partitionIndexes.push_back(
        partitionIndexFromTopicName(config_->getTopic(), partition));
  }

  const auto selectedPartitionIndexes =
      selectPartitionsForTask(partitionIndexes);
  std::unordered_map<std::string, TopicPartitionOffset> topicPartitionOffsets;
  topicPartitionOffsets.reserve(selectedPartitionIndexes.size());
  for (const auto partitionIndex : selectedPartitionIndexes) {
    auto partitionedTopic =
        partitionedTopicName(config_->getTopic(), partitionIndex);
    topicPartitionOffsets.emplace(
        partitionedTopic, TopicPartitionOffset{partitionedTopic, "", true});
  }
  return topicPartitionOffsets;
}

std::vector<int32_t> PulsarDataSource::selectPartitionsForTask(
    const std::vector<int32_t>& partitionIndexes) const {
  const int32_t taskIndex = getTaskIndex();
  const int32_t taskParallelism = getTaskParallelism();
  VELOX_CHECK_GE(taskIndex, 0, "Pulsar task index must not be negative.");
  VELOX_CHECK_GT(
      taskParallelism, 0, "Pulsar task parallelism must be positive.");
  VELOX_CHECK_LT(
      taskIndex,
      taskParallelism,
      "Pulsar task index must be less than task parallelism.");

  std::vector<int32_t> selected;
  for (const auto partitionIndex : partitionIndexes) {
    if (getSplitOwner(config_->getTopic(), partitionIndex, taskParallelism) ==
        taskIndex) {
      selected.push_back(partitionIndex);
    }
  }
  return selected;
}

int32_t PulsarDataSource::getTaskIndex() const {
  const int32_t taskIndex = std::stoi(
      queryCtx_->sessionProperties()->get<std::string>(kTaskIndex, "-1"));
  VELOX_CHECK_GE(taskIndex, 0, "Pulsar task index must not be negative.");
  return taskIndex;
}

int32_t PulsarDataSource::getTaskParallelism() const {
  const int32_t taskParallelism = std::stoi(
      queryCtx_->sessionProperties()->get<std::string>(kTaskParallelism, "-1"));
  VELOX_CHECK_GT(
      taskParallelism, 0, "Pulsar task parallelism must be positive.");
  return taskParallelism;
}

std::optional<RowVectorPtr> PulsarDataSource::next(
    uint64_t,
    velox::ContinueFuture& future) {
  VELOX_CHECK(consumer_, "Pulsar consumer is not created");
  std::optional<RowVectorPtr> res;
  size_t consumedMsgBytes = 0;
  if (cancelled_) {
    return RowVectorPtr{nullptr};
  }
  if (queue_.empty()) {
    VELOX_CHECK_NOT_NULL(
        consumer_.get(),
        "Failed to consume pulsar messages as the consumer is null.");
    consumer_->consumeBatch(queue_, consumedMsgBytes);
    consumePos_ = 0;
    if (consumedMsgBytes == 0) {
      return blockOnReceiveTimeout(future);
    }
  }

  outRow_->prepareForReuse();
  size_t processDataSize = config_->getDataBatchSize() > 1
      ? queue_.size()
      : config_->getDataBatchSize();
  outRow_->resize(processDataSize);
  for (size_t pos = 0; pos < processDataSize; ++pos) {
    const auto& message = queue_[pos + consumePos_];
    deserializer_->deserialize(message.payload, pos, outRow_);
    const auto consumedTopicPartitionOffset =
        consumer_->topicPartitionOffset(message.message);
    if (!config_->getCheckpointEnabled()) {
      consumer_->acknowledge(message.message, false);
    }
    topicPartitionOffsets_.insert_or_assign(
        consumedTopicPartitionOffset.partitionedTopic,
        consumedTopicPartitionOffset);
    completedBytes_ += message.payload.size();
    completedRows_ += 1;
  }
  res.emplace(std::dynamic_pointer_cast<RowVector>(outRow_));
  consumePos_ += processDataSize;
  if (consumePos_ >= queue_.size()) {
    queue_.clear();
    consumePos_ = 0;
  }
  return res;
}

std::vector<std::string> PulsarDataSource::snapshotState(int64_t checkpointId) {
  VELOX_CHECK_GT(
      topicPartitionOffsets_.size(),
      0,
      "Cannot snapshot Pulsar split without active topic partitions.");
  std::vector<TopicPartitionOffset> topicPartitionOffsets;
  topicPartitionOffsets.reserve(topicPartitionOffsets_.size());
  for (const auto& [_, topicPartitionOffset] : topicPartitionOffsets_) {
    topicPartitionOffsets.push_back(topicPartitionOffset);
  }
  PulsarConnectorSplit split(
      connectorId_,
      config_->getServiceUrl(),
      config_->getTopic(),
      config_->getSubscriptionName(),
      config_->getFormat(),
      std::move(topicPartitionOffsets));
  checkpointStateToCommit_ = snapshotToJson(checkpointId, split);
  checkpointAckOffsets_ = topicPartitionOffsets_;
  return {checkpointStateToCommit_};
}

std::vector<std::string> PulsarDataSource::commit(int64_t) {
  if (checkpointStateToCommit_.empty()) {
    return {};
  }

  auto consumer = consumer_;
  if (!consumer || consumer->closed()) {
    LOG(WARNING) << fmt::format(
        "Skip committing Pulsar checkpoint because consumer is {}.",
        consumer ? "closed" : "not created");
    return {};
  }

  for (const auto& [_, topicPartitionOffset] : checkpointAckOffsets_) {
    if (!topicPartitionOffset.messageId.empty()) {
      consumer->acknowledge(topicPartitionOffset, true);
    }
  }

  std::vector<std::string> committed{checkpointStateToCommit_};
  checkpointStateToCommit_.clear();
  checkpointAckOffsets_.clear();
  return committed;
}

void PulsarDataSource::restoreState(
    const std::vector<std::string>& checkpointRecords) {
  restoredCheckpointRecords_ = checkpointRecords;
}

std::string PulsarDataSource::snapshotToJson(
    int64_t checkpointId,
    const PulsarConnectorSplit& split) const {
  folly::dynamic obj = split.serialize();
  obj["connector"] = "pulsar";
  obj["planNodeId"] = queryCtx_->planNodeId();
  obj["checkpointId"] = checkpointId;
  return folly::toJson(obj);
}

std::unordered_map<std::string, TopicPartitionOffset>
PulsarDataSource::offsetsFromCheckpointRecords(
    const std::vector<std::string>& checkpointRecords) const {
  int64_t selectedCheckpointId = std::numeric_limits<int64_t>::min();
  std::unordered_map<std::string, TopicPartitionOffset> selectedOffsets;
  for (const auto& checkpointRecord : checkpointRecords) {
    folly::dynamic obj;
    try {
      obj = folly::parseJson(checkpointRecord);
    } catch (const std::exception&) {
      continue;
    }
    if (!obj.isObject() || !obj.count("connector") ||
        !obj["connector"].isString() ||
        obj["connector"].asString() != "pulsar") {
      continue;
    }
    if (obj.count("planNodeId") &&
        (!obj["planNodeId"].isString() ||
         obj["planNodeId"].asString() != queryCtx_->planNodeId())) {
      continue;
    }
    if (!obj.count("checkpointId") || !obj["checkpointId"].isInt()) {
      continue;
    }
    const int64_t checkpointId = obj["checkpointId"].asInt();
    if (checkpointId < selectedCheckpointId) {
      continue;
    }

    std::shared_ptr<PulsarConnectorSplit> split;
    try {
      split = PulsarConnectorSplit::create(obj);
    } catch (const std::exception&) {
      continue;
    }
    if (!split || split->serviceUrl_ != config_->getServiceUrl() ||
        split->topic_ != config_->getTopic() ||
        split->subscriptionName_ != config_->getSubscriptionName() ||
        split->format_ != config_->getFormat()) {
      continue;
    }

    std::unordered_map<std::string, TopicPartitionOffset> offsets;
    offsets.reserve(split->topicPartitions_.size());
    for (const auto& topicPartitionOffset : split->topicPartitions_) {
      if (topicPartitionOffset.partitionedTopic.empty()) {
        continue;
      }
      offsets.insert_or_assign(
          topicPartitionOffset.partitionedTopic, topicPartitionOffset);
    }
    if (offsets.empty()) {
      continue;
    }
    selectedCheckpointId = checkpointId;
    selectedOffsets = std::move(offsets);
  }
  return selectedOffsets;
}

std::unordered_map<std::string, RuntimeCounter>
PulsarDataSource::runtimeStats() {
  return {
      {"completedRows", RuntimeCounter(completedRows_)},
      {"completedBytes",
       RuntimeCounter(completedBytes_, RuntimeCounter::Unit::kBytes)},
      {"committedMessages", RuntimeCounter(acknowledgedMessages_)},
  };
}

} // namespace facebook::velox::connector::pulsar
