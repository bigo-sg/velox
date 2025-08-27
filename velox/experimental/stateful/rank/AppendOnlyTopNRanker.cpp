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
#include "velox/experimental/stateful/rank/AppendOnlyTopNRanker.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

AppendOnlyTopNRanker::AppendOnlyTopNRanker(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& rankNode,
    std::unique_ptr<exec::Operator> veloxRanker,
    std::shared_ptr<KeySelector> sortKeySelector,
    // TODO: support it later
    // RankType rankType,
    // RankRange rankRange,
    bool generateUpdateBefore,
    bool outputRankNumber,
    long cacheSize)
    : exec::Operator(
      driverCtx,
      rankNode->outputType(),
      operatorId,
      rankNode->id(),
      "AppendOnlyTopNRanker"),
      veloxRanker_(std::move(veloxRanker)),
      sortKeySelector_(std::move(sortKeySelector)),
      // rankType_(rankType),
      // rankRange_(rankRange),
      generateUpdateBefore_(generateUpdateBefore),
      outputRankNumber_(outputRankNumber),
      cacheSize_(cacheSize) {
}

void AppendOnlyTopNRanker::initialize() {
  exec::Operator::initialize();
  veloxRanker_->initialize();
}

void AppendOnlyTopNRanker::open(StreamOperatorStateHandler* stateHandler) {
  /**
  int lruCacheSize = Math.max(1, (int) (cacheSize / getDefaultTopNSize()));
  CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
  if (ttlConfig.isEnabled()) {
      cacheBuilder.expireAfterWrite(
              ttlConfig.getTtl().toMilliseconds(), TimeUnit.MILLISECONDS);
  }
  kvSortedMap = cacheBuilder.maximumSize(lruCacheSize).build();
  LOG.info(
          "Top{} operator is using LRU caches key-size: {}",
          getDefaultTopNSize(),
          lruCacheSize);
  */
  StateDescriptor stateDesc("data-state-with-append");
  // TODO: support ttl
  // StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
  // if (ttlConfig.isEnabled()) {
  //    stateDesc.enableTimeToLive(ttlConfig);
  // }
  dataState_ = stateHandler->getRankMapState(stateDesc);
}

RowVectorPtr AppendOnlyTopNRanker::processElements(uint32_t key, RowVectorPtr input) {
  // TODO: not identically equal to flink, flink may generate output each row,
  // but we only generate output once a batch.
  std::list<RowVectorPtr> outputs;

  auto sortKeyToData = sortKeySelector_->partition(input);
  for (auto& [sortKey, data] : sortKeyToData) {
      auto preResult = dataState_->get(key, State::VOID_NAMESPACE, sortKey);
      if (preResult) {
          preResult = TimeWindowUtil::mergeVectors({data, preResult}, pool());
      } else {
        preResult = data;
      }

      // TODO: velox ranker may not output every time.
      veloxRanker_->addInput(preResult);
      RowVectorPtr output = veloxRanker_->getOutput();
      if (output) {
        outputs.push_back(output);
      }
  }
  return TimeWindowUtil::mergeVectors(outputs, pool());
}

void AppendOnlyTopNRanker::close() {
  exec::Operator::close();
  veloxRanker_->close();
  dataState_->clear();
  dataState_.reset();
}
} // namespace facebook::velox::stateful
