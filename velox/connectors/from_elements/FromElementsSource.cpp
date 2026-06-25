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

#include "velox/connectors/from_elements/FromElementsSource.h"
#include "velox/type/tz/TimeZoneMap.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::from_elements {

FromElementsSource::FromElementsSource(
    const RowTypePtr& outputType,
    const ConnectorQueryCtx* queryCtx,
    const std::vector<std::string>& s)
    : outputType_(outputType),
      queryCtx_(queryCtx),
      formatter_(createFormatter(
          outputType,
          tz::locateZone(queryCtx->sessionTimezone()))) {
  VELOX_CHECK(formatter_ != nullptr);
  auto row = RowVector::createEmpty(outputType_, queryCtx->memoryPool());
  row->resize(s.size());
  VectorPtr vec = std::dynamic_pointer_cast<BaseVector>(row);
  for (int i = 0; i < s.size(); ++i) {
    formatter_->fromString(s[i], outputType_, i, vec);
  }
  data_ = row;
}

std::optional<RowVectorPtr> FromElementsSource::next(
    uint64_t size,
    velox::ContinueFuture& future) {
  std::optional<RowVectorPtr> res;
  if (data_ == nullptr) {
    return res;
  } else {
    RowVectorPtr row = std::dynamic_pointer_cast<RowVector>(data_);
    res.emplace(row);
    completedRows_ += data_->size();
    completedBytes_ += data_->inMemoryBytes();
    data_ = nullptr;
    return res;
  }
}

void FromElementsSource::addSplit(std::shared_ptr<ConnectorSplit> split) {}

} // namespace facebook::velox::connector::from_elements
