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

#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "velox/core/PlanNode.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/type/Type.h"

namespace facebook::velox::stateful {

/// A plan node whose declared output type is the source's output type plus a
/// trailing $row_kind column. Purely a schema declaration: consumers such as
/// FilterProject::initialize() resolve identity projections against
/// project->sources()[0]->outputType(), while the real upstream plan node has
/// no $row_kind column. The node never executes — operators driven directly
/// via addInput bypass it — and is never serialized.
class SchemaWithRowKindNode : public core::PlanNode {
 public:
  SchemaWithRowKindNode(const core::PlanNodeId& id, core::PlanNodePtr source)
      : core::PlanNode(id),
        sources_{std::move(source)},
        outputType_{withRowKindColumn(sources_[0]->outputType())} {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "SchemaWithRowKind";
  }

 private:
  void addDetails(std::stringstream&) const override {}

  static RowTypePtr withRowKindColumn(const RowTypePtr& input) {
    auto types = input->children();
    auto names = input->names();
    types.emplace_back(TINYINT());
    names.emplace_back(std::string(kRowKindColumnName));
    return std::make_shared<RowType>(std::move(names), std::move(types));
  }

  const std::vector<core::PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

} // namespace facebook::velox::stateful
