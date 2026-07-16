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
#include <string_view>

namespace facebook::velox::stateful {

enum class RowKind : int8_t {
  INSERT = 0,
  UPDATE_BEFORE = 1,
  UPDATE_AFTER = 2,
  DELETE = 3,
};

// Name of the synthetic trailing column that carries RowKind across the JNI
// boundary inside a merged RowVector.
constexpr std::string_view kRowKindColumnName = "$row_kind";

} // namespace facebook::velox::stateful
