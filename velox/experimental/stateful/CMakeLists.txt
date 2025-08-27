# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(agg)
add_subdirectory(join)
add_subdirectory(rank)
add_subdirectory(state)
add_subdirectory(window)

velox_add_library(
  velox_stateful_exec
  OBJECT
  StatefulTask.cpp
  StatefulPlanner.cpp
  StatefulPlanNode.cpp
  StatefulOperator.cpp
  StreamPartition.cpp
  StreamJoin.cpp
  StreamKeyedOperator.cpp
  WindowJoin.cpp
  KeySelector.cpp
  CombinedWatermarkStatus.cpp
  LocalWindowAggregator.cpp
  WindowAggregator.cpp
  GroupWindowAggregator.cpp
  WatermarkAssigner.cpp)

velox_link_libraries(
  velox_stateful_exec
  PRIVATE
  velox_stateful_agg
  velox_stateful_join
  velox_stateful_rank
  velox_stateful_state
  velox_stateful_window
  velox_common_base
  velox_exec)
