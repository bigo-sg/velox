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
include_guard(GLOBAL)

set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_PERF_TOOLS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIB OFF CACHE BOOL "" FORCE)
set(BUILD_DYNAMIC_LIB ON CACHE BOOL "" FORCE)

FetchContent_Declare(
  pulsarclient
  GIT_REPOSITORY https://github.com/apache/pulsar-client-cpp.git
  GIT_TAG v3.3.0
  PATCH_COMMAND git apply
                ${CMAKE_CURRENT_LIST_DIR}/pulsarclient/pulsar-client-cpp-subproject.patch)

FetchContent_MakeAvailable(pulsarclient)

add_library(PulsarClient::pulsar ALIAS pulsarShared)
