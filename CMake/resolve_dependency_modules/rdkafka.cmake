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

set(VELOX_RDKAFKA_VERSION v2.10.0)
set(VELOX_RDKAFKA_BUILD_SHA256_CHECKSUM
    004b1cc2685d1d6d416b90b426a0a9d27327a214c6b807df6f9ea5887346ba3a)
set(VELOX_RDKAFKA_SOURCE_URL
    "https://github.com/confluentinc/librdkafka/archive/refs/tags/${VELOX_RDKAFKA_VERSION}.tar.gz"
)

velox_resolve_dependency_url(RDKAFKA)

message(STATUS "Building RDKAFKA from source")
FetchContent_Declare(
  rdkafka
  URL ${VELOX_RDKAFKA_SOURCE_URL}
  URL_HASH ${VELOX_RDKAFKA_BUILD_SHA256_CHECKSUM})

set(RDKAFKA_BUILD_EXAMPLES
    OFF
    CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_TESTS
    OFF
    CACHE BOOL "" FORCE)
set(WITH_CURL
    OFF
    CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(rdkafka)

if(TARGET rdkafka AND NOT TARGET RdKafka::rdkafka)
  add_library(RdKafka::rdkafka ALIAS rdkafka)
endif()

if(TARGET rdkafka)
  set(_rdkafka_include_dir "${CMAKE_BINARY_DIR}/rdkafka-include")
  file(MAKE_DIRECTORY "${_rdkafka_include_dir}")
  file(COPY "${rdkafka_SOURCE_DIR}/src/rdkafka.h"
       DESTINATION "${_rdkafka_include_dir}")
  file(COPY "${rdkafka_SOURCE_DIR}/src/rdkafka_mock.h"
       DESTINATION "${_rdkafka_include_dir}")
  set_target_properties(
    rdkafka PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                       "$<BUILD_INTERFACE:${_rdkafka_include_dir}>")
endif()

set(_rdkafka_config_dir "${CMAKE_BINARY_DIR}/rdkafka-config")
file(MAKE_DIRECTORY "${_rdkafka_config_dir}")
file(WRITE "${_rdkafka_config_dir}/RdKafkaConfig.cmake"
     "if(NOT TARGET RdKafka::rdkafka)\n"
     "  message(FATAL_ERROR \"Bundled RdKafka target RdKafka::rdkafka is missing\")\n"
     "endif()\n"
     "set(RdKafka_FOUND TRUE)\n")
set(RdKafka_DIR
    "${_rdkafka_config_dir}"
    CACHE PATH "" FORCE)
unset(_rdkafka_config_dir)
unset(_rdkafka_include_dir)
