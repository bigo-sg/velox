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

set(VELOX_ROCKSDB_VERSION FRocksDB-6.20.3)
# release artifacts are tough (except the auto generated ones)
set(VELOX_ROCKSDB_BUILD_SHA256_CHECKSUM
    00ec077666ef76859d68cdff04a8cd40cad5afcb9ec1d100016358d7140a578d)
set(VELOX_ROCKSDB_SOURCE_URL
    "https://github.com/ververica/frocksdb/archive/refs/heads/FRocksDB-6.20.3.zip"
)
set(ROCKSDB_BUILD_SHARED ON)
set(PORTABLE ON)
set(FORCE_SSE42 ON)
set(WITH_BENCHMARK_TOOLS OFF)
set(WITH_CORE_TOOLS OFF)
set(WITH_TESTS OFF)
set(WITH_TOOLS OFF)

velox_resolve_dependency_url(ROCKSDB)

message(STATUS "Building ROCKSDB from source")
FetchContent_Declare(
  rocksdb
  URL ${VELOX_ROCKSDB_SOURCE_URL}
  URL_HASH ${VELOX_ROCKSDB_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(rocksdb)
