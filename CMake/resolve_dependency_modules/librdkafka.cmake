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

set(VELOX_LIBRDKAFKA_VERSION v2.10.0)
# release artifacts are tough (except the auto generated ones)
set(VELOX_LIBRDKAFKA_BUILD_SHA256_CHECKSUM 004b1cc2685d1d6d416b90b426a0a9d27327a214c6b807df6f9ea5887346ba3a)
set(VELOX_LIBRDKAFKA_SOURCE_URL "https://github.com/confluentinc/librdkafka/archive/refs/tags/v2.10.0.tar.gz")
velox_resolve_dependency_url(LIBRDKAFKA)

message(STATUS "Building LIBRDKAFKA from source")
FetchContent_Declare(
  librdkafka
  URL ${VELOX_LIBRDKAFKA_SOURCE_URL}
  URL_HASH ${VELOX_LIBRDKAFKA_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(librdkafka)