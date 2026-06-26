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

set(VELOX_CPR_VERSION 1.10.5)
set(VELOX_CPR_BUILD_SHA256_CHECKSUM
    c8590568996cea918d7cf7ec6845d954b9b95ab2c4980b365f582a665dea08d8)
set(VELOX_CPR_SOURCE_URL
    "https://github.com/libcpr/cpr/archive/refs/tags/${VELOX_CPR_VERSION}.tar.gz"
)

# Add the dependency for curl, so that we can define the source URL for curl in
# curl.cmake. This will override the curl version declared by cpr.
set(curl_SOURCE BUNDLED)
velox_resolve_dependency(curl)

# Build bundled curl with OpenSSL so it has CURL_OPENSSL_4 versioned symbols
# compatible with librdkafka and libpulsar which were linked against system
# curl.
set(CURL_USE_OPENSSL
    ON
    CACHE BOOL "")

velox_resolve_dependency_url(CPR)

message(STATUS "Building cpr from source")
FetchContent_Declare(
  cpr
  URL ${VELOX_CPR_SOURCE_URL}
  URL_HASH ${VELOX_CPR_BUILD_SHA256_CHECKSUM}
  PATCH_COMMAND
    git apply ${CMAKE_CURRENT_LIST_DIR}/cpr/cpr-libcurl-compatible.patch && git
    apply ${CMAKE_CURRENT_LIST_DIR}/cpr/cpr-remove-sancheck.patch)
set(BUILD_SHARED_LIBS ${VELOX_BUILD_SHARED})
set(CPR_USE_SYSTEM_CURL OFF)
# ZLIB has already been found by find_package(ZLIB, REQUIRED), set CURL_ZLIB=OFF
# to save compile time.
set(CURL_ZLIB OFF)
FetchContent_MakeAvailable(cpr)
# libcpr in its CMakeLists.txt file disables the BUILD_TESTING globally when
# CPR_USE_SYSTEM_CURL=OFF. unset BUILD_TESTING here.
unset(BUILD_TESTING)
unset(BUILD_SHARED_LIBS)

# Add versioned symbol support to bundled curl so that librdkafka and libpulsar
# (compiled against system curl with CURL_OPENSSL_4) can resolve their versioned
# symbol references at link time.
FetchContent_GetProperties(curl)
if(curl_POPULATED AND TARGET libcurl_shared)
  set(_curl_vers_in "${curl_SOURCE_DIR}/lib/libcurl.vers.in")
  set(_curl_vers_out "${curl_BINARY_DIR}/lib/libcurl.vers")
  if(EXISTS "${_curl_vers_in}")
    file(READ "${_curl_vers_in}" _curl_vers_content)
    string(REPLACE "@CURL_LT_SHLIB_VERSIONED_FLAVOUR@" "OPENSSL_"
                   _curl_vers_content "${_curl_vers_content}")
    file(WRITE "${_curl_vers_out}" "${_curl_vers_content}")
    get_target_property(_curl_link_flags libcurl_shared LINK_FLAGS)
    if(NOT _curl_link_flags)
      set(_curl_link_flags "")
    endif()
    if(NOT "${_curl_link_flags}" MATCHES "version-script")
      set_target_properties(
        libcurl_shared
        PROPERTIES LINK_FLAGS
                   "${_curl_link_flags} -Wl,--version-script,${_curl_vers_out}")
      message(STATUS "curl: enabled versioned symbols (CURL_OPENSSL_4)")
    endif()
    unset(_curl_link_flags)
  endif()
  unset(_curl_vers_in)
  unset(_curl_vers_out)
endif()
