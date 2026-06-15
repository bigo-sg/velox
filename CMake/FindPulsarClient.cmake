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

find_path(PulsarClient_INCLUDE_DIR pulsar/Client.h)
find_library(PulsarClient_LIBRARY NAMES pulsar)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  PulsarClient
  REQUIRED_VARS PulsarClient_LIBRARY PulsarClient_INCLUDE_DIR)

if(PulsarClient_FOUND AND NOT TARGET PulsarClient::pulsar)
  add_library(PulsarClient::pulsar UNKNOWN IMPORTED)
  set_target_properties(
    PulsarClient::pulsar
    PROPERTIES IMPORTED_LOCATION "${PulsarClient_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${PulsarClient_INCLUDE_DIR}")
endif()

mark_as_advanced(PulsarClient_INCLUDE_DIR PulsarClient_LIBRARY)
