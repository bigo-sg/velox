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

add_executable(
  velox_pulsar_connector_test
  PulsarConnectorTest.cpp
  ../PulsarClientConfig.cpp
  ../PulsarConfig.cpp
  ../PulsarConnectorSplit.cpp
  ../PulsarTableHandle.cpp)

add_test(velox_pulsar_connector_test velox_pulsar_connector_test)

target_include_directories(velox_pulsar_connector_test
                           PRIVATE ${PulsarClient_INCLUDE_DIR})

target_link_libraries(
  velox_pulsar_connector_test
  velox_connector
  velox_type
  velox_exception
  Folly::folly
  PulsarClient::pulsar
  GTest::gtest
  GTest::gtest_main)

option(VELOX_ENABLE_PULSAR_INTEGRATION_TESTS
       "Enable Pulsar connector standalone integration tests." OFF)

if(${VELOX_ENABLE_PULSAR_INTEGRATION_TESTS})
  add_executable(velox_pulsar_connector_integration_test
                 PulsarConnectorIntegrationTest.cpp)

  target_link_libraries(
    velox_pulsar_connector_integration_test
    velox_pulsar_connector
    velox_kafka_connector_format
    velox_connector
    velox_vector
    velox_exception
    Folly::folly
    PulsarClient::pulsar
    GTest::gtest
    GTest::gtest_main)

  add_test(
    NAME velox_pulsar_connector_integration_test
    COMMAND
      bash ${CMAKE_CURRENT_SOURCE_DIR}/run_pulsar_standalone_test.sh
      $<TARGET_FILE:velox_pulsar_connector_integration_test>
      ${CMAKE_CURRENT_BINARY_DIR}/standalone)
endif()
