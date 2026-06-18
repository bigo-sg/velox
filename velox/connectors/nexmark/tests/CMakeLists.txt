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
add_executable(velox_nexmark_connector_test NexmarkConnectorTest.cpp)

add_test(velox_nexmark_connector_test velox_nexmark_connector_test)

target_link_libraries(
  velox_nexmark_connector_test
  velox_nexmark_connector
  velox_vector_test_lib
  velox_exec_test_lib
  velox_aggregates
  GTest::gtest
  GTest::gtest_main)

add_executable(velox_nexmark_generator_test NexmarkGeneratorTest.cpp)

add_test(velox_nexmark_generator_test velox_nexmark_generator_test)

target_link_libraries(
  velox_nexmark_generator_test
  velox_nexmark_connector
  velox_vector_test_lib
  GTest::gtest
  GTest::gtest_main)

add_executable(velox_nexmark_utils_test NexmarkUtilsTest.cpp)

add_test(velox_nexmark_utils_test velox_nexmark_utils_test)

target_link_libraries(
  velox_nexmark_utils_test
  velox_nexmark_connector
  GTest::gtest
  GTest::gtest_main)
