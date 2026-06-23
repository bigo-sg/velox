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

add_executable(velox_print_sink_test PrintSinkTest.cpp)
add_executable(velox_print_table_handle_test PrintTableHandleTest.cpp)

add_test(velox_print_sink_test velox_print_sink_test)
add_test(velox_print_table_handle_test velox_print_table_handle_test)

target_link_libraries(
  velox_print_sink_test
  velox_print_connector
  GTest::gtest
  GTest::gtest_main)

target_link_libraries(
  velox_print_table_handle_test
  velox_print_connector
  velox_core
  GTest::gtest
  GTest::gtest_main)
