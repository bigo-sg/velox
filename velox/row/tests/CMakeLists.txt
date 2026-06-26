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
include(GoogleTest)

add_executable(velox_row_test CompactRowTest.cpp UnsafeRowTest.cpp)

gtest_add_tests(TARGET velox_row_test TEST_LIST velox_row_test_cases)
set_tests_properties(${velox_row_test_cases} PROPERTIES DISABLED TRUE)

target_link_libraries(
  velox_row_test
  PRIVATE
    velox_exception
    velox_row_fast
    velox_vector
    velox_vector_fuzzer
    velox_vector_test_lib
    Folly::folly
    GTest::gtest
    GTest::gtest_main)
