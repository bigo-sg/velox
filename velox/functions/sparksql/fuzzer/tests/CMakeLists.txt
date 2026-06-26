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

add_executable(velox_spark_query_runner_test SparkQueryRunnerTest.cpp)

gtest_add_tests(TARGET velox_spark_query_runner_test
                TEST_LIST velox_spark_query_runner_test_cases)
set_tests_properties(${velox_spark_query_runner_test_cases} PROPERTIES DISABLED
                                                                       TRUE)

target_link_libraries(
  velox_spark_query_runner_test
  PRIVATE
    velox_functions_spark_aggregates
    velox_spark_query_runner
    velox_functions_spark
    velox_exec_test_lib
    velox_dwio_parquet_writer
    velox_parse_utils
    velox_vector_test_lib
    GTest::gtest
    GTest::gtest_main
    Folly::folly)
