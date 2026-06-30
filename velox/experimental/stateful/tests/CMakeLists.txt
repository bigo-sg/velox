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

add_executable(velox_stateful_watermark_generator_test
               WatermarkGeneratorTest.cpp)

add_executable(velox_stateful_combined_watermark_status_test
               CombinedWatermarkStatusTest.cpp
               ../CombinedWatermarkStatus.cpp)

add_test(velox_stateful_watermark_generator_test
         velox_stateful_watermark_generator_test)

add_test(velox_stateful_combined_watermark_status_test
         velox_stateful_combined_watermark_status_test)

target_link_libraries(
  velox_stateful_combined_watermark_status_test
  velox_common_base
  GTest::gtest
  GTest::gtest_main)

target_link_libraries(
  velox_stateful_watermark_generator_test
  velox_stateful_exec
  velox_stateful_agg
  velox_stateful_join
  velox_stateful_rank
  velox_stateful_state
  velox_stateful_window
  velox_stateful_udf
  velox_vector_test_lib
  velox_exec_test_lib
  velox_exec
  velox_common_base
  GTest::gtest
  GTest::gtest_main)
