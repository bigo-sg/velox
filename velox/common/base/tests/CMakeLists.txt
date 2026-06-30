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

add_executable(
  velox_base_test
  AdmissionControllerTest.cpp
  AsyncSourceTest.cpp
  BitUtilTest.cpp
  BloomFilterTest.cpp
  CoalesceIoTest.cpp
  ConcurrentCounterTest.cpp
  ExceptionTest.cpp
  FsTest.cpp
  IndexedPriorityQueueTest.cpp
  LazyCPUThreadPoolExecutorTest.cpp
  RangeTest.cpp
  RuntimeMetricsTest.cpp
  ScopedLockTest.cpp
  SemaphoreTest.cpp
  SimdUtilTest.cpp
  SkewedPartitionBalancerTest.cpp
  SpillConfigTest.cpp
  SpillStatsTest.cpp
  StatsReporterTest.cpp
  StatusTest.cpp
  SuccinctPrinterTest.cpp)

gtest_add_tests(TARGET velox_base_test TEST_LIST velox_base_test_cases)
set_tests_properties(${velox_base_test_cases} PROPERTIES DISABLED TRUE)

target_link_libraries(
  velox_base_test
  PRIVATE
    velox_caching
    velox_common_base
    velox_memory
    velox_time
    velox_status
    velox_exception
    velox_temp_path
    Boost::filesystem
    Boost::headers
    Folly::folly
    fmt::fmt
    gflags::gflags
    GTest::gtest
    GTest::gmock
    GTest::gtest_main)

add_executable(velox_id_map_test IdMapTest.cpp)

add_test(velox_id_map_test velox_id_map_test)

target_link_libraries(
  velox_id_map_test
  velox_id_map
  velox_common_base
  velox_memory
  Boost::headers
  glog::glog
  GTest::gtest
  GTest::gtest_main
  pthread)

add_executable(velox_memcpy_meter Memcpy.cpp)
target_link_libraries(
  velox_memcpy_meter
  PRIVATE
    velox_common_base
    velox_exception
    velox_time
    Folly::folly
    gflags::gflags)
