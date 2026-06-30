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

add_executable(simple_lru_cache_test SimpleLRUCacheTest.cpp)
add_test(simple_lru_cache_test simple_lru_cache_test)
target_link_libraries(
  simple_lru_cache_test
  PRIVATE
    Folly::folly
    velox_time
    glog::glog
    GTest::gtest
    GTest::gtest_main)

add_executable(
  velox_cache_test
  AsyncDataCacheTest.cpp
  CacheTTLControllerTest.cpp
  SsdFileTest.cpp
  SsdFileTrackerTest.cpp
  StringIdMapTest.cpp)
add_test(
  velox_cache_test
  velox_cache_test
  --gtest_filter=-SsdFileTest.fileCorruption:SsdFileTest.evictlogFileErrorInjection:AsyncDataCacheTest/AsyncDataCacheTest.outOfCapacity/0:AsyncDataCacheTest/AsyncDataCacheTest.outOfCapacity/1:AsyncDataCacheTest/AsyncDataCacheTest.outOfCapacity/2:AsyncDataCacheTest/AsyncDataCacheTest.shrinkCache/0:AsyncDataCacheTest/AsyncDataCacheTest.shrinkCache/1:AsyncDataCacheTest/AsyncDataCacheTest.shrinkCache/2
)
target_link_libraries(
  velox_cache_test
  PRIVATE
    velox_caching
    velox_file
    velox_file_test_utils
    velox_memory
    velox_temp_path
    velox_flag_definitions
    Folly::folly
    glog::glog
    GTest::gtest
    GTest::gtest_main)

add_executable(cached_factory_test CachedFactoryTest.cpp)
gtest_add_tests(TARGET cached_factory_test TEST_LIST cached_factory_test_cases)
set_tests_properties(${cached_factory_test_cases} PROPERTIES DISABLED TRUE)
target_link_libraries(
  cached_factory_test
  PRIVATE
    velox_process
    Folly::folly
    velox_time
    glog::glog
    GTest::gtest
    GTest::gtest_main)
