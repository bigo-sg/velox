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
  velox_serializer_test
  CompactRowSerializerTest.cpp PrestoOutputStreamListenerTest.cpp
  PrestoSerializerTest.cpp UnsafeRowSerializerTest.cpp)

add_test(
  velox_serializer_test
  velox_serializer_test
  --gtest_filter=-PrestoSerializerTest/PrestoSerializerTest.typeMismatch/0:PrestoSerializerTest/PrestoSerializerTest.typeMismatch/1:PrestoSerializerTest/PrestoSerializerTest.typeMismatch/2:PrestoSerializerTest/PrestoSerializerTest.typeMismatch/3:PrestoSerializerTest/PrestoSerializerTest.typeMismatch/4:PrestoSerializerTest/PrestoSerializerTest.typeMismatch/5:PrestoSerializerTest/PrestoSerializerTest.checksum/0:PrestoSerializerTest/PrestoSerializerTest.checksum/1:PrestoSerializerTest/PrestoSerializerTest.checksum/2:PrestoSerializerTest/PrestoSerializerTest.checksum/3:PrestoSerializerTest/PrestoSerializerTest.checksum/4:PrestoSerializerTest/PrestoSerializerTest.checksum/5:PrestoSerializerTest/PrestoSerializerTest.nullVector/0:PrestoSerializerTest/PrestoSerializerTest.nullVector/1:PrestoSerializerTest/PrestoSerializerTest.nullVector/2:PrestoSerializerTest/PrestoSerializerTest.nullVector/3:PrestoSerializerTest/PrestoSerializerTest.nullVector/4:PrestoSerializerTest/PrestoSerializerTest.nullVector/5:PrestoSerializerTest/PrestoSerializerTest.basic/0:PrestoSerializerTest/PrestoSerializerTest.basic/1:PrestoSerializerTest/PrestoSerializerTest.basic/2:PrestoSerializerTest/PrestoSerializerTest.basic/3:PrestoSerializerTest/PrestoSerializerTest.basic/4:PrestoSerializerTest/PrestoSerializerTest.basic/5
)

target_link_libraries(
  velox_serializer_test
  velox_presto_serializer
  velox_vector_test_lib
  velox_vector_fuzzer
  velox_row_fast
  GTest::gtest
  GTest::gtest_main
  glog::glog)

if(VELOX_ENABLE_BENCHMARKS)
  add_executable(velox_serializer_benchmark SerializerBenchmark.cpp)

  target_link_libraries(
    velox_serializer_benchmark
    velox_presto_serializer
    velox_vector_test_lib
    velox_vector_fuzzer
    GTest::gtest
    GTest::gtest_main
    glog::glog)
endif()
