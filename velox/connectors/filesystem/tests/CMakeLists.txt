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
  velox_filesystem_connector_test
  FileSystemConnectorTest.cpp
  ${CMAKE_SOURCE_DIR}/velox/connectors/hive/storage_adapters/hdfs/tests/HdfsMiniCluster.cpp
)

add_test(
  velox_filesystem_connector_test
  velox_filesystem_connector_test
  --gtest_filter=-FileSystemConnectorTest.testWritePartitionedTable:FileSystemConnectorTest.testFileRolling:FileSystemConnectorTest.testFileCommit
)

target_link_libraries(
  velox_filesystem_connector_test
  velox_filesystem_connector
  velox_hdfs
  velox_dwio_text_writer_register
  velox_vector_test_lib
  velox_exec
  velox_exec_test_lib
  GTest::gtest
  GTest::gtest_main
  -Wl,--whole-archive
  velox_caching
  -Wl,--no-whole-archive)

if(VELOX_ENABLE_PARQUET)
  target_link_libraries(velox_filesystem_connector_test
                        velox_dwio_parquet_writer)
endif()

target_compile_options(velox_filesystem_connector_test
                       PRIVATE -Wno-deprecated-declarations)
