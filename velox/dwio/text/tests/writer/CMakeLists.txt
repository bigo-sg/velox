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

add_executable(velox_text_writer_test
               TextWriterTest.cpp BufferedWriterSinkTest.cpp FileReaderUtil.cpp)

gtest_add_tests(TARGET velox_text_writer_test
                TEST_LIST velox_text_writer_test_cases)
set_tests_properties(${velox_text_writer_test_cases} PROPERTIES DISABLED TRUE)

target_link_libraries(
  velox_text_writer_test
  velox_dwio_text_writer
  velox_dwio_common_test_utils
  velox_link_libs
  Boost::regex
  Folly::folly
  ${TEST_LINK_LIBS}
  GTest::gtest
  fmt::fmt)
