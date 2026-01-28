/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/dwio/text/reader/TextReader.h"
#include <iostream>
#include <memory>
#include <unordered_map>
#include "velox/common/base/Fs.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/dwio/text/writer/TextWriter.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {
class TextReaderTest : public testing::Test,
                       public velox::test::VectorTestBase {
 public:
  void SetUp() override {
    velox::filesystems::registerLocalFileSystem();
    dwio::common::LocalFileSink::registerFactory();
    text::registerTextReaderFactory();
    rootPool_ = memory::memoryManager()->addRootPool("TextReaderTests");
    leafPool_ = rootPool_->addLeafChild("TextReaderTests");
    schema_ =
        ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
            {BOOLEAN(),
             TINYINT(),
             SMALLINT(),
             INTEGER(),
             BIGINT(),
             REAL(),
             DOUBLE(),
             TIMESTAMP(),
             VARCHAR()});
    std::shared_ptr<exec::test::TempDirectoryPath> tempPath =
        exec::test::TempDirectoryPath::create();
    filePath_ =
        fs::path(fmt::format("{}/test_text_reader.txt", tempPath->getPath()));
    initFileData();
    initFileReader();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void initFileData() {
    auto data = makeRowVector(
        {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
        {makeConstant(true, 3),
         makeFlatVector<int8_t>({1, 2, 3}),
         makeFlatVector<int16_t>({1, 2, 3}), // TODO null
         makeFlatVector<int32_t>({1, 2, 3}),
         makeFlatVector<int64_t>({1, 2, 3}),
         makeFlatVector<float>({1.1, kInf, 3.1}),
         makeFlatVector<double>({1.1, kNaN, 3.1}),
         makeFlatVector<Timestamp>(
             3, [](auto i) { return Timestamp(i, i * 1'000'000); }),
         makeFlatVector<StringView>({"hello", "world", "cpp"}, VARCHAR())});

    text::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    auto sink = std::make_unique<dwio::common::LocalFileSink>(
        filePath_, dwio::common::FileSink::Options{.pool = leafPool_.get()});
    auto writer = std::make_unique<text::TextWriter>(
        schema_,
        std::move(sink),
        std::make_shared<text::WriterOptions>(writerOptions));
    for (int i = 0; i < 1024; ++i) {
      writer->write(data);
      writer->flush();
    }
    writer->close();
  }

  void initFileReader() {
    std::unordered_map<std::string, std::string> configMap;
    auto fs = filesystems::getFileSystem(
        filePath_, std::make_shared<config::ConfigBase>(std::move(configMap)));
    std::shared_ptr<ReadFile> readFile = fs->openFileForRead(filePath_);
    VELOX_CHECK(readFile != nullptr);
    std::unique_ptr<dwio::common::BufferedInput> input =
        std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
    text::RowReaderOptions rowReaderOptions(pool_.get(), "\x01", 1, 1024);
    rowReaderOptions.setFileFormat(dwio::common::FileFormat::TEXT);
    rowReaderOptions.setFileSchema(schema_);
    auto readerFactory =
        dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
    auto textReader =
        readerFactory->createReader(std::move(input), rowReaderOptions);
    VELOX_CHECK(textReader != nullptr);
    reader_ = textReader->createRowReader(rowReaderOptions);
  }

  constexpr static float kInf = std::numeric_limits<float>::infinity();
  constexpr static double kNaN = std::numeric_limits<double>::quiet_NaN();
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::string filePath_;
  RowTypePtr schema_;
  std::unique_ptr<dwio::common::RowReader> reader_;
};

TEST_F(TextReaderTest, TestReadData) {
  VectorPtr t = RowVector::createEmpty(schema_, leafPool_.get());
  VectorPtr res = RowVector::createEmpty(schema_, leafPool_.get());
  while (reader_->next(1024, t, nullptr) != 0) {
    res->append(t.get());
    t->prepareForReuse();
  }
  RowVectorPtr r = std::dynamic_pointer_cast<RowVector>(res);
  ASSERT_TRUE(r->size() == 3072);
  std::vector<VectorPtr>& vecs = r->children();
  ASSERT_TRUE(vecs.size() == 9);
  auto v0 = vecs[0]->asFlatVector<bool>();
  auto v1 = vecs[1]->asFlatVector<int8_t>();
  auto v2 = vecs[2]->asFlatVector<int16_t>();
  auto v3 = vecs[3]->asFlatVector<int32_t>();
  auto v4 = vecs[4]->asFlatVector<int64_t>();
  auto v5 = vecs[5]->asFlatVector<float>();
  auto v6 = vecs[6]->asFlatVector<double>();
  auto v7 = vecs[7]->asFlatVector<Timestamp>();
  auto v8 = vecs[8]->asFlatVector<StringView>();
  ASSERT_TRUE(
      v0->valueAt(0) == true && v0->valueAt(1) == true &&
      v0->valueAt(2) == true);
  ASSERT_TRUE(
      v1->valueAt(0) == 1 && v1->valueAt(1) == 2 && v1->valueAt(2) == 3);
  ASSERT_TRUE(
      v2->valueAt(0) == 1 && v2->valueAt(1) == 2 && v2->valueAt(2) == 3);
  ASSERT_TRUE(
      v3->valueAt(0) == 1 && v3->valueAt(1) == 2 && v3->valueAt(2) == 3);
  ASSERT_TRUE(
      v4->valueAt(0) == 1 && v4->valueAt(1) == 2 && v4->valueAt(2) == 3);
  ASSERT_TRUE(
      v5->valueAt(30) == 1.1f && v5->valueAt(31) == kInf &&
      v5->valueAt(32) == 3.1f);
  ASSERT_TRUE(
      v6->valueAt(0) == 1.1 && std::to_string(v6->valueAt(1)) == "nan" &&
      v6->valueAt(2) == 3.1);
  for (size_t i = 0; i < 3; ++i) {
    auto t = v7->valueAt(i);
    ASSERT_TRUE(t.getSeconds() == i && t.getNanos() == i * 1'000'000);
  }
  ASSERT_TRUE(
      v8->valueAt(3069).str() == "hello" &&
      v8->valueAt(3070).str() == "world" && v8->valueAt(3071).str() == "cpp");
}

} // namespace facebook::velox::test
