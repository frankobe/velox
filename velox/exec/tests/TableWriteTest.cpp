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
#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/WriteProtocol.h"
#include "velox/connectors/hive/HiveWriteProtocol.h"
#include "velox/dwio/common/DataSink.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;

class TableWriteTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    HiveNoCommitWriteProtocol::registerProtocol();
  }

  VectorPtr createConstant(variant value, vector_size_t size) const {
    return BaseVector::createConstant(value, size, pool_.get());
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveConnectorSplits(
      const std::shared_ptr<TempDirectoryPath>& directoryPath) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    for (auto& filePath : fs::directory_iterator(directoryPath->path)) {
      splits.push_back(makeHiveConnectorSplit(filePath.path().string()));
    }
    return splits;
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
};

// Runs a pipeline with read + filter + project (with substr) + write
TEST_F(TableWriteTest, scanFilterProjectWrite) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType_, filePaths.size(), 1000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto planBuilder = PlanBuilder();
  auto project = planBuilder.tableScan(rowType_)
                     .filter("c0 <> 0")
                     .project({"c0", "c1", "c1 + c2", "substr(c5, 1, 1)"})
                     .planNode();

  std::vector<std::string> columnNames = {
      "c0", "c1", "c1_plus_c2", "substr_c5"};
  auto plan = planBuilder
                  .tableWrite(
                      columnNames,
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              columnNames,
                              rowType_->children(),
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      WriteProtocol::CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp WHERE c0 <> 0");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query

  auto types = project->outputType()->children();
  auto rowType = ROW(std::move(columnNames), std::move(types));
  assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT c0, c1, c1 + c2, substr(c5, 1, 1) FROM tmp WHERE c0 <> 0");
}

TEST_F(TableWriteTest, renameAndReorderColumns) {
  auto rowType =
      ROW({"a", "b", "c", "d"}, {BIGINT(), INTEGER(), DOUBLE(), VARCHAR()});
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType, filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto tableRowType = ROW({"d", "c", "b"}, {VARCHAR(), DOUBLE(), INTEGER()});
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .tableWrite(
                      tableRowType,
                      {"x", "y", "z"},
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              {"x", "y", "z"},
                              tableRowType->children(),
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      WriteProtocol::CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  assertQuery(
      PlanBuilder()
          .tableScan(ROW({"x", "y", "z"}, {VARCHAR(), DOUBLE(), INTEGER()}))
          .planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT d, c, b FROM tmp");
}

// Runs a pipeline with read + write
TEST_F(TableWriteTest, directReadWrite) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType_, filePaths.size(), 1000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .tableScan(rowType_)
                  .tableWrite(
                      rowType_->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType_->names(),
                              rowType_->children(),
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      WriteProtocol::CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query

  assertQuery(
      PlanBuilder().tableScan(rowType_).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");
}

// Tests writing constant vectors
TEST_F(TableWriteTest, constantVectors) {
  vector_size_t size = 1'000;

  // Make constant vectors of various types with null and non-null values.
  std::string somewhatLongString = "Somewhat long string";
  auto vector = makeRowVector({
      createConstant((int64_t)123'456, size),
      createConstant(variant(TypeKind::BIGINT), size),
      createConstant((int32_t)12'345, size),
      createConstant(variant(TypeKind::INTEGER), size),
      createConstant((int16_t)1'234, size),
      createConstant(variant(TypeKind::SMALLINT), size),
      createConstant((int8_t)123, size),
      createConstant(variant(TypeKind::TINYINT), size),
      createConstant(true, size),
      createConstant(false, size),
      createConstant(variant(TypeKind::BOOLEAN), size),
      createConstant(somewhatLongString.c_str(), size),
      createConstant(variant(TypeKind::VARCHAR), size),
  });
  auto rowType = std::dynamic_pointer_cast<const RowType>(vector->type());

  createDuckDbTable({vector});

  auto outputDirectory = TempDirectoryPath::create();
  auto op = PlanBuilder()
                .values({vector})
                .tableWrite(
                    rowType->names(),
                    std::make_shared<core::InsertTableHandle>(
                        kHiveConnectorId,
                        makeHiveInsertTableHandle(
                            rowType_->names(),
                            rowType_->children(),
                            {},
                            makeLocationHandle(outputDirectory->path))),
                    WriteProtocol::CommitStrategy::kNoCommit,
                    "rows")
                .project({"rows"})
                .planNode();

  assertQuery(op, fmt::format("SELECT {}", size));

  assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");
}

TEST_F(TableWriteTest, TestASecondCommitStrategy) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType_, filePaths.size(), 1000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .tableScan(rowType_)
                  .tableWrite(
                      rowType_->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType_->names(),
                              rowType_->children(),
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      WriteProtocol::CommitStrategy::kTaskCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  // No write protocol is registered for CommitStrategy::kTaskCommit.
  VELOX_ASSERT_THROW(
      assertQuery(plan, filePaths, "SELECT count(*) FROM tmp"),
      "No write protocol found for commit strategy TASK_COMMIT");

  // HiveTaskCommitWriteProtocol is registered for CommitStrategy::kTaskCommit.
  HiveTaskCommitWriteProtocol::registerProtocol();
  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  // HiveTaskCommitWriteProtocol writes to dot-prefixed file in the
  // outputDirectory which is still picked up by table scan.
  assertQuery(
      PlanBuilder().tableScan(rowType_).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");
}

// Test TableWriter does not create a file if input is empty.
TEST_F(TableWriteTest, writeNoFile) {
  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .tableScan(rowType_)
                  .filter("false")
                  .tableWrite(
                      rowType_->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType_->names(),
                              rowType_->children(),
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      WriteProtocol::CommitStrategy::kNoCommit,
                      "rows")
                  .planNode();

  auto execute = [&](const std::shared_ptr<const core::PlanNode>& plan,
                     std::shared_ptr<core::QueryCtx> queryCtx) {
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = queryCtx;
    readCursor(params, [&](Task* task) { task->noMoreSplits("0"); });
  };

  execute(plan, std::make_shared<core::QueryCtx>(executor_.get()));
  ASSERT_TRUE(fs::is_empty(outputDirectory->path));
}
