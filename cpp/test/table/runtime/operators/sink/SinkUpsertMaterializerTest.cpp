/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * Unit tests for SinkUpsertMaterializer (retract -> upsert materialization before
 * the sink, native port of org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer).
 * Design: docs/detailed-design/支持SinkUpsertMaterializer_native算子详细设计.md
 * Remote run: tneltest --gtest_filter='SinkUpsertMaterializerTest.*'
 */

#include "table/runtime/operators/sink/SinkUpsertMaterializer.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "table/runtime/operators/TableOperatorConstants.h"
#include "core/graph/OperatorConfig.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/EmbeddedRocksDBStateBackend.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/RowKind.h"
#include "table/typeutils/RowDataSerializer.h"
#include "../../../../core/operators/OutputTest.h"

using namespace omnistream;
using json = nlohmann::json;

namespace {

const std::string kDescription = R"DELIM({
    "originDescription": null,
    "inputTypes": ["BIGINT", "BIGINT"],
    "outputTypes": ["BIGINT", "BIGINT"],
    "grouping": [0],
    "inputUpsertKey": null
})DELIM";

using KeyedOp = KeyedProcessOperator<RowData*, RowData*, omnistream::VectorBatch*>;

enum class BackendKind { HEAP, ROCKSDB };

KeyedOp* CreateMaterializer(BatchOutputTest* output, const std::string& description, BackendKind backend = BackendKind::HEAP)
{
    json desc = json::parse(description);
    OperatorConfig opConfig(
        std::string(omnistream::OPERATOR_NAME_SINK_UPSERT_MATERIALIZER),
        "SinkMaterializer(pk=[pk])",
        desc["inputTypes"],
        desc["outputTypes"],
        desc);
    omnistream::StreamOperatorFactory factory;
    auto* keyedOp = dynamic_cast<KeyedOp*>(factory.createOperatorAndCollector(opConfig, output));
    EXPECT_NE(keyedOp, nullptr);

    auto* env = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    StreamTaskStateInitializerImpl* initializer;
    if (backend == BackendKind::ROCKSDB) {
        const auto rocksPath =
            std::filesystem::temp_directory_path() / "sink-materializer-rocksdb";
        std::filesystem::remove_all(rocksPath);
        taskInfo->setStateBackend("EmbeddedRocksDBStateBackend");
        taskInfo->setRocksdbStorePaths({rocksPath.string()});
        auto jobId = omnistream::JobIDPOD(1, 2);
        auto jobVertexId = omnistream::JobVertexID(3, 4);
        auto attemptId = omnistream::ExecutionAttemptIDPOD::randomId();
        auto* stateStore = new TaskLocalStateStore(jobId, jobVertexId, 0, nullptr);
        auto* responder = new omnistream::NoOpCheckpoingResponder();
        env->SetTaskStateManager(
            std::make_shared<omnistream::TaskStateManager>(jobId, attemptId, stateStore, responder));
        env->setTaskConfiguration(*taskInfo);
        auto* rocksBackend = new EmbeddedRocksDBStateBackend(*taskInfo);
        initializer = new StreamTaskStateInitializerImpl(rocksBackend, env);
    } else {
        taskInfo->setStateBackend("HashMapStateBackend");
        env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
        env->setTaskConfiguration(*taskInfo);
        initializer = new StreamTaskStateInitializerImpl(env);
    }
    auto* typeInfo =
        new std::vector<omnistream::RowField>({omnistream::RowField("pk", BasicLogicalType::BIGINT)});
    TypeSerializer* keySerializer = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, keySerializer);
    keyedOp->open();
    return keyedOp;
}

omnistream::VectorBatch* MakeBatch(
    const std::vector<int64_t>& pk, const std::vector<int64_t>& val, const std::vector<RowKind>& kinds)
{
    const int rowCount = static_cast<int>(pk.size());
    auto* vbatch = new omnistream::VectorBatch(rowCount);
    auto* vPk = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto* vVal = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int i = 0; i < rowCount; ++i) {
        vPk->SetValue(i, pk[i]);
        if (val[i] < 0) {
            vVal->SetNull(i);
        } else {
            vVal->SetValue(i, val[i]);
        }
        vbatch->setRowKind(i, kinds[i]);
    }
    vbatch->Append(vPk);
    vbatch->Append(vVal);
    return vbatch;
}

bool LongColEquals(omniruntime::vec::BaseVector* left, omniruntime::vec::BaseVector* right)
{
    if (left->GetSize() != right->GetSize() || left->GetNullCount() != right->GetNullCount()) {
        return false;
    }
    auto* l = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(left);
    auto* r = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(right);
    for (int i = 0; i < l->GetSize(); ++i) {
        if (l->IsNull(i) != r->IsNull(i)) {
            return false;
        }
        if (!l->IsNull(i) && l->GetValue(i) != r->GetValue(i)) {
            return false;
        }
    }
    return true;
}

bool OutputEquals(omnistream::VectorBatch* actual, omnistream::VectorBatch* expected)
{
    if (actual->GetVectorCount() != expected->GetVectorCount() ||
        actual->GetRowCount() != expected->GetRowCount()) {
        return false;
    }
    for (int i = 0; i < actual->GetRowCount(); ++i) {
        if (actual->getRowKind(i) != expected->getRowKind(i)) {
            return false;
        }
    }
    return LongColEquals(actual->Get(0), expected->Get(0)) && LongColEquals(actual->Get(1), expected->Get(1));
}

} // namespace

// Add / retract lifecycle of one key plus a second key, covering:
// first add -> +I, second add -> +U, retract-latest -> re-emit second-latest as +U,
// retract-to-empty -> -D, retract with empty state swallowed.
TEST(SinkUpsertMaterializerTest, AddRetractLifecycle)
{
    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, kDescription);

    // M1: first row of key 1 emits +I.
    op->processBatch(new StreamRecord(MakeBatch({1}, {100}, {RowKind::INSERT})));
    auto* expect1 = MakeBatch({1}, {100}, {RowKind::INSERT});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect1));

    // M2: second row of key 1 emits +U (no upsert key: append).
    op->processBatch(new StreamRecord(MakeBatch({1}, {200}, {RowKind::UPDATE_AFTER})));
    auto* expect2 = MakeBatch({1}, {200}, {RowKind::UPDATE_AFTER});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect2));

    // M3: first row of key 2 emits +I.
    op->processBatch(new StreamRecord(MakeBatch({2}, {50}, {RowKind::INSERT})));
    auto* expect3 = MakeBatch({2}, {50}, {RowKind::INSERT});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect3));

    // M4: retract the latest row of key 1 -> list keeps (100) -> re-emit it as +U.
    op->processBatch(new StreamRecord(MakeBatch({1}, {200}, {RowKind::DELETE})));
    auto* expect4 = MakeBatch({1}, {100}, {RowKind::UPDATE_AFTER});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect4));

    // M5: retract the remaining row -> list becomes empty -> emit -D.
    op->processBatch(new StreamRecord(MakeBatch({1}, {100}, {RowKind::UPDATE_BEFORE})));
    auto* expect5 = MakeBatch({1}, {100}, {RowKind::DELETE});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect5));

    // M6: retract for a key with empty state is swallowed (0-row output batch).
    op->processBatch(new StreamRecord(MakeBatch({1}, {100}, {RowKind::DELETE})));
    EXPECT_EQ(0, output->getVectorBatch()->GetRowCount());

    // M7: after the swallowed retract a new row of key 1 emits +I again.
    op->processBatch(new StreamRecord(MakeBatch({1}, {300}, {RowKind::INSERT})));
    auto* expect7 = MakeBatch({1}, {300}, {RowKind::INSERT});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect7));

    delete expect1;
    delete expect2;
    delete expect3;
    delete expect4;
    delete expect5;
    delete expect7;
    delete op;
    delete output;
}

// Several rows of one key inside a single batch, plus middle-row removal and
// latest-row removal (re-emitting the new latest row as +U needs a non-empty
// remainder, so the batch keeps three rows before the retracts start).
TEST(SinkUpsertMaterializerTest, MultipleRowsInOneBatchAndMiddleRemoval)
{
    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, kDescription);

    // Three rows of key 3 in one batch: +I then +U then +U.
    op->processBatch(new StreamRecord(MakeBatch(
        {3, 3, 3}, {10, 20, 30}, {RowKind::INSERT, RowKind::UPDATE_AFTER, RowKind::UPDATE_AFTER})));
    auto* expect1 = MakeBatch(
        {3, 3, 3}, {10, 20, 30}, {RowKind::INSERT, RowKind::UPDATE_AFTER, RowKind::UPDATE_AFTER});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect1));

    // Remove the middle row (10): no emission at all.
    op->processBatch(new StreamRecord(MakeBatch({3}, {10}, {RowKind::DELETE})));
    EXPECT_EQ(0, output->getVectorBatch()->GetRowCount());

    // Remove the latest row (30): the list keeps (20) -> re-emit it as +U.
    op->processBatch(new StreamRecord(MakeBatch({3}, {30}, {RowKind::DELETE})));
    auto* expect3 = MakeBatch({3}, {20}, {RowKind::UPDATE_AFTER});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect3));

    // Remove the remaining row: list becomes empty -> emit -D.
    op->processBatch(new StreamRecord(MakeBatch({3}, {20}, {RowKind::DELETE})));
    auto* expect4 = MakeBatch({3}, {20}, {RowKind::DELETE});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect4));

    delete expect1;
    delete expect3;
    delete expect4;
    delete op;
    delete output;
}

// inputUpsertKey present: an add replaces the stored row with the same upsert key
// instead of appending; retracts match on the upsert-key projection.
TEST(SinkUpsertMaterializerTest, UpsertKeyReplace)
{
    const std::string upsertKeyDescription = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
        "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
        "grouping": [0],
        "inputUpsertKey": [1]
    })DELIM";

    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, upsertKeyDescription);

    auto makeBatch = [](const std::vector<int64_t>& pk, const std::vector<int64_t>& uk,
                        const std::vector<int64_t>& val, const std::vector<RowKind>& kinds) {
        const int rowCount = static_cast<int>(pk.size());
        auto* vbatch = new omnistream::VectorBatch(rowCount);
        auto* vPk = new omniruntime::vec::Vector<int64_t>(rowCount);
        auto* vUk = new omniruntime::vec::Vector<int64_t>(rowCount);
        auto* vVal = new omniruntime::vec::Vector<int64_t>(rowCount);
        for (int i = 0; i < rowCount; ++i) {
            vPk->SetValue(i, pk[i]);
            vUk->SetValue(i, uk[i]);
            vVal->SetValue(i, val[i]);
            vbatch->setRowKind(i, kinds[i]);
        }
        vbatch->Append(vPk);
        vbatch->Append(vUk);
        vbatch->Append(vVal);
        return vbatch;
    };

    // First row of key 5: +I.
    op->processBatch(new StreamRecord(makeBatch({5}, {7}, {100}, {RowKind::INSERT})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::INSERT, output->getVectorBatch()->getRowKind(0));

    // Same upsert key (uk=7): replaces the stored row, emits +U (not appended).
    op->processBatch(new StreamRecord(makeBatch({5}, {7}, {200}, {RowKind::UPDATE_AFTER})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::UPDATE_AFTER, output->getVectorBatch()->getRowKind(0));
    EXPECT_EQ(
        200,
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(output->getVectorBatch()->Get(2))->GetValue(0));

    // New upsert key (uk=8): appended, emits +U.
    op->processBatch(new StreamRecord(makeBatch({5}, {8}, {300}, {RowKind::UPDATE_AFTER})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::UPDATE_AFTER, output->getVectorBatch()->getRowKind(0));

    // New upsert key (uk=9): appended, emits +U. The list now holds three rows,
    // so a later middle removal still leaves two rows for the re-emit path.
    op->processBatch(new StreamRecord(makeBatch({5}, {9}, {400}, {RowKind::UPDATE_AFTER})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::UPDATE_AFTER, output->getVectorBatch()->getRowKind(0));

    // Retract matches on the upsert-key projection (uk=7): removes the first
    // (middle) row -> no emission.
    op->processBatch(new StreamRecord(makeBatch({5}, {7}, {200}, {RowKind::DELETE})));
    EXPECT_EQ(0, output->getVectorBatch()->GetRowCount());

    // Retract the latest row (uk=9): the list keeps (uk=8, val=300) -> re-emit
    // it as +U (the re-emit path needs a non-empty remainder).
    op->processBatch(new StreamRecord(makeBatch({5}, {9}, {400}, {RowKind::DELETE})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::UPDATE_AFTER, output->getVectorBatch()->getRowKind(0));
    EXPECT_EQ(
        300,
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(output->getVectorBatch()->Get(2))->GetValue(0));

    // Retract the remaining row (uk=8): list becomes empty -> emit -D.
    op->processBatch(new StreamRecord(makeBatch({5}, {8}, {300}, {RowKind::DELETE})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::DELETE, output->getVectorBatch()->getRowKind(0));
    EXPECT_EQ(
        300,
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(output->getVectorBatch()->Get(2))->GetValue(0));

    delete op;
    delete output;
}

// Null values survive the materialization round trip.
TEST(SinkUpsertMaterializerTest, NullValuePreserved)
{
    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, kDescription);

    // val encoded as -1 in MakeBatch means null.
    op->processBatch(new StreamRecord(MakeBatch({4}, {-1}, {RowKind::INSERT})));
    auto* actual = output->getVectorBatch();
    ASSERT_EQ(1, actual->GetRowCount());
    EXPECT_EQ(RowKind::INSERT, actual->getRowKind(0));
    EXPECT_TRUE(actual->Get(1)->IsNull(0));
    EXPECT_EQ(4, reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(actual->Get(0))->GetValue(0));

    delete op;
    delete output;
}

// ---------------------------------------------------------------------------
// Shared scenario bodies, run once per backend: the RocksDB path must produce
// exactly the same changelog as HEAP (batch-end write-back is transparent).
// ---------------------------------------------------------------------------

namespace {

// Add / retract lifecycle of one key plus a second key: +I / +U / re-emit
// second-latest +U / -D / swallowed retract / +I again.
void RunAddRetractLifecycle(KeyedOp* op, BatchOutputTest* output)
{
    op->processBatch(new StreamRecord(MakeBatch({1}, {100}, {RowKind::INSERT})));
    auto* expect1 = MakeBatch({1}, {100}, {RowKind::INSERT});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect1));

    op->processBatch(new StreamRecord(MakeBatch({1}, {200}, {RowKind::UPDATE_AFTER})));
    auto* expect2 = MakeBatch({1}, {200}, {RowKind::UPDATE_AFTER});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect2));

    op->processBatch(new StreamRecord(MakeBatch({2}, {50}, {RowKind::INSERT})));
    auto* expect3 = MakeBatch({2}, {50}, {RowKind::INSERT});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect3));

    op->processBatch(new StreamRecord(MakeBatch({1}, {200}, {RowKind::DELETE})));
    auto* expect4 = MakeBatch({1}, {100}, {RowKind::UPDATE_AFTER});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect4));

    op->processBatch(new StreamRecord(MakeBatch({1}, {100}, {RowKind::UPDATE_BEFORE})));
    auto* expect5 = MakeBatch({1}, {100}, {RowKind::DELETE});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect5));

    op->processBatch(new StreamRecord(MakeBatch({1}, {100}, {RowKind::DELETE})));
    EXPECT_EQ(0, output->getVectorBatch()->GetRowCount());

    op->processBatch(new StreamRecord(MakeBatch({1}, {300}, {RowKind::INSERT})));
    auto* expect7 = MakeBatch({1}, {300}, {RowKind::INSERT});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect7));

    delete expect1;
    delete expect2;
    delete expect3;
    delete expect4;
    delete expect5;
    delete expect7;
}

// Three same-key rows in one batch (chaining through the per-key working set),
// then middle removal (silent), latest removal (re-emit +U) and removal to empty (-D).
void RunMultipleRowsInOneBatchAndMiddleRemoval(KeyedOp* op, BatchOutputTest* output)
{
    op->processBatch(new StreamRecord(MakeBatch(
        {3, 3, 3}, {10, 20, 30}, {RowKind::INSERT, RowKind::UPDATE_AFTER, RowKind::UPDATE_AFTER})));
    auto* expect1 = MakeBatch(
        {3, 3, 3}, {10, 20, 30}, {RowKind::INSERT, RowKind::UPDATE_AFTER, RowKind::UPDATE_AFTER});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect1));

    op->processBatch(new StreamRecord(MakeBatch({3}, {10}, {RowKind::DELETE})));
    EXPECT_EQ(0, output->getVectorBatch()->GetRowCount());

    op->processBatch(new StreamRecord(MakeBatch({3}, {30}, {RowKind::DELETE})));
    auto* expect3 = MakeBatch({3}, {20}, {RowKind::UPDATE_AFTER});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect3));

    op->processBatch(new StreamRecord(MakeBatch({3}, {20}, {RowKind::DELETE})));
    auto* expect4 = MakeBatch({3}, {20}, {RowKind::DELETE});
    EXPECT_TRUE(OutputEquals(output->getVectorBatch(), expect4));

    delete expect1;
    delete expect3;
    delete expect4;
}

const char* kUpsertKeyDescription = R"DELIM({
    "originDescription": null,
    "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
    "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
    "grouping": [0],
    "inputUpsertKey": [1]
})DELIM";

// inputUpsertKey: replace-in-place on the upsert key, retract matches on the
// projection, three accumulated rows then middle/latest/empty removals.
void RunUpsertKeyReplace(KeyedOp* op, BatchOutputTest* output)
{
    auto makeBatch = [](const std::vector<int64_t>& pk, const std::vector<int64_t>& uk,
                        const std::vector<int64_t>& val, const std::vector<RowKind>& kinds) {
        const int rowCount = static_cast<int>(pk.size());
        auto* vbatch = new omnistream::VectorBatch(rowCount);
        auto* vPk = new omniruntime::vec::Vector<int64_t>(rowCount);
        auto* vUk = new omniruntime::vec::Vector<int64_t>(rowCount);
        auto* vVal = new omniruntime::vec::Vector<int64_t>(rowCount);
        for (int i = 0; i < rowCount; ++i) {
            vPk->SetValue(i, pk[i]);
            vUk->SetValue(i, uk[i]);
            vVal->SetValue(i, val[i]);
            vbatch->setRowKind(i, kinds[i]);
        }
        vbatch->Append(vPk);
        vbatch->Append(vUk);
        vbatch->Append(vVal);
        return vbatch;
    };
    auto col2 = [output]() {
        return reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(output->getVectorBatch()->Get(2));
    };

    op->processBatch(new StreamRecord(makeBatch({5}, {7}, {100}, {RowKind::INSERT})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::INSERT, output->getVectorBatch()->getRowKind(0));

    op->processBatch(new StreamRecord(makeBatch({5}, {7}, {200}, {RowKind::UPDATE_AFTER})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::UPDATE_AFTER, output->getVectorBatch()->getRowKind(0));
    EXPECT_EQ(200, col2()->GetValue(0));

    op->processBatch(new StreamRecord(makeBatch({5}, {8}, {300}, {RowKind::UPDATE_AFTER})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::UPDATE_AFTER, output->getVectorBatch()->getRowKind(0));

    op->processBatch(new StreamRecord(makeBatch({5}, {9}, {400}, {RowKind::UPDATE_AFTER})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::UPDATE_AFTER, output->getVectorBatch()->getRowKind(0));

    op->processBatch(new StreamRecord(makeBatch({5}, {7}, {200}, {RowKind::DELETE})));
    EXPECT_EQ(0, output->getVectorBatch()->GetRowCount());

    op->processBatch(new StreamRecord(makeBatch({5}, {9}, {400}, {RowKind::DELETE})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::UPDATE_AFTER, output->getVectorBatch()->getRowKind(0));
    EXPECT_EQ(300, col2()->GetValue(0));

    op->processBatch(new StreamRecord(makeBatch({5}, {8}, {300}, {RowKind::DELETE})));
    ASSERT_EQ(1, output->getVectorBatch()->GetRowCount());
    EXPECT_EQ(RowKind::DELETE, output->getVectorBatch()->getRowKind(0));
    EXPECT_EQ(300, col2()->GetValue(0));
}

void RunNullValuePreserved(KeyedOp* op, BatchOutputTest* output)
{
    op->processBatch(new StreamRecord(MakeBatch({4}, {-1}, {RowKind::INSERT})));
    auto* actual = output->getVectorBatch();
    ASSERT_EQ(1, actual->GetRowCount());
    EXPECT_EQ(RowKind::INSERT, actual->getRowKind(0));
    EXPECT_TRUE(actual->Get(1)->IsNull(0));
    EXPECT_EQ(4, reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(actual->Get(0))->GetValue(0));
}

} // namespace

// The same four scenarios on EmbeddedRocksDBStateBackend: identical changelog,
// exercising the batch-end write-back (deserialize copy -> mutate -> serialize
// back -> free copy) and the falcon-cache-off ownership path.
TEST(SinkUpsertMaterializerRocksDBTest, AddRetractLifecycle)
{
    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, kDescription, BackendKind::ROCKSDB);
    RunAddRetractLifecycle(op, output);
    // RocksDB: deleting the operator crashes the framework teardown (same as the
    // existing RowTimeDeduplicateFunctionTest RocksDB cases, which never delete op).
    delete output;
}

TEST(SinkUpsertMaterializerRocksDBTest, MultipleRowsInOneBatchAndMiddleRemoval)
{
    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, kDescription, BackendKind::ROCKSDB);
    RunMultipleRowsInOneBatchAndMiddleRemoval(op, output);
    delete output;
}

TEST(SinkUpsertMaterializerRocksDBTest, UpsertKeyReplace)
{
    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, kUpsertKeyDescription, BackendKind::ROCKSDB);
    RunUpsertKeyReplace(op, output);
    delete output;
}

TEST(SinkUpsertMaterializerRocksDBTest, NullValuePreserved)
{
    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, kDescription, BackendKind::ROCKSDB);
    RunNullValuePreserved(op, output);
    // RocksDB: deleting the operator crashes the framework teardown (same as the
    // existing RowTimeDeduplicateFunctionTest RocksDB cases, which never delete op).
    delete output;
}

// With the falcon value-state cache enabled the loaded lists stay owned by the
// cache; the operator must not free them and the write-back keeps working.
TEST(SinkUpsertMaterializerRocksDBCacheTest, AddRetractLifecycle)
{
    struct FalconConfigGuard {
        FalconConfigGuard()
        {
            Configuration::TM_CONFIG->setConfiguration(
                {{"state.backend.rocksdb.falcon.use-state-cache", "true"},
                 {"state.backend.rocksdb.falcon.state-cache-sizeLimit", "1"}});
        }
        ~FalconConfigGuard()
        {
            Configuration::TM_CONFIG->setConfiguration(
                {{"state.backend.rocksdb.falcon.use-state-cache", "false"},
                 {"state.backend.rocksdb.falcon.state-cache-sizeLimit", "3000"}});
        }
    } falconConfigGuard;

    auto* output = new BatchOutputTest();
    auto* op = CreateMaterializer(output, kDescription, BackendKind::ROCKSDB);
    RunAddRetractLifecycle(op, output);
    delete output;
}

