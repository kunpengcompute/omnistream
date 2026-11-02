/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * Unit tests for the streaming Limit shape (StreamExecLimit -> Rank with
 * PartitionSpec.ALL_IN_ONE + SortSpec.ANY): empty partitionKey and empty
 * sortFieldIndices. Covers AppendOnlyTopNFunction / FastTop1Function
 * (insert-only input) and RetractableTopNFunction (input with retracts).
 * Design: docs/detailed-design/支持流式Limit_native算子详细设计.md
 * Remote run: tneltest --gtest_filter='StreamLimitTest.*'
 */

#include <gtest/gtest.h>
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "table/runtime/operators/rank/FastTop1Function.h"
#include "table/runtime/operators/rank/RetractableTopNFunction.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>

namespace {

// Mirrors the JSON written by the shadowed StreamExecRank overlay for a
// StreamExecLimit node: ALL_IN_ONE -> partitionKey [], SortSpec.ANY ->
// sortFieldIndices [], outputRankNumber always false.
std::string MakeLimitConfig(const std::string& rankRange, const std::string& processFunction, bool generateUpdateBefore)
{
    return R"DELIM({"originDescription":null,
"inputTypes":["BIGINT", "BIGINT"],
"outputTypes":["BIGINT", "BIGINT"],
"partitionKey":[],
"outputRankNumber":false,
"rankRange":")DELIM" + rankRange + R"DELIM(",
"generateUpdateBefore":)DELIM" + (generateUpdateBefore ? "true" : "false") + R"DELIM(,
"processFunction":")DELIM" + processFunction + R"DELIM(",
"sortFieldIndices":[],
"sortAscendingOrders":[],
"sortNullsIsLast":[]})DELIM";
}

struct LimitFixture {
    KeyedProcessOperator<RowData*, RowData*, RowData*>* op = nullptr;
    BatchOutputTest* output = nullptr;
};

template <typename Func>
LimitFixture MakeOperator(const std::string& description)
{
    LimitFixture fixture;
    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    auto* func = new Func(rankConfig);
    fixture.output = new BatchOutputTest();
    fixture.op = new KeyedProcessOperator(
        reinterpret_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(func), fixture.output, rankConfig);
    fixture.op->setup();
    auto* env2 = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env2);
    // The streaming Limit key is the arity-0 row (ALL_IN_ONE).
    TypeSerializer* ser = new BinaryRowDataSerializer(0);
    fixture.op->initializeState(initializer, ser);
    EXPECT_NO_THROW(fixture.op->open());
    return fixture;
}

omnistream::VectorBatch* MakeBatch(const std::vector<int64_t>& ids, const std::vector<int64_t>& vals)
{
    const int rowCnt = static_cast<int>(ids.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(ids.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(vals.data())));
    return vb;
}

omnistream::VectorBatch* MakeBatchWithKinds(
    const std::vector<int64_t>& ids, const std::vector<int64_t>& vals, const std::vector<RowKind>& kinds)
{
    auto* vb = MakeBatch(ids, vals);
    std::vector<RowKind> kindBuf = kinds;
    vb->setRowKinds(0, kindBuf.data(), static_cast<int>(kindBuf.size()));
    return vb;
}

// BatchOutputTest keeps the LAST collected record: an unchanged record pointer
// across a processBatch call means nothing was emitted.
bool NothingEmitted(BatchOutputTest* output, void* recordBefore)
{
    return output->getStreamRecord() == recordBefore;
}

void ExpectRow(omnistream::VectorBatch* vb, int row, RowKind kind, int64_t id, int64_t val)
{
    EXPECT_EQ(kind, vb->getRowKind(row)) << "row " << row;
    EXPECT_EQ(id, reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vb->Get(0))->GetValue(row)) << "row " << row;
    EXPECT_EQ(val, reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vb->Get(1))->GetValue(row)) << "row " << row;
}

} // namespace

// L1: empty partitionKey/sortFieldIndices parse (previously threw) and
// LIMIT 3 keeps the first three rows in arrival order (FirstN semantics).
TEST(StreamLimitTest, AppendOnlyTopNLimitNKeepsFirstRows)
{
    auto fixture = MakeOperator<AppendOnlyTopNFunction<RowData*>>(
        MakeLimitConfig("rankStart=1, rankEnd=3", "AppendOnlyTopNFunction", false));
    fixture.op->processBatch(new StreamRecord(MakeBatch({1, 2, 3, 4, 5}, {10, 20, 30, 40, 50})));

    auto* actual = fixture.output->getVectorBatch();
    ASSERT_NE(actual, nullptr);
    ASSERT_EQ(3, actual->GetRowCount());
    ExpectRow(actual, 0, RowKind::INSERT, 1, 10);
    ExpectRow(actual, 1, RowKind::INSERT, 2, 20);
    ExpectRow(actual, 2, RowKind::INSERT, 3, 30);
}

// L2: offset window - rows in [1, rankStart) are counted but not emitted.
TEST(StreamLimitTest, AppendOnlyTopNOffsetSkipsLeadingRows)
{
    auto fixture = MakeOperator<AppendOnlyTopNFunction<RowData*>>(
        MakeLimitConfig("rankStart=3, rankEnd=4", "AppendOnlyTopNFunction", false));
    fixture.op->processBatch(new StreamRecord(MakeBatch({1, 2, 3, 4, 5}, {10, 20, 30, 40, 50})));

    auto* actual = fixture.output->getVectorBatch();
    ASSERT_NE(actual, nullptr);
    ASSERT_EQ(2, actual->GetRowCount());
    ExpectRow(actual, 0, RowKind::INSERT, 3, 30);
    ExpectRow(actual, 1, RowKind::INSERT, 4, 40);
}

// L3: the row counter carries across batches; after rankEnd no more output.
TEST(StreamLimitTest, AppendOnlyTopNCrossBatchCounter)
{
    auto fixture = MakeOperator<AppendOnlyTopNFunction<RowData*>>(
        MakeLimitConfig("rankStart=1, rankEnd=3", "AppendOnlyTopNFunction", false));
    fixture.op->processBatch(new StreamRecord(MakeBatch({1, 2}, {10, 20})));
    auto* actual1 = fixture.output->getVectorBatch();
    ASSERT_EQ(2, actual1->GetRowCount());

    fixture.op->processBatch(new StreamRecord(MakeBatch({3, 4, 5}, {30, 40, 50})));
    auto* actual2 = fixture.output->getVectorBatch();
    ASSERT_EQ(1, actual2->GetRowCount());
    ExpectRow(actual2, 0, RowKind::INSERT, 3, 30);
}

// L4: LIMIT 1 goes through FastTop1Function: with an empty sort key the
// comparison loop runs zero times (all equal), so the first row wins.
TEST(StreamLimitTest, FastTop1Limit1KeepsFirstRow)
{
    auto fixture = MakeOperator<FastTop1Function<RowData*>>(
        MakeLimitConfig("rankStart=1, rankEnd=1", "FastTop1Function", false));
    fixture.op->processBatch(new StreamRecord(MakeBatch({1, 2, 3}, {10, 20, 30})));
    auto* actual1 = fixture.output->getVectorBatch();
    ASSERT_NE(actual1, nullptr);
    ASSERT_EQ(1, actual1->GetRowCount());
    ExpectRow(actual1, 0, RowKind::INSERT, 1, 10);

    // a later batch must not replace the winner (empty sort key -> equal):
    // Flink's FastTop1 emits nothing when the new row does not beat the stored
    // top-1. The native FastTop1 still flushes an empty output batch, so the
    // assertion is on the row count, not on the record pointer.
    fixture.op->processBatch(new StreamRecord(MakeBatch({4}, {40})));
    ASSERT_EQ(0, fixture.output->getVectorBatch()->GetRowCount());
}

// R1: LIMIT 3 on a retract stream. Insert 4 rows -> first 3 emitted; retract
// the 2nd row -> DELETE it and INSERT the promoted 4th row (window stays full).
TEST(StreamLimitTest, RetractBasicDeleteAndPromotion)
{
    auto fixture = MakeOperator<RetractableTopNFunction<RowData*>>(
        MakeLimitConfig("rankStart=1, rankEnd=3", "RetractableTopNFunction", false));
    fixture.op->processBatch(new StreamRecord(MakeBatch({1, 2, 3, 4}, {10, 20, 30, 40})));
    auto* actual1 = fixture.output->getVectorBatch();
    ASSERT_EQ(3, actual1->GetRowCount());

    fixture.op->processBatch(new StreamRecord(MakeBatchWithKinds({2}, {20}, {RowKind::DELETE})));
    auto* actual2 = fixture.output->getVectorBatch();
    ASSERT_EQ(2, actual2->GetRowCount());
    ExpectRow(actual2, 0, RowKind::DELETE, 2, 20);
    ExpectRow(actual2, 1, RowKind::INSERT, 4, 40);
}

// R2: retract of a row beyond the window must be removed SILENTLY (the
// !stateRemoved fallback). If the row stayed in state, the later rank-1
// retract would resurrect it via promotion.
TEST(StreamLimitTest, RetractBeyondWindowSilentFallback)
{
    auto fixture = MakeOperator<RetractableTopNFunction<RowData*>>(
        MakeLimitConfig("rankStart=1, rankEnd=2", "RetractableTopNFunction", false));
    fixture.op->processBatch(new StreamRecord(MakeBatch({1, 2, 3}, {10, 20, 30})));
    ASSERT_EQ(2, fixture.output->getVectorBatch()->GetRowCount());

    void* before = fixture.output->getStreamRecord();
    fixture.op->processBatch(new StreamRecord(MakeBatchWithKinds({3}, {30}, {RowKind::DELETE})));
    EXPECT_TRUE(NothingEmitted(fixture.output, before)); // silent removal

    fixture.op->processBatch(new StreamRecord(MakeBatchWithKinds({1}, {10}, {RowKind::DELETE})));
    auto* actual = fixture.output->getVectorBatch();
    ASSERT_EQ(1, actual->GetRowCount()); // exactly the DELETE, no +I of row 3
    ExpectRow(actual, 0, RowKind::DELETE, 1, 10);
}

// R3: OFFSET retract cascade (LIMIT 2 OFFSET 2, generateUpdateBefore). Removing
// the rank-3 row shifts ranks 4,5 down: UB/UA pairs for both window slots.
TEST(StreamLimitTest, RetractOffsetCascade)
{
    auto fixture = MakeOperator<RetractableTopNFunction<RowData*>>(
        MakeLimitConfig("rankStart=3, rankEnd=4", "RetractableTopNFunction", true));
    fixture.op->processBatch(new StreamRecord(MakeBatch({1, 2, 3, 4, 5}, {10, 20, 30, 40, 50})));
    ASSERT_EQ(2, fixture.output->getVectorBatch()->GetRowCount());

    fixture.op->processBatch(new StreamRecord(MakeBatchWithKinds({3}, {30}, {RowKind::DELETE})));
    auto* actual = fixture.output->getVectorBatch();
    ASSERT_EQ(4, actual->GetRowCount());
    ExpectRow(actual, 0, RowKind::UPDATE_BEFORE, 3, 30);
    ExpectRow(actual, 1, RowKind::UPDATE_AFTER, 4, 40);
    ExpectRow(actual, 2, RowKind::UPDATE_BEFORE, 4, 40);
    ExpectRow(actual, 3, RowKind::UPDATE_AFTER, 5, 50);
}
