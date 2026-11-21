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
 * Unit tests for the native RetractableTopNFunction operator
 * (cpp/table/runtime/operators/rank/RetractableTopNFunction.h). The scenarios are
 * ported 1:1 from Flink's RetractableTopNFunctionTest.java (input row type
 * VARCHAR(partition) / BIGINT / INT, partitionKey=[0], sortFieldIndices=[2]
 * ascending, Constant/Variable Rank Range, with/without row number and
 * generateUpdateBefore on/off). The input is fed as VectorBatch(es) with per-row
 * RowKind (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE) and the single/flattened
 * output batch is asserted row-by-row (RowKind + column values) in emission order,
 * which must match the original Flink expected changelog exactly.
 *
 * DATE sort columns (OMNI_DATE32) are additionally covered: DATE is stored and
 * compared as int32 exactly like Flink's DATE->getInt codegen, so the changelog
 * for a DATE sort column must be identical to the INT baseline. A DATE-keyed
 * "sorted-map" state round-trip pins the Flink-compatible checkpoint bytes.
 *
 * Deferred (see design doc):
 *  - testConstantRankRangeWithoutOffset{With,Without}RowNumber: the snapshot /
 *    restore halves exercise checkpoint recovery (out of scope for now).
 *  - RetractableTopNSavepointAdaptor restore (checkpoint/savepoint, out of scope).
 *
 * Remote run: tneltest --gtest_filter='RetractableTopNFunctionTest.*'
 */

#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <vector>

#include "table/runtime/operators/rank/RetractableTopNFunction.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/SortedMapRowDataLongSerializer.h"
#include "table/typeutils/SortedMapValue.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/data/RowKind.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>

namespace {

using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;

// One expected output row. rank == -1 means the operator did not output a rank column.
struct RtnRow {
    RowKind kind;
    std::string pk;
    int64_t id;
    int32_t sortVal;
    int64_t rank;
};

RtnRow I(const std::string& pk, int64_t id, int32_t sortVal, int64_t rank = -1)
{
    return {RowKind::INSERT, pk, id, sortVal, rank};
}
RtnRow UB(const std::string& pk, int64_t id, int32_t sortVal, int64_t rank = -1)
{
    return {RowKind::UPDATE_BEFORE, pk, id, sortVal, rank};
}
RtnRow UA(const std::string& pk, int64_t id, int32_t sortVal, int64_t rank = -1)
{
    return {RowKind::UPDATE_AFTER, pk, id, sortVal, rank};
}
RtnRow D(const std::string& pk, int64_t id, int32_t sortVal, int64_t rank = -1)
{
    return {RowKind::DELETE, pk, id, sortVal, rank};
}

// Mirrors the JSON written by the shadowed StreamExecRank overlay for a
// RetractStrategy Rank. The stream is keyed on column 0 and sorted on column 2.
std::string MakeRankConfig(
    bool outputRankNumber,
    bool generateUpdateBefore,
    const std::string& rankRange,
    const std::string& inputTypes = R"TYPES(["VARCHAR(2147483647)","BIGINT","INT"])TYPES")
{
    std::string json = std::string(R"({"originDescription":null,
"inputTypes":)") + inputTypes + R"(,
"outputTypes":)" + inputTypes + R"(,
"partitionKey":[0],
"outputRankNumber":)" + (outputRankNumber ? "true" : "false") + R"(,
"rankRange":")" + rankRange + R"(",
"generateUpdateBefore":)" + (generateUpdateBefore ? "true" : "false") +
        R"(,
"processFunction":"RetractableTopNFunction",
"sortFieldIndices":[2],
"sortAscendingOrders":[true],
"sortNullsIsLast":[true])";
    return json + R"(})";
}

omniruntime::vec::BaseVector* MakeVarcharVector(const std::vector<std::string>& values)
{
    auto* vec = new VarcharVector(
        static_cast<int>(values.size()), omniruntime::vec::INITIAL_STRING_SIZE,
        omniruntime::type::DataTypeId::OMNI_VARCHAR);
    for (size_t i = 0; i < values.size(); ++i) {
        vec->SetValue(static_cast<int>(i), std::string_view(values[i].data(), values[i].size()));
    }
    return vec;
}

// 3-column input batch: VARCHAR partition key, BIGINT id, INT sort value.
omnistream::VectorBatch* MakeBatch(
    const std::vector<std::string>& pk,
    const std::vector<int64_t>& id,
    const std::vector<int32_t>& sortVal,
    const std::vector<RowKind>& kinds)
{
    const int rowCnt = static_cast<int>(pk.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(MakeVarcharVector(pk));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(id.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int32_t*>(sortVal.data())));
    std::vector<RowKind> kindBuf = kinds;
    vb->setRowKinds(0, kindBuf.data(), static_cast<int>(kindBuf.size()));
    return vb;
}

// 3-column input batch with a DATE (OMNI_DATE32) sort column: VARCHAR partition
// key, BIGINT id, DATE sort value. The DATE column is an int32 Vector carrying the
// OMNI_DATE32 type id (extractRowData dispatches on the vector type), matching how
// DATE columns arrive from deserialization.
omnistream::VectorBatch* MakeBatchDate(
    const std::vector<std::string>& pk,
    const std::vector<int64_t>& id,
    const std::vector<int32_t>& dateVal,
    const std::vector<RowKind>& kinds)
{
    const int rowCnt = static_cast<int>(pk.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(MakeVarcharVector(pk));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(id.data())));
    auto* dateVec = new omniruntime::vec::Vector<int32_t>(rowCnt, omniruntime::type::DataTypeId::OMNI_DATE32);
    for (int i = 0; i < rowCnt; ++i) {
        dateVec->SetValue(i, dateVal[static_cast<size_t>(i)]);
    }
    vb->Append(dateVec);
    std::vector<RowKind> kindBuf = kinds;
    vb->setRowKinds(0, kindBuf.data(), static_cast<int>(kindBuf.size()));
    return vb;
}

// 4-column input batch (used by the retract-then-delete scenario): VARCHAR
// partition key, BIGINT id, INT sort value, INT payload.
omnistream::VectorBatch* MakeBatch4(
    const std::vector<std::string>& pk,
    const std::vector<int64_t>& id,
    const std::vector<int32_t>& sortVal,
    const std::vector<int32_t>& payload,
    const std::vector<RowKind>& kinds)
{
    const int rowCnt = static_cast<int>(pk.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(MakeVarcharVector(pk));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(id.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int32_t*>(sortVal.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int32_t*>(payload.data())));
    std::vector<RowKind> kindBuf = kinds;
    vb->setRowKinds(0, kindBuf.data(), static_cast<int>(kindBuf.size()));
    return vb;
}

KeyedProcessOperator<RowData*, RowData*, RowData*>* OpenRetractableTopN(
    const std::string& description, OutputTestVectorBatch* output)
{
    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    auto* topN = new RetractableTopNFunction<RowData*>(rankConfig);
    auto* func = reinterpret_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(topN);
    nlohmann::json newRankConfig = rankConfig;
    auto* op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
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
    // The partition key is a single-column row (VARCHAR column 0).
    TypeSerializer* ser = new BinaryRowDataSerializer(1);
    op->initializeState(initializer, ser);
    op->open();
    return op;
}

std::string ReadVarchar(omnistream::VectorBatch* vb, int col, int row)
{
    auto sv = reinterpret_cast<VarcharVector*>(vb->Get(col))->GetValue(row);
    return std::string(sv.data(), sv.size());
}

int64_t ReadLong(omnistream::VectorBatch* vb, int col, int row)
{
    return reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vb->Get(col))->GetValue(row);
}

int32_t ReadInt(omnistream::VectorBatch* vb, int col, int row)
{
    return reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vb->Get(col))->GetValue(row);
}

// Flatten every collected output batch into one arrival-order row stream.
std::vector<RtnRow> CollectRows(OutputTestVectorBatch* output)
{
    std::vector<RtnRow> rows;
    for (auto* vb : output->getAll()) {
        if (vb == nullptr) {
            ADD_FAILURE() << "null output batch in CollectRows";
            continue;
        }
        const bool hasRank = vb->GetVectorCount() > 3;
        for (int i = 0; i < vb->GetRowCount(); ++i) {
            RtnRow r;
            r.kind = vb->getRowKind(i);
            r.pk = ReadVarchar(vb, 0, i);
            r.id = ReadLong(vb, 1, i);
            r.sortVal = ReadInt(vb, 2, i);
            r.rank = hasRank ? ReadLong(vb, 3, i) : -1;
            rows.push_back(r);
        }
    }
    return rows;
}

void AssertRows(const std::vector<RtnRow>& actual, const std::vector<RtnRow>& expected)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].kind, actual[i].kind) << "rowKind mismatch at row " << i;
        EXPECT_EQ(expected[i].pk, actual[i].pk) << "pk mismatch at row " << i;
        EXPECT_EQ(expected[i].id, actual[i].id) << "id mismatch at row " << i;
        EXPECT_EQ(expected[i].sortVal, actual[i].sortVal) << "sortVal mismatch at row " << i;
        if (expected[i].rank >= 0) {
            EXPECT_EQ(expected[i].rank, actual[i].rank) << "rank mismatch at row " << i;
        }
    }
}

// Flink testProcessRetractMessageWithNotGenerateUpdateBefore:
// retracts drive UPDATE_AFTER-only shifts (no UPDATE_BEFORE).
TEST(RetractableTopNFunctionTest, ProcessRetractMessageWithoutGenerateUpdateBefore)
{
    const std::string desc = MakeRankConfig(true, false, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "book", "book", "fruit", "fruit", "fruit"},
        {1, 2, 4, 1, 5, 4, 3, 5},
        {12, 19, 11, 12, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::UPDATE_BEFORE,
         RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 1, 12, 1),
        I("book", 2, 19, 2),
        UA("book", 4, 11, 1),
        UA("book", 1, 12, 2),
        UA("book", 2, 19, 2),
        UA("book", 5, 11, 2),
        I("fruit", 4, 33, 1),
        I("fruit", 3, 44, 2),
        UA("fruit", 5, 22, 1),
        UA("fruit", 4, 33, 2)};
    AssertRows(CollectRows(output), expected);
}

// Flink testProcessRetractMessageWithGenerateUpdateBefore: every shift emits the
// full UPDATE_BEFORE/UPDATE_AFTER pair.
TEST(RetractableTopNFunctionTest, ProcessRetractMessageWithGenerateUpdateBefore)
{
    const std::string desc = MakeRankConfig(true, true, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "book", "book", "fruit", "fruit", "fruit"},
        {1, 2, 4, 1, 5, 4, 3, 5},
        {12, 19, 11, 12, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::UPDATE_BEFORE,
         RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 1, 12, 1),
        I("book", 2, 19, 2),
        UB("book", 1, 12, 1),
        UA("book", 4, 11, 1),
        UB("book", 2, 19, 2),
        UA("book", 1, 12, 2),
        UB("book", 1, 12, 2),
        UA("book", 2, 19, 2),
        UB("book", 2, 19, 2),
        UA("book", 5, 11, 2),
        I("fruit", 4, 33, 1),
        I("fruit", 3, 44, 2),
        UB("fruit", 4, 33, 1),
        UA("fruit", 5, 22, 1),
        UB("fruit", 3, 44, 2),
        UA("fruit", 4, 33, 2)};
    AssertRows(CollectRows(output), expected);
}

// DATE sort column variant of ProcessRetractMessageWithGenerateUpdateBefore: the
// sort column is DATE (OMNI_DATE32) instead of INT. DATE is stored/compared as
// int32 exactly like Flink's DATE->getInt codegen, so the expected changelog must be
// identical to the INT baseline. This exercises the DATE branches of SortKeyCmp
// (sorted-map ordering), projectSortKey, rowsEqual (the UPDATE_BEFORE retract) and
// createOutputBatch (DATE output column) end to end.
TEST(RetractableTopNFunctionTest, DateSortKeyProcessRetractMessageWithGenerateUpdateBefore)
{
    const std::string inputTypes = R"TYPES(["VARCHAR(2147483647)","BIGINT","DATE"])TYPES";
    const std::string desc = MakeRankConfig(true, true, "rankStart=1, rankEnd=2", inputTypes);
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatchDate(
        {"book", "book", "book", "book", "book", "fruit", "fruit", "fruit"},
        {1, 2, 4, 1, 5, 4, 3, 5},
        {12, 19, 11, 12, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::UPDATE_BEFORE,
         RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 1, 12, 1),
        I("book", 2, 19, 2),
        UB("book", 1, 12, 1),
        UA("book", 4, 11, 1),
        UB("book", 2, 19, 2),
        UA("book", 1, 12, 2),
        UB("book", 1, 12, 2),
        UA("book", 2, 19, 2),
        UB("book", 2, 19, 2),
        UA("book", 5, 11, 2),
        I("fruit", 4, 33, 1),
        I("fruit", 3, 44, 2),
        UB("fruit", 4, 33, 1),
        UA("fruit", 5, 22, 1),
        UB("fruit", 3, 44, 2),
        UA("fruit", 4, 33, 2)};
    AssertRows(CollectRows(output), expected);
}

// Flink testConstantRankRangeWithoutOffsetWithRowNumber (streaming half; the
// snapshot/restore half is checkpoint-related and deferred).
TEST(RetractableTopNFunctionTest, ConstantRankRangeWithoutOffsetWithRowNumber)
{
    const std::string desc = MakeRankConfig(true, true, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "fruit", "fruit", "fruit"},
        {1, 2, 4, 4, 3, 5},
        {12, 19, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 1, 12, 1),
        I("book", 2, 19, 2),
        UB("book", 1, 12, 1),
        UA("book", 4, 11, 1),
        UB("book", 2, 19, 2),
        UA("book", 1, 12, 2),
        I("fruit", 4, 33, 1),
        I("fruit", 3, 44, 2),
        UB("fruit", 4, 33, 1),
        UA("fruit", 5, 22, 1),
        UB("fruit", 3, 44, 2),
        UA("fruit", 4, 33, 2)};
    AssertRows(CollectRows(output), expected);
}

// Flink testConstantRankRangeWithoutOffsetWithoutRowNumber (streaming half).
TEST(RetractableTopNFunctionTest, ConstantRankRangeWithoutOffsetWithoutRowNumber)
{
    const std::string desc = MakeRankConfig(false, true, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "fruit", "fruit", "fruit"},
        {1, 2, 4, 4, 3, 5},
        {12, 19, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 1, 12),
        I("book", 2, 19),
        D("book", 2, 19),
        I("book", 4, 11),
        I("fruit", 4, 33),
        I("fruit", 3, 44),
        D("fruit", 3, 44),
        I("fruit", 5, 22)};
    AssertRows(CollectRows(output), expected);
}

// Flink testVariableRankRangeWithRowNumber: rank end is read from column 1 per
// partition (book=2, fruit=1) and frozen on the first record.
TEST(RetractableTopNFunctionTest, VariableRankRangeWithRowNumber)
{
    const std::string desc = MakeRankConfig(true, true, "rankEnd=$1");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "fruit", "fruit", "fruit"},
        {2, 2, 2, 1, 1, 1},
        {12, 19, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 2, 12, 1),
        I("book", 2, 19, 2),
        UB("book", 2, 12, 1),
        UA("book", 2, 11, 1),
        UB("book", 2, 19, 2),
        UA("book", 2, 12, 2),
        I("fruit", 1, 33, 1),
        UB("fruit", 1, 33, 1),
        UA("fruit", 1, 22, 1)};
    AssertRows(CollectRows(output), expected);
}

// Flink testVariableRankRangeWithoutRowNumber.
TEST(RetractableTopNFunctionTest, VariableRankRangeWithoutRowNumber)
{
    const std::string desc = MakeRankConfig(false, true, "rankEnd=$1");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "fruit", "fruit", "fruit"},
        {2, 2, 2, 1, 1, 1},
        {12, 19, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 2, 12),
        I("book", 2, 19),
        D("book", 2, 19),
        I("book", 2, 11),
        I("fruit", 1, 33),
        D("fruit", 1, 33),
        I("fruit", 1, 22)};
    AssertRows(CollectRows(output), expected);
}

// Flink testDisableGenerateUpdateBeforeWithRowNumber.
TEST(RetractableTopNFunctionTest, DisableGenerateUpdateBeforeWithRowNumber)
{
    const std::string desc = MakeRankConfig(true, false, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "fruit", "fruit", "fruit"},
        {1, 2, 4, 4, 3, 5},
        {12, 19, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 1, 12, 1),
        I("book", 2, 19, 2),
        UA("book", 4, 11, 1),
        UA("book", 1, 12, 2),
        I("fruit", 4, 33, 1),
        I("fruit", 3, 44, 2),
        UA("fruit", 5, 22, 1),
        UA("fruit", 4, 33, 2)};
    AssertRows(CollectRows(output), expected);
}

// Flink testDisableGenerateUpdateBeforeWithoutRowNumber.
TEST(RetractableTopNFunctionTest, DisableGenerateUpdateBeforeWithoutRowNumber)
{
    const std::string desc = MakeRankConfig(false, false, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "fruit", "fruit", "fruit"},
        {1, 2, 4, 4, 3, 5},
        {12, 19, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 1, 12),
        I("book", 2, 19),
        D("book", 2, 19),
        I("book", 4, 11),
        I("fruit", 4, 33),
        I("fruit", 3, 44),
        D("fruit", 3, 44),
        I("fruit", 5, 22)};
    AssertRows(CollectRows(output), expected);
}

// Flink testConstantRankRangeWithoutRowNumber: several rows share sort keys and
// Top-3 keeps them all until the retract frees a slot.
TEST(RetractableTopNFunctionTest, ConstantRankRangeWithoutRowNumber)
{
    const std::string desc = MakeRankConfig(false, false, "rankStart=1, rankEnd=3");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"a", "a", "a", "a", "a", "a", "a"},
        {1, 2, 3, 4, 5, 6, 2},
        {1, 2, 2, 2, 3, 4, 2},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT, RowKind::UPDATE_BEFORE})));

    const std::vector<RtnRow> expected = {
        I("a", 1, 1),
        I("a", 2, 2),
        I("a", 3, 2),
        D("a", 2, 2),
        I("a", 4, 2)};
    AssertRows(CollectRows(output), expected);
}

// Flink testConstantRankRangeWithRowNumber.
TEST(RetractableTopNFunctionTest, ConstantRankRangeWithRowNumber)
{
    const std::string desc = MakeRankConfig(true, false, "rankStart=1, rankEnd=3");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"a", "a", "a", "a", "a", "a", "a"},
        {1, 2, 3, 4, 5, 6, 2},
        {1, 2, 2, 2, 3, 4, 2},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT, RowKind::UPDATE_BEFORE})));

    const std::vector<RtnRow> expected = {
        I("a", 1, 1, 1),
        I("a", 2, 2, 2),
        I("a", 3, 2, 3),
        UA("a", 3, 2, 2),
        UA("a", 4, 2, 3)};
    AssertRows(CollectRows(output), expected);
}

// Flink testRetractRecordOutOfRankRangeWithoutRowNumber: retracts of rows beyond
// the rank window are removed silently (the !stateRemoved fallback), while in-window
// retracts promote the successor.
TEST(RetractableTopNFunctionTest, RetractRecordOutOfRankRangeWithoutRowNumber)
{
    const std::string desc = MakeRankConfig(false, false, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"a", "a", "a", "a", "a", "a", "a", "a"},
        {1, 2, 3, 4, 5, 4, 1, 2},
        {1, 2, 2, 4, 4, 4, 1, 2},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::DELETE, RowKind::DELETE, RowKind::DELETE})));

    const std::vector<RtnRow> expected = {
        I("a", 1, 1),
        I("a", 2, 2),
        D("a", 1, 1),
        I("a", 3, 2),
        D("a", 2, 2),
        I("a", 5, 4)};
    AssertRows(CollectRows(output), expected);
}

// Flink testRetractRecordOutOfRankRangeWithRowNumber.
TEST(RetractableTopNFunctionTest, RetractRecordOutOfRankRangeWithRowNumber)
{
    const std::string desc = MakeRankConfig(true, false, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"a", "a", "a", "a", "a", "a", "a", "a"},
        {1, 2, 3, 4, 5, 4, 1, 2},
        {1, 2, 2, 4, 4, 4, 1, 2},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::DELETE, RowKind::DELETE, RowKind::DELETE})));

    const std::vector<RtnRow> expected = {
        I("a", 1, 1, 1),
        I("a", 2, 2, 2),
        UA("a", 2, 2, 1),
        UA("a", 3, 2, 2),
        UA("a", 3, 2, 1),
        UA("a", 5, 4, 2)};
    AssertRows(CollectRows(output), expected);
}

// Flink testRetractAndThenDeleteRecordWithoutRowNumber: a Top-1 whose winner is
// retracted then deleted must stay empty; later re-inserts start a fresh window.
TEST(RetractableTopNFunctionTest, RetractAndThenDeleteRecordWithoutRowNumber)
{
    const std::string inputTypes = R"TYPES(["VARCHAR(2147483647)","BIGINT","INT","INT"])TYPES";
    const std::string desc = MakeRankConfig(false, true, "rankStart=1, rankEnd=1", inputTypes);
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch4(
        {"a", "a", "a", "a", "a", "a"},
        {1, 1, 1, 1, 1, 1},
        {10, 9, 10, 9, 10, 9},
        {0, 0, 0, 0, 1, 1},
        {RowKind::INSERT, RowKind::INSERT, RowKind::DELETE, RowKind::DELETE,
         RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("a", 1, 10, -1),
        D("a", 1, 10, -1),
        I("a", 1, 9, -1),
        D("a", 1, 9, -1),
        I("a", 1, 10, -1),
        D("a", 1, 10, -1),
        I("a", 1, 9, -1)};
    const std::vector<int32_t> payloads = {0, 0, 0, 0, 1, 1, 1};
    ASSERT_EQ(1u, output->getAll().size());
    const auto* vb = output->getAll().front();
    ASSERT_NE(vb, nullptr);
    ASSERT_EQ(static_cast<int>(expected.size()), vb->GetRowCount());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].kind, vb->getRowKind(static_cast<int>(i))) << "rowKind mismatch at row " << i;
        EXPECT_EQ(expected[i].pk, ReadVarchar(vb, 0, static_cast<int>(i))) << "pk mismatch at row " << i;
        EXPECT_EQ(expected[i].id, ReadLong(vb, 1, static_cast<int>(i))) << "id mismatch at row " << i;
        EXPECT_EQ(expected[i].sortVal, ReadInt(vb, 2, static_cast<int>(i))) << "sortVal mismatch at row " << i;
        EXPECT_EQ(payloads[i], ReadInt(vb, 3, static_cast<int>(i))) << "payload mismatch at row " << i;
    }
}

// Flink testConstantRankRangeWithOffset: rankStart=2 forces the with-row-number
// algorithm while emitting no rank column. Rows outside [rankStart, rankEnd] are
// counted but not emitted.
TEST(RetractableTopNFunctionTest, ConstantRankRangeWithOffset)
{
    const std::string desc = MakeRankConfig(false, true, "rankStart=2, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatch(
        {"book", "book", "book", "fruit", "fruit", "fruit"},
        {1, 2, 4, 4, 3, 5},
        {12, 19, 11, 33, 44, 22},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT,
         RowKind::INSERT, RowKind::INSERT})));

    const std::vector<RtnRow> expected = {
        I("book", 2, 19),
        UB("book", 2, 19),
        UA("book", 1, 12),
        I("fruit", 3, 44),
        UB("fruit", 3, 44),
        UA("fruit", 4, 33)};
    AssertRows(CollectRows(output), expected);
}

// Cross-batch independence: the same scenario as
// ProcessRetractMessageWithGenerateUpdateBefore, fed one record per processBatch
// call. The concatenated output must be byte-identical to the single-batch run.
TEST(RetractableTopNFunctionTest, PerRecordBatchesMatchSingleBatch)
{
    const std::string desc = MakeRankConfig(true, true, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenRetractableTopN(desc, output);

    struct Record {
        std::string pk;
        int64_t id;
        int32_t sortVal;
        RowKind kind;
    };
    const std::vector<Record> records = {
        {"book", 1, 12, RowKind::INSERT},
        {"book", 2, 19, RowKind::INSERT},
        {"book", 4, 11, RowKind::INSERT},
        {"book", 1, 12, RowKind::UPDATE_BEFORE},
        {"book", 5, 11, RowKind::INSERT},
        {"fruit", 4, 33, RowKind::INSERT},
        {"fruit", 3, 44, RowKind::INSERT},
        {"fruit", 5, 22, RowKind::INSERT}};
    for (const auto& r : records) {
        op->processBatch(new StreamRecord(
            MakeBatch({r.pk}, {r.id}, {r.sortVal}, {r.kind})));
    }

    const std::vector<RtnRow> expected = {
        I("book", 1, 12, 1),
        I("book", 2, 19, 2),
        UB("book", 1, 12, 1),
        UA("book", 4, 11, 1),
        UB("book", 2, 19, 2),
        UA("book", 1, 12, 2),
        UB("book", 1, 12, 2),
        UA("book", 2, 19, 2),
        UB("book", 2, 19, 2),
        UA("book", 5, 11, 2),
        I("fruit", 4, 33, 1),
        I("fruit", 3, 44, 2),
        UB("fruit", 4, 33, 1),
        UA("fruit", 5, 22, 1),
        UB("fruit", 3, 44, 2),
        UA("fruit", 4, 33, 2)};
    AssertRows(CollectRows(output), expected);
}

// "sorted-map" ValueState<SortedMap<RowData, Long>> round-trip with a DATE sort key.
// The native serializer emits Flink's SortedMapSerializer bytes (length-prefixed
// BinaryRowData keys + int64 counts in comparator order); a DATE key must serialize,
// deserialize and re-order through SortKeyCmp exactly like an INT key, keeping the
// checkpoint bytes Flink-compatible.
TEST(RetractableTopNFunctionTest, DateSortKeySortedMapRoundTrip)
{
    using DataTypeId = omniruntime::type::DataTypeId;
    SortKeyCmp cmp(std::vector<DataTypeId>{DataTypeId::OMNI_DATE32}, std::vector<bool>{true},
                   std::vector<bool>{true});
    SortedMapValue original(cmp);
    auto* k1 = BinaryRowData::createBinaryRowDataWithMem(1);
    k1->setInt(0, 20);
    auto* k2 = BinaryRowData::createBinaryRowDataWithMem(1);
    k2->setInt(0, 10);
    original.map().emplace(k1, 2L);
    original.map().emplace(k2, 1L);

    SortedMapRowDataLongSerializer ser(
        1, std::vector<DataTypeId>{DataTypeId::OMNI_DATE32}, std::vector<bool>{true}, std::vector<bool>{true});
    DataOutputSerializer output;
    ser.serialize(&original, output);

    DataInputDeserializer input(
        reinterpret_cast<const uint8_t*>(output.getData()), static_cast<int>(output.getPosition()), 0);
    auto* restored = static_cast<SortedMapValue*>(ser.deserialize(input));

    ASSERT_EQ(2u, restored->map().size());
    auto it = restored->map().begin();
    EXPECT_EQ(10, *it->first->getInt(0)) << "first DATE sort key out of order";
    EXPECT_EQ(1L, it->second);
    ++it;
    EXPECT_EQ(20, *it->first->getInt(0)) << "second DATE sort key out of order";
    EXPECT_EQ(2L, it->second);

    delete restored;
}
} // namespace