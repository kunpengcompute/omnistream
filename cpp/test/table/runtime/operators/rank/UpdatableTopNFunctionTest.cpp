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
 * Unit tests for the native UpdatableTopNFunction operator
 * (cpp/table/runtime/operators/rank/UpdatableTopNFunction.h). The scenarios are
 * ported 1:1 from Flink's UpdatableTopNFunctionTest.java and the still-applicable
 * cases inherited from TopNFunctionTestBase (input row type VARCHAR(partition) /
 * BIGINT(rowKey/primaryKey) / INT(sort), partitionKey=[0], primaryKeys=[1],
 * sortFieldIndices=[2] ascending, Constant/Variable Rank Range, with/without row
 * number and generateUpdateBefore on/off).
 *
 * UpdatableTopNFunction is the UpdateFastStrategy variant, so the input is an
 * append-only changelog (INSERT / UPDATE_AFTER). Rows whose primary key already
 * exists in the Top-N are handled as updates (the sort key must move towards a
 * better rank); the emitted changelog (INSERT / UPDATE_BEFORE / UPDATE_AFTER /
 * DELETE) is asserted row-by-row in emission order, which must match the original
 * Flink expected changelog exactly.
 *
 * Native coverage (appended below the Flink ports):
 *  - HEAP "data-state-with-update" MapState content (row + innerRank);
 *  - lenient monotonicity-violation path;
 *  - per-record batches (cross-batch flush / stateKey transfer);
 *  - LRU eviction flush + restore from state;
 *  - composite primary key and mixed DESC/ASC multi-column sort;
 *  - DATE (OMNI_DATE32) sort/output column;
 *  - invalid variable rank-end column type and missing primaryKeys errors.
 *
 * Known gaps (not covered by this UT; tracked separately):
 *  - HEAP native checkpoint save/restore has no ROW_INT_PAIR_BK dispatch, so the
 *    operator state is skipped on snapshot and cannot be restored on HEAP;
 *  - Flink savepoint interop has no UpdatableTopNSavepointAdaptor (the factory
 *    marks it unsupported); BSS GetMapState rejects ROW_INT_PAIR_BK;
 *  - state TTL / LRU expiry and DECIMAL output are not supported natively;
 *  - testConstantRankRangeWithoutOffset restore half stays out of scope.
 *  - RANK / DENSE_RANK and ConstantRankRangeWithoutEnd do not apply (native config
 *    has no rankType / open-ended range fields).
 *
 * Remote run: tneltest --gtest_filter='UpdatableTopNFunctionTest.*'
 */

#include <gtest/gtest.h>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "table/runtime/operators/rank/UpdatableTopNFunction.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/RowKind.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/RowDataIntPairSerializer.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>

namespace {

using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;

// One expected output row. rank == -1 means the operator did not output a rank column.
struct UtnRow {
    RowKind kind;
    std::string pk;
    int64_t id;
    int32_t sortVal;
    int64_t rank;
};

UtnRow I(const std::string& pk, int64_t id, int32_t sortVal, int64_t rank = -1)
{
    return {RowKind::INSERT, pk, id, sortVal, rank};
}
UtnRow UB(const std::string& pk, int64_t id, int32_t sortVal, int64_t rank = -1)
{
    return {RowKind::UPDATE_BEFORE, pk, id, sortVal, rank};
}
UtnRow UA(const std::string& pk, int64_t id, int32_t sortVal, int64_t rank = -1)
{
    return {RowKind::UPDATE_AFTER, pk, id, sortVal, rank};
}
UtnRow D(const std::string& pk, int64_t id, int32_t sortVal, int64_t rank = -1)
{
    return {RowKind::DELETE, pk, id, sortVal, rank};
}

// Mirrors the JSON written by the shadowed StreamExecRank overlay for an
// UpdateFastStrategy Rank. The stream is keyed on column 0, the unique key is the
// primaryKeys projection (column 1 by default) and the stream is sorted on column 2
// ascending. `cacheSize` only matters for the flush/eviction paths and defaults to
// Flink's 10000. The extra parameters let the native cases override the unique key,
// the sort spec (multi-column / DESC) and the LRU capacity.
std::string MakeRankConfig(
    bool outputRankNumber,
    bool generateUpdateBefore,
    const std::string& rankRange,
    const std::string& inputTypes = R"TYPES(["VARCHAR(2147483647)","BIGINT","INT"])TYPES",
    const std::string& primaryKeys = "[1]",
    const std::string& sortFieldIndices = "[2]",
    const std::string& sortAscendingOrders = "[true]",
    const std::string& sortNullsIsLast = "[true]",
    const std::string& cacheSize = "10000")
{
    std::string json = std::string(R"({"originDescription":null,
"inputTypes":)") + inputTypes + R"(,
"outputTypes":)" + inputTypes + R"(,
"partitionKey":[0],
"primaryKeys":)" + primaryKeys + R"(,
"outputRankNumber":)" + (outputRankNumber ? "true" : "false") + R"(,
"rankRange":")" + rankRange + R"(",
"generateUpdateBefore":)" + (generateUpdateBefore ? "true" : "false") +
        R"(,
"processFunction":"UpdatableTopNFunction",
"sortFieldIndices":)" + sortFieldIndices + R"(,
"sortAscendingOrders":)" + sortAscendingOrders + R"(,
"sortNullsIsLast":)" + sortNullsIsLast + R"(,
"cacheSize":)" + cacheSize;
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

// 3-column input batch built from a row list: VARCHAR partition key, BIGINT id
// (primary key), INT sort value. The rank field is ignored on input.
omnistream::VectorBatch* MakeBatchFromRows(const std::vector<UtnRow>& rows)
{
    const int rowCnt = static_cast<int>(rows.size());
    std::vector<std::string> pk;
    std::vector<int64_t> ids;
    std::vector<int32_t> sortVals;
    std::vector<RowKind> kinds;
    pk.reserve(rows.size());
    ids.reserve(rows.size());
    sortVals.reserve(rows.size());
    kinds.reserve(rows.size());
    for (const auto& row : rows) {
        pk.push_back(row.pk);
        ids.push_back(row.id);
        sortVals.push_back(row.sortVal);
        kinds.push_back(row.kind);
    }
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(MakeVarcharVector(pk));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, ids.data()));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, sortVals.data()));
    vb->setRowKinds(0, kinds.data(), rowCnt);
    return vb;
}

KeyedProcessOperator<RowData*, RowData*, RowData*>* OpenUpdatableTopN(
    const std::string& description, OutputTestVectorBatch* output)
{
    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    auto* topN = new UpdatableTopNFunction<RowData*>(rankConfig);
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
std::vector<UtnRow> CollectRows(OutputTestVectorBatch* output)
{
    std::vector<UtnRow> rows;
    for (auto* vb : output->getAll()) {
        if (vb == nullptr) {
            ADD_FAILURE() << "null output batch in CollectRows";
            continue;
        }
        const bool hasRank = vb->GetVectorCount() > 3;
        for (int i = 0; i < vb->GetRowCount(); ++i) {
            UtnRow r;
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

void AssertRows(const std::vector<UtnRow>& actual, const std::vector<UtnRow>& expected)
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

// Output row for the composite-key case: both the partition key (name) and the
// second primary-key column (id) are VARCHAR.
struct UtnRow2 {
    RowKind kind;
    std::string name;
    std::string id;
    int32_t num;
    int64_t rank;
};

UtnRow2 I2(const std::string& name, const std::string& id, int32_t num, int64_t rank = -1)
{
    return {RowKind::INSERT, name, id, num, rank};
}
UtnRow2 UB2(const std::string& name, const std::string& id, int32_t num, int64_t rank = -1)
{
    return {RowKind::UPDATE_BEFORE, name, id, num, rank};
}
UtnRow2 UA2(const std::string& name, const std::string& id, int32_t num, int64_t rank = -1)
{
    return {RowKind::UPDATE_AFTER, name, id, num, rank};
}

// 3-column composite-key input batch: VARCHAR partition key (name), VARCHAR
// primary-key column (id, together with name it forms the unique key) and INT sort
// value. Mirrors the production SQL shape used by docs/sql/updatabletopn.
omnistream::VectorBatch* MakeBatchComposite(
    const std::vector<std::string>& name,
    const std::vector<std::string>& id,
    const std::vector<int32_t>& num,
    const std::vector<RowKind>& kinds)
{
    const int rowCnt = static_cast<int>(name.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(MakeVarcharVector(name));
    vb->Append(MakeVarcharVector(id));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int32_t*>(num.data())));
    std::vector<RowKind> kindBuf = kinds;
    vb->setRowKinds(0, kindBuf.data(), static_cast<int>(kindBuf.size()));
    return vb;
}

// 3-column input batch with a DATE (OMNI_DATE32) sort column built from a row list:
// VARCHAR partition key, BIGINT id (primary key) and a DATE sort value carried by
// an int32 vector.
omnistream::VectorBatch* MakeBatchDateFromRows(const std::vector<UtnRow>& rows)
{
    const int rowCnt = static_cast<int>(rows.size());
    std::vector<std::string> pk;
    std::vector<int64_t> ids;
    std::vector<RowKind> kinds;
    pk.reserve(rows.size());
    ids.reserve(rows.size());
    kinds.reserve(rows.size());
    for (const auto& row : rows) {
        pk.push_back(row.pk);
        ids.push_back(row.id);
        kinds.push_back(row.kind);
    }
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(MakeVarcharVector(pk));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, ids.data()));
    auto* dateVec = new omniruntime::vec::Vector<int32_t>(rowCnt, omniruntime::type::DataTypeId::OMNI_DATE32);
    for (int i = 0; i < rowCnt; ++i) {
        dateVec->SetValue(i, rows[static_cast<size_t>(i)].sortVal);
    }
    vb->Append(dateVec);
    vb->setRowKinds(0, kinds.data(), rowCnt);
    return vb;
}

std::vector<UtnRow2> CollectRowsComposite(OutputTestVectorBatch* output)
{
    std::vector<UtnRow2> rows;
    for (auto* vb : output->getAll()) {
        if (vb == nullptr) {
            ADD_FAILURE() << "null output batch in CollectRowsComposite";
            continue;
        }
        const bool hasRank = vb->GetVectorCount() > 3;
        for (int i = 0; i < vb->GetRowCount(); ++i) {
            UtnRow2 r;
            r.kind = vb->getRowKind(i);
            r.name = ReadVarchar(vb, 0, i);
            r.id = ReadVarchar(vb, 1, i);
            r.num = ReadInt(vb, 2, i);
            r.rank = hasRank ? ReadLong(vb, 3, i) : -1;
            rows.push_back(r);
        }
    }
    return rows;
}

void AssertRows2(const std::vector<UtnRow2>& actual, const std::vector<UtnRow2>& expected)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].kind, actual[i].kind) << "rowKind mismatch at row " << i;
        EXPECT_EQ(expected[i].name, actual[i].name) << "name mismatch at row " << i;
        EXPECT_EQ(expected[i].id, actual[i].id) << "id mismatch at row " << i;
        EXPECT_EQ(expected[i].num, actual[i].num) << "num mismatch at row " << i;
        if (expected[i].rank >= 0) {
            EXPECT_EQ(expected[i].rank, actual[i].rank) << "rank mismatch at row " << i;
        }
    }
}

// Executes one input batch and asserts the flattened changelog.
void RunSingleBatch(
    const std::string& desc,
    const std::vector<UtnRow>& input,
    const std::vector<UtnRow>& expected)
{
    SCOPED_TRACE(desc);
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenUpdatableTopN(desc, output);
    op->processBatch(new StreamRecord(MakeBatchFromRows(input)));
    AssertRows(CollectRows(output), expected);
}

// Executes several input batches in order and asserts the flattened changelog.
void RunBatches(
    const std::string& desc,
    const std::vector<std::vector<UtnRow>>& batches,
    const std::vector<UtnRow>& expected)
{
    SCOPED_TRACE(desc);
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenUpdatableTopN(desc, output);
    for (const auto& batch : batches) {
        op->processBatch(new StreamRecord(MakeBatchFromRows(batch)));
    }
    AssertRows(CollectRows(output), expected);
}

// Same as RunSingleBatch but the sort column is DATE (OMNI_DATE32).
void RunSingleBatchDate(
    const std::string& desc,
    const std::vector<UtnRow>& input,
    const std::vector<UtnRow>& expected)
{
    SCOPED_TRACE(desc);
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenUpdatableTopN(desc, output);
    op->processBatch(new StreamRecord(MakeBatchDateFromRows(input)));
    AssertRows(CollectRows(output), expected);
}

// Input shared by the four Flink testSortKeyChanges* cases and the DATE variant.
const std::vector<UtnRow> kSortKeyChangesInput = {
    I("book", 2, 19),
    I("book", 3, 16),
    UA("book", 2, 11),
    UA("book", 3, 15),
    I("book", 4, 2),
    UA("book", 2, 1)};

// Shared by testSortKeyChangesWhenOutputRankNumber and the per-record-batch case.
const std::vector<UtnRow> kSortKeyChangesRowNumberUbExpected = {
    I("book", 2, 19, 1),
    UB("book", 2, 19, 1),
    UA("book", 3, 16, 1),
    I("book", 2, 19, 2),
    UB("book", 3, 16, 1),
    UB("book", 2, 19, 2),
    UA("book", 2, 11, 1),
    UA("book", 3, 16, 2),
    UB("book", 3, 16, 2),
    UA("book", 3, 15, 2),
    UB("book", 2, 11, 1),
    UA("book", 4, 2, 1),
    UB("book", 3, 15, 2),
    UA("book", 2, 11, 2),
    UB("book", 4, 2, 1),
    UB("book", 2, 11, 2),
    UA("book", 2, 1, 1),
    UA("book", 4, 2, 2)};

// Shared by testSortKeyChangesWhenNotOutputRankNumber and the DATE variant.
const std::vector<UtnRow> kSortKeyChangesNoRowNumberUbExpected = {
    I("book", 2, 19),
    I("book", 3, 16),
    UB("book", 2, 19),
    UA("book", 2, 11),
    UB("book", 3, 16),
    UA("book", 3, 15),
    D("book", 3, 15),
    I("book", 4, 2),
    UB("book", 2, 11),
    UA("book", 2, 1)};

// Input shared by the two base constant-range cases with rank output toggles.
const std::vector<UtnRow> kBaseConstantInput = {
    I("book", 1, 12),
    I("book", 2, 19),
    I("book", 4, 11),
    I("book", 5, 11),
    I("fruit", 4, 33),
    I("fruit", 3, 44),
    I("fruit", 5, 22)};

// Input shared by the two constant-range-with-offset cases.
const std::vector<UtnRow> kOffsetInput = {
    I("book", 1, 12),
    I("book", 2, 19),
    I("book", 4, 11),
    I("fruit", 4, 33),
    I("fruit", 3, 44),
    I("fruit", 5, 22)};

// Flink UpdatableTopNFunctionTest.testVariableRankRange: variable rank end read
// from column 1 (per-partition frozen). No rank column, generateUpdateBefore on.
TEST(UpdatableTopNFunctionTest, VariableRankRange)
{
    RunSingleBatch(
        MakeRankConfig(false, true, "rankEnd=$1"),
        {I("book", 2, 19),
         UA("book", 2, 18),
         I("fruit", 1, 44),
         UA("fruit", 1, 33),
         UA("fruit", 1, 22)},
        {I("book", 2, 19),
         UB("book", 2, 19),
         UA("book", 2, 18),
         I("fruit", 1, 44),
         UB("fruit", 1, 44),
         UA("fruit", 1, 33),
         UB("fruit", 1, 33),
         UA("fruit", 1, 22)});
}

// Flink UpdatableTopNFunctionTest.testOutputRankNumberWithVariableRankRange:
// variable rank end from column 1, with rank column, generateUpdateBefore on.
TEST(UpdatableTopNFunctionTest, OutputRankNumberWithVariableRankRange)
{
    RunSingleBatch(
        MakeRankConfig(true, true, "rankEnd=$1"),
        {I("book", 2, 19),
         UA("book", 2, 12),
         UA("book", 2, 11),
         I("fruit", 1, 44),
         UA("fruit", 1, 33),
         UA("fruit", 1, 22)},
        {I("book", 2, 19, 1),
         UB("book", 2, 19, 1),
         UA("book", 2, 12, 1),
         UB("book", 2, 12, 1),
         UA("book", 2, 11, 1),
         I("fruit", 1, 44, 1),
         UB("fruit", 1, 44, 1),
         UA("fruit", 1, 33, 1),
         UB("fruit", 1, 33, 1),
         UA("fruit", 1, 22, 1)});
}

// Flink UpdatableTopNFunctionTest.testSortKeyChangesWhenOutputRankNumber: rank
// column emitted, generateUpdateBefore on; sort key moves up and cascades ranks.
TEST(UpdatableTopNFunctionTest, SortKeyChangesWhenOutputRankNumber)
{
    RunSingleBatch(
        MakeRankConfig(true, true, "rankStart=1, rankEnd=2"),
        kSortKeyChangesInput,
        kSortKeyChangesRowNumberUbExpected);
}

// Flink
// UpdatableTopNFunctionTest.testSortKeyChangesWhenOutputRankNumberAndNotGenerateUpdateBefore:
// same scenario as above with generateUpdateBefore off, so only UPDATE_AFTER shifts.
TEST(UpdatableTopNFunctionTest, SortKeyChangesWhenOutputRankNumberAndNotGenerateUpdateBefore)
{
    RunSingleBatch(
        MakeRankConfig(true, false, "rankStart=1, rankEnd=2"),
        kSortKeyChangesInput,
        {I("book", 2, 19, 1),
         UA("book", 3, 16, 1),
         I("book", 2, 19, 2),
         UA("book", 2, 11, 1),
         UA("book", 3, 16, 2),
         UA("book", 3, 15, 2),
         UA("book", 4, 2, 1),
         UA("book", 2, 11, 2),
         UA("book", 2, 1, 1),
         UA("book", 4, 2, 2)});
}

// Flink UpdatableTopNFunctionTest.testSortKeyChangesWhenNotOutputRankNumber: no
// rank column, generateUpdateBefore on; the retired row is emitted as DELETE.
TEST(UpdatableTopNFunctionTest, SortKeyChangesWhenNotOutputRankNumber)
{
    RunSingleBatch(
        MakeRankConfig(false, true, "rankStart=1, rankEnd=2"),
        kSortKeyChangesInput,
        kSortKeyChangesNoRowNumberUbExpected);
}

// Flink
// UpdatableTopNFunctionTest.testSortKeyChangesWhenNotOutputRankNumberAndNotGenerateUpdateBefore:
// no rank column and generateUpdateBefore off.
TEST(UpdatableTopNFunctionTest, SortKeyChangesWhenNotOutputRankNumberAndNotGenerateUpdateBefore)
{
    RunSingleBatch(
        MakeRankConfig(false, false, "rankStart=1, rankEnd=2"),
        kSortKeyChangesInput,
        {I("book", 2, 19),
         I("book", 3, 16),
         UA("book", 2, 11),
         UA("book", 3, 15),
         D("book", 3, 15),
         I("book", 4, 2),
         UA("book", 2, 1)});
}

// Flink TopNFunctionTestBase.testDisableGenerateUpdateBefore (inherited by
// UpdatableTopNFunctionTest): no rank column, generateUpdateBefore off. Duplicate
// sort keys are kept until the Top-N overflows; the retired row is deleted.
TEST(UpdatableTopNFunctionTest, DisableGenerateUpdateBefore)
{
    RunSingleBatch(
        MakeRankConfig(false, false, "rankStart=1, rankEnd=2"),
        {I("book", 1, 12),
         I("book", 2, 19),
         I("book", 3, 19),
         I("book", 4, 11),
         I("book", 5, 11),
         I("fruit", 4, 33),
         I("fruit", 3, 44),
         I("fruit", 5, 22)},
        {I("book", 1, 12),
         I("book", 2, 19),
         D("book", 2, 19),
         I("book", 4, 11),
         D("book", 1, 12),
         I("book", 5, 11),
         I("fruit", 4, 33),
         I("fruit", 3, 44),
         D("fruit", 3, 44),
         I("fruit", 5, 22)});
}

// Flink TopNFunctionTestBase.testDisableGenerateUpdateBeforeAndOutputRankNumber:
// rank column emitted, generateUpdateBefore off, so shifts are UPDATE_AFTER only.
TEST(UpdatableTopNFunctionTest, DisableGenerateUpdateBeforeAndOutputRankNumber)
{
    RunSingleBatch(
        MakeRankConfig(true, false, "rankStart=1, rankEnd=2"),
        kBaseConstantInput,
        {I("book", 1, 12, 1),
         I("book", 2, 19, 2),
         UA("book", 4, 11, 1),
         UA("book", 1, 12, 2),
         UA("book", 5, 11, 2),
         I("fruit", 4, 33, 1),
         I("fruit", 3, 44, 2),
         UA("fruit", 5, 22, 1),
         UA("fruit", 4, 33, 2)});
}

// Flink TopNFunctionTestBase.testOutputRankNumberWithConstantRankRange: rank column
// emitted, generateUpdateBefore on.
TEST(UpdatableTopNFunctionTest, OutputRankNumberWithConstantRankRange)
{
    RunSingleBatch(
        MakeRankConfig(true, true, "rankStart=1, rankEnd=2"),
        kBaseConstantInput,
        {I("book", 1, 12, 1),
         I("book", 2, 19, 2),
         UB("book", 1, 12, 1),
         UA("book", 4, 11, 1),
         UB("book", 2, 19, 2),
         UA("book", 1, 12, 2),
         UB("book", 1, 12, 2),
         UA("book", 5, 11, 2),
         I("fruit", 4, 33, 1),
         I("fruit", 3, 44, 2),
         UB("fruit", 4, 33, 1),
         UA("fruit", 5, 22, 1),
         UB("fruit", 3, 44, 2),
         UA("fruit", 4, 33, 2)});
}

// Flink TopNFunctionTestBase.testConstantRankRangeWithOffset: rankStart=2 forces the
// with-row-number algorithm while emitting no rank column; rows outside
// [rankStart, rankEnd] are counted but not emitted.
TEST(UpdatableTopNFunctionTest, ConstantRankRangeWithOffset)
{
    RunSingleBatch(
        MakeRankConfig(false, true, "rankStart=2, rankEnd=2"),
        kOffsetInput,
        {I("book", 2, 19),
         UB("book", 2, 19),
         UA("book", 1, 12),
         I("fruit", 3, 44),
         UB("fruit", 3, 44),
         UA("fruit", 4, 33)});
}

// Flink TopNFunctionTestBase.testConstantRankRangeWithoutOffset (streaming half only;
// the snapshot/restore half is checkpoint-related and deferred): no rank column,
// generateUpdateBefore on, the overflow row is deleted.
TEST(UpdatableTopNFunctionTest, ConstantRankRangeWithoutOffset)
{
    RunSingleBatch(
        MakeRankConfig(false, true, "rankStart=1, rankEnd=2"),
        kOffsetInput,
        {I("book", 1, 12),
         I("book", 2, 19),
         D("book", 2, 19),
         I("book", 4, 11),
         I("fruit", 4, 33),
         I("fruit", 3, 44),
         D("fruit", 3, 44),
         I("fruit", 5, 22)});
}

// Native: flushes the HEAP "data-state-with-update" MapState and checks its content.
// Flink stores MapState<rowKey, Tuple2<row, innerRank>>; the native port writes the
// same projection (rowKey = primaryKeys=[1], value = full input row + innerRank).
// The update 20 -> 15 happens before the first flush, so exactly two state entries
// must remain (id 1 and id 2), each with innerRank 1.
TEST(UpdatableTopNFunctionTest, DataStateWithUpdateMatchesFlinkMapState)
{
    const std::string desc = MakeRankConfig(false, true, "rankStart=1, rankEnd=2");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenUpdatableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatchFromRows(
        {I("book", 1, 10),
         I("book", 2, 20),
         UA("book", 2, 15)})));

    const std::vector<UtnRow> expected = {
        I("book", 1, 10),
        I("book", 2, 20),
        UB("book", 2, 20),
        UA("book", 2, 15)};
    AssertRows(CollectRows(output), expected);

    auto* partitionKey = BinaryRowData::createBinaryRowDataWithMem(1);
    partitionKey->setStringView(0, std::string_view("book"));
    op->setCurrentKey(partitionKey);

    auto* heap = dynamic_cast<HeapKeyedStateBackend<RowData*>*>(op->getKeyedStateBackend());
    ASSERT_NE(heap, nullptr);
    using StateMap = emhash7::HashMap<RowData*, RowDataIntPair*>*;
    auto* table = reinterpret_cast<CopyOnWriteStateTable<RowData*, VoidNamespace, StateMap>*>(
        heap->getStateTablePtr("data-state-with-update"));
    ASSERT_NE(table, nullptr);
    auto* entries = table->get(VoidNamespace());
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 2u);

    std::map<int64_t, std::pair<int32_t, int>> byId;
    for (const auto& kv : *entries) {
        ASSERT_NE(kv.first, nullptr);
        ASSERT_NE(kv.second, nullptr);
        ASSERT_NE(kv.second->row, nullptr);
        const int64_t id = *kv.first->getLong(0);
        byId.emplace(id, std::make_pair(*kv.second->row->getInt(2), kv.second->innerRank));
    }
    ASSERT_EQ(byId.count(1), 1u);
    EXPECT_EQ(byId[1].first, 10);
    EXPECT_EQ(byId[1].second, 1);
    ASSERT_EQ(byId.count(2), 1u);
    EXPECT_EQ(byId[2].first, 15);
    EXPECT_EQ(byId[2].second, 1);

    delete partitionKey;
}

// Native: lenient monotonicity-violation path (UpdateFastStrategy guarantees the
// sort key moves towards a better rank; a regression is tolerated and logged).
// Only the with-row-number path has this branch, so outputRankNumber must be on.
TEST(UpdatableTopNFunctionTest, LenientMonotonicityViolation)
{
    RunSingleBatch(
        MakeRankConfig(true, true, "rankStart=1, rankEnd=2"),
        {I("book", 2, 11),
         I("book", 3, 12),
         UA("book", 2, 20)},
        {I("book", 2, 11, 1),
         I("book", 3, 12, 2),
         UB("book", 2, 11, 1),
         UB("book", 3, 12, 2),
         UA("book", 3, 12, 1),
         UA("book", 2, 20, 2)});
}

// Native: one record per processBatch. The concatenated output must be identical to
// the single-batch SortKeyChangesWhenOutputRankNumber run, which exercises the
// per-batch flush and the HEAP stateKey transfer across batches.
TEST(UpdatableTopNFunctionTest, PerRecordBatchesMatchSingleBatch)
{
    std::vector<std::vector<UtnRow>> batches;
    batches.reserve(kSortKeyChangesInput.size());
    for (const auto& row : kSortKeyChangesInput) {
        batches.push_back({row});
    }
    RunBatches(
        MakeRankConfig(true, true, "rankStart=1, rankEnd=2"),
        batches,
        kSortKeyChangesRowNumberUbExpected);
}

// Native: cacheSize=2 with rankEnd=2 yields an LRU capacity of 1. The second
// partition in batch 1 evicts the first, whose dirty rows are flushed by the cache
// removal listener; batch 2 re-touches it and must restore the Top-N from state.
TEST(UpdatableTopNFunctionTest, CacheEvictionFlushAndRestore)
{
    const std::string desc = MakeRankConfig(
        false, true, "rankStart=1, rankEnd=2",
        R"TYPES(["VARCHAR(2147483647)","BIGINT","INT"])TYPES",
        "[1]", "[2]", "[true]", "[true]", "2");
    RunBatches(
        desc,
        {{I("book", 1, 10), I("fruit", 4, 20)},
         {I("book", 2, 5)}},
        {I("book", 1, 10),
         I("fruit", 4, 20),
         I("book", 2, 5)});
}

// Native: production SQL shape (docs/sql/updatabletopn) with a composite unique key
// (name, id) = primaryKeys [0,1] and a two-column sort spec (num DESC, id ASC).
// rankEnd=3, rank column emitted. No updates: this pins the multi-column sort-key
// comparison, the composite row-key projection and the overflow emission order.
TEST(UpdatableTopNFunctionTest, CompositeKeyMixedSortOrder)
{
    const std::string desc = MakeRankConfig(
        true, true, "rankStart=1, rankEnd=3",
        R"TYPES(["VARCHAR(2147483647)","VARCHAR(2147483647)","INT"])TYPES",
        "[0,1]", "[2,1]", "[false,true]", "[true,true]");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenUpdatableTopN(desc, output);

    op->processBatch(new StreamRecord(MakeBatchComposite(
        {"book", "book", "book", "book"},
        {"A", "B", "C", "D"},
        {10, 20, 15, 12},
        {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT, RowKind::INSERT})));

    const std::vector<UtnRow2> expected = {
        I2("book", "A", 10, 1),
        UB2("book", "A", 10, 1),
        UA2("book", "B", 20, 1),
        I2("book", "A", 10, 2),
        UB2("book", "A", 10, 2),
        UA2("book", "C", 15, 2),
        I2("book", "A", 10, 3),
        UB2("book", "A", 10, 3),
        UA2("book", "D", 12, 3)};
    AssertRows2(CollectRowsComposite(output), expected);
}

// Native: DATE (OMNI_DATE32) sort column. DATE is stored/compared as int32 exactly
// like INT, and createOutputBatch emits it as an int32 vector, so the changelog must
// be identical to the INT SortKeyChangesWhenNotOutputRankNumber baseline.
TEST(UpdatableTopNFunctionTest, DateSortKeySortKeyChangesWhenNotOutputRankNumber)
{
    RunSingleBatchDate(
        MakeRankConfig(
            false, true, "rankStart=1, rankEnd=2",
            R"TYPES(["VARCHAR(2147483647)","BIGINT","DATE"])TYPES"),
        kSortKeyChangesInput,
        kSortKeyChangesNoRowNumberUbExpected);
}

// Native: a variable rank end read from a VARCHAR column is rejected lazily, when
// the first row is processed (fetchRankEnd), not in open().
TEST(UpdatableTopNFunctionTest, InvalidVariableRankRangeColumnTypeThrows)
{
    const std::string desc = MakeRankConfig(false, true, "rankEnd=$0");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenUpdatableTopN(desc, output);

    EXPECT_THROW(
        op->processBatch(new StreamRecord(MakeBatchFromRows(
            {I("book", 2, 19)}))),
        std::runtime_error);
}

// Native: the operator requires primaryKeys; the constructor rejects a config
// without the field instead of silently falling back to a full-row unique key.
TEST(UpdatableTopNFunctionTest, MissingPrimaryKeysThrows)
{
    nlohmann::json rankConfig =
        nlohmann::json::parse(MakeRankConfig(false, true, "rankStart=1, rankEnd=2"));
    rankConfig.erase("primaryKeys");

    EXPECT_THROW(
        {
            UpdatableTopNFunction<RowData*> func(rankConfig);
            (void)func;
        },
        std::runtime_error);
}

} // namespace
