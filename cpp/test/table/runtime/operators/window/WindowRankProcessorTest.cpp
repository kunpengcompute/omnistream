/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/WindowRankProcessor.h"
#include "streaming/api/operators/StreamOperatorFactory.h"

using json = nlohmann::json;
using namespace omnistream;
using KeyType = std::shared_ptr<RowData>;

namespace {

constexpr int64_t W1 = 1731944420000; // window end 1
constexpr int64_t W2 = 1731944425000; // window end 2

// Parameterized description JSON, mirroring the fields written by the
// OmniAdaptor StreamExecWindowRank overlay.
std::string MakeDescription(
    const std::vector<int32_t>& partitionKey,
    const std::vector<int32_t>& sortFields,
    const std::vector<bool>& ascending,
    const std::vector<bool>& nullsLast,
    int64_t rankStart,
    int64_t rankEnd,
    bool outputRankNumber,
    int32_t windowEndIndex,
    const std::string& inputTypesJson,
    const std::string& outputTypesJson)
{
    json desc;
    desc["isWindowRank"] = true;
    desc["originDescription"] = "WindowRank(rankStart=..., rankEnd=...)";
    desc["partitionKey"] = partitionKey;
    desc["sortFieldIndices"] = sortFields;
    desc["sortAscendingOrders"] = ascending;
    desc["sortNullsIsLast"] = nullsLast;
    desc["rankStart"] = rankStart;
    desc["rankEnd"] = rankEnd;
    desc["outputRankNumber"] = outputRankNumber;
    desc["windowEndIndex"] = windowEndIndex;
    desc["shiftTimeZone"] = "UTC";
    desc["inputTypes"] = json::parse(inputTypesJson);
    desc["outputTypes"] = json::parse(outputTypesJson);
    return desc.dump();
}

// 4-column batch: [id BIGINT, category BIGINT, val BIGINT, windowEnd BIGINT].
omnistream::VectorBatch* MakeBatch4Long(
    const std::vector<int64_t>& id, const std::vector<int64_t>& category, const std::vector<int64_t>& val,
    const std::vector<int64_t>& windowEnd)
{
    const int rowCnt = static_cast<int>(id.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(id.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(category.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(val.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(windowEnd.data())));
    std::vector<RowKind> kinds(rowCnt, RowKind::INSERT);
    vb->setRowKinds(0, kinds.data(), rowCnt);
    return vb;
}

// [id BIGINT, name VARCHAR, category BIGINT, val BIGINT, windowEnd BIGINT].
omnistream::VectorBatch* MakeBatchVarchar(
    const std::vector<int64_t>& id, const std::vector<std::string>& name, const std::vector<int64_t>& category,
    const std::vector<int64_t>& windowEnd)
{
    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    const int rowCnt = static_cast<int>(id.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(id.data())));
    auto* nameVec = new VarcharVector(
        rowCnt, omniruntime::vec::INITIAL_STRING_SIZE, omniruntime::type::DataTypeId::OMNI_VARCHAR);
    for (int i = 0; i < rowCnt; ++i) {
        nameVec->SetValue(i, std::string_view(name[i]));
    }
    vb->Append(nameVec);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(category.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(id.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(windowEnd.data())));
    std::vector<RowKind> kinds(rowCnt, RowKind::INSERT);
    vb->setRowKinds(0, kinds.data(), rowCnt);
    return vb;
}

// [id INTEGER, category BIGINT, val BIGINT, windowEnd BIGINT] — INT column coverage.
omnistream::VectorBatch* MakeBatch4IntId(
    const std::vector<int32_t>& id, const std::vector<int64_t>& category, const std::vector<int64_t>& val,
    const std::vector<int64_t>& windowEnd)
{
    const int rowCnt = static_cast<int>(id.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int32_t*>(id.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(category.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(val.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(windowEnd.data())));
    std::vector<RowKind> kinds(rowCnt, RowKind::INSERT);
    vb->setRowKinds(0, kinds.data(), rowCnt);
    return vb;
}

SlicingWindowOperator<KeyType, int64_t>* OpenWindowRank(const std::string& description, Output* output)
{
    json windowing = json::parse(description);
    auto processor = std::make_unique<WindowRankProcessor>(windowing, output);
    auto* op = new SlicingWindowOperator<KeyType, int64_t>(std::move(processor), windowing);
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
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    op->setup();
    op->initializeState(initializer, new LongSerializer());
    op->open();
    return op;
}

// Build a partition key row from the given category values (BIGINT columns).
KeyType MakeKey(const std::vector<int64_t>& keyValues)
{
    auto* key = BinaryRowData::createBinaryRowDataWithMem(static_cast<int>(keyValues.size()));
    for (size_t i = 0; i < keyValues.size(); ++i) {
        key->setLong(static_cast<int>(i), keyValues[i]);
    }
    return KeyType(key);
}

// Fire one (key, windowEnd) exactly like SlicingWindowOperator::onTimer does.
void FireWindow(SlicingWindowOperator<KeyType, int64_t>* op, KeyType key, int64_t windowEnd)
{
    auto* timer = new TimerHeapInternalTimer<KeyType, int64_t>(-1, key, windowEnd);
    op->onTimer(timer);
}

std::vector<std::vector<int64_t>> CollectLongRows(OutputTestVectorBatch* output)
{
    std::vector<std::vector<int64_t>> rows;
    for (omnistream::VectorBatch* batch : output->getAll()) {
        for (int i = 0; i < batch->GetRowCount(); ++i) {
            std::vector<int64_t> row;
            for (int col = 0; col < batch->GetVectorCount(); ++col) {
                const auto typeId = batch->Get(col)->GetTypeId();
                if (typeId == omniruntime::type::DataTypeId::OMNI_INT ||
                    typeId == omniruntime::type::DataTypeId::OMNI_DATE32) {
                    row.push_back(batch->GetValueAt<int32_t>(col, i));
                } else {
                    row.push_back(batch->GetValueAt<int64_t>(col, i));
                }
            }
            rows.push_back(row);
        }
    }
    return rows;
}

} // namespace

// R1: two partitions in one window, DESC top-2, rank column appended.
TEST(WindowRankProcessorTest, R1BasicDescendingTopN)
{
    const std::string desc = MakeDescription(
        {1}, {2}, {false}, {true}, 1, 2, true, 3, R"(["BIGINT","BIGINT","BIGINT","BIGINT"])",
        R"(["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    auto* batch = MakeBatch4Long(
        {1, 2, 3, 4, 5, 6, 7}, {1, 1, 1, 1, 2, 2, 2}, {50, 300, 100, 200, 7, 9, 8},
        {W1, W1, W1, W1, W1, W1, W1});
    op->processBatch(batch);
    delete batch;

    FireWindow(op, MakeKey({1}), W1);
    FireWindow(op, MakeKey({2}), W1);

    auto rows = CollectLongRows(output);
    ASSERT_EQ(rows.size(), 4u);
    // partition 1: val 300 -> rank 1, val 200 -> rank 2 (cols: id, category, val, windowEnd, rn)
    EXPECT_EQ(rows[0], std::vector<int64_t>({2, 1, 300, W1, 1}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({4, 1, 200, W1, 2}));
    // partition 2: val 9 -> rank 1, val 8 -> rank 2
    EXPECT_EQ(rows[2], std::vector<int64_t>({6, 2, 9, W1, 1}));
    EXPECT_EQ(rows[3], std::vector<int64_t>({7, 2, 8, W1, 2}));

    op->close();
    delete output;
}

// R2: offset rank range [2, 3] — the first rank is skipped.
TEST(WindowRankProcessorTest, R2OffsetRankRange)
{
    const std::string desc = MakeDescription(
        {1}, {2}, {false}, {true}, 2, 3, true, 3, R"(["BIGINT","BIGINT","BIGINT","BIGINT"])",
        R"(["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    auto* batch = MakeBatch4Long(
        {1, 2, 3, 4, 5, 6, 7}, {1, 1, 1, 1, 2, 2, 2}, {50, 300, 100, 200, 7, 9, 8},
        {W1, W1, W1, W1, W1, W1, W1});
    op->processBatch(batch);
    delete batch;

    FireWindow(op, MakeKey({1}), W1);
    FireWindow(op, MakeKey({2}), W1);

    auto rows = CollectLongRows(output);
    // partition 1: ranks 2-3 = val 200 (id4), val 100 (id3)
    // partition 2: 9,8,7 desc -> rank2 = 8 (id7), rank3 = 7 (id5)
    ASSERT_EQ(rows.size(), 4u);
    EXPECT_EQ(rows[0], std::vector<int64_t>({4, 1, 200, W1, 2}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({3, 1, 100, W1, 3}));
    EXPECT_EQ(rows[2], std::vector<int64_t>({7, 2, 8, W1, 2}));
    EXPECT_EQ(rows[3], std::vector<int64_t>({5, 2, 7, W1, 3}));

    op->close();
    delete output;
}

// R3: no rank column — output columns are exactly the input columns.
TEST(WindowRankProcessorTest, R3WithoutRankNumber)
{
    const std::string desc = MakeDescription(
        {1}, {2}, {false}, {true}, 1, 2, false, 3, R"(["BIGINT","BIGINT","BIGINT","BIGINT"])",
        R"(["BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    auto* batch = MakeBatch4Long(
        {1, 2, 3, 4, 5, 6, 7}, {1, 1, 1, 1, 2, 2, 2}, {50, 300, 100, 200, 7, 9, 8},
        {W1, W1, W1, W1, W1, W1, W1});
    op->processBatch(batch);
    delete batch;

    FireWindow(op, MakeKey({1}), W1);
    FireWindow(op, MakeKey({2}), W1);

    auto rows = CollectLongRows(output);
    ASSERT_EQ(rows.size(), 4u);
    EXPECT_EQ(rows[0], std::vector<int64_t>({2, 1, 300, W1}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({4, 1, 200, W1}));
    EXPECT_EQ(rows[2], std::vector<int64_t>({6, 2, 9, W1}));
    EXPECT_EQ(rows[3], std::vector<int64_t>({7, 2, 8, W1}));

    op->close();
    delete output;
}

// R4: two windows of the same key rank independently.
TEST(WindowRankProcessorTest, R4TwoWindowsIsolated)
{
    const std::string desc = MakeDescription(
        {1}, {2}, {false}, {true}, 1, 2, true, 3, R"(["BIGINT","BIGINT","BIGINT","BIGINT"])",
        R"(["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    auto* batch = MakeBatch4Long({1, 2, 3, 4}, {1, 1, 1, 1}, {50, 300, 400, 200}, {W1, W1, W2, W2});
    op->processBatch(batch);
    delete batch;

    FireWindow(op, MakeKey({1}), W1);
    FireWindow(op, MakeKey({1}), W2);

    auto rows = CollectLongRows(output);
    ASSERT_EQ(rows.size(), 4u);
    EXPECT_EQ(rows[0], std::vector<int64_t>({2, 1, 300, W1, 1}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({1, 1, 50, W1, 2}));
    EXPECT_EQ(rows[2], std::vector<int64_t>({3, 1, 400, W2, 1}));
    EXPECT_EQ(rows[3], std::vector<int64_t>({4, 1, 200, W2, 2}));

    op->close();
    delete output;
}

// R5: null sort keys rank last (nullsIsLast=true) and fall out of the top-2.
TEST(WindowRankProcessorTest, R5SortKeyNull)
{
    const std::string desc = MakeDescription(
        {1}, {2}, {false}, {true}, 1, 2, true, 3, R"(["BIGINT","BIGINT","BIGINT","BIGINT"])",
        R"(["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    const int rowCnt = 4;
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, std::vector<int64_t>{1, 2, 3, 4}.data()));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, std::vector<int64_t>{1, 1, 1, 1}.data()));
    auto* valVec = new omniruntime::vec::Vector<int64_t>(rowCnt);
    valVec->SetValue(0, 50);
    valVec->SetValue(1, 300);
    valVec->SetNull(2);
    valVec->SetValue(3, 200);
    vb->Append(valVec);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, std::vector<int64_t>{W1, W1, W1, W1}.data()));
    std::vector<RowKind> kinds(rowCnt, RowKind::INSERT);
    vb->setRowKinds(0, kinds.data(), rowCnt);
    op->processBatch(vb);
    delete vb;

    FireWindow(op, MakeKey({1}), W1);

    auto rows = CollectLongRows(output);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0], std::vector<int64_t>({2, 1, 300, W1, 1}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({4, 1, 200, W1, 2}));

    op->close();
    delete output;
}

// R6: tied sort values keep the arrival order (ROW_NUMBER never ties).
TEST(WindowRankProcessorTest, R6TieKeepsArrivalOrder)
{
    const std::string desc = MakeDescription(
        {1}, {2}, {false}, {true}, 1, 2, true, 3, R"(["BIGINT","BIGINT","BIGINT","BIGINT"])",
        R"(["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    auto* batch = MakeBatch4Long({1, 2, 3, 4}, {1, 1, 1, 1}, {100, 100, 100, 200}, {W1, W1, W1, W1});
    op->processBatch(batch);
    delete batch;

    FireWindow(op, MakeKey({1}), W1);

    auto rows = CollectLongRows(output);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0], std::vector<int64_t>({4, 1, 200, W1, 1}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({1, 1, 100, W1, 2}));

    op->close();
    delete output;
}

// R7: VARCHAR ascending sort keys (lexicographic).
TEST(WindowRankProcessorTest, R7VarcharAscending)
{
    const std::string desc = MakeDescription(
        {2}, {1}, {true}, {true}, 1, 2, true, 4, R"TYPES(["BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT"])TYPES",
        R"TYPES(["BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","BIGINT"])TYPES");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    auto* batch = MakeBatchVarchar(
        {1, 2, 3, 4}, {"carol", "alice", "bob", "dave"}, {1, 1, 1, 1}, {W1, W1, W1, W1});
    op->processBatch(batch);
    delete batch;

    FireWindow(op, MakeKey({1}), W1);

    // long columns (0=id, 2=category, 3=val, 4=windowEnd, 5=rn); the name column
    // is asserted separately below through its varchar vector.
    auto& outputs = output->getAll();
    ASSERT_EQ(outputs.size(), 1u);
    auto* outBatch = outputs.at(0);
    ASSERT_EQ(outBatch->GetRowCount(), 2);
    EXPECT_EQ(outBatch->GetValueAt<int64_t>(0, 0), 2); // alice: id=2
    EXPECT_EQ(outBatch->GetValueAt<int64_t>(3, 0), 2);
    EXPECT_EQ(outBatch->GetValueAt<int64_t>(4, 0), W1);
    EXPECT_EQ(outBatch->GetValueAt<int64_t>(5, 0), 1); // rank=1
    EXPECT_EQ(outBatch->GetValueAt<int64_t>(0, 1), 3); // bob: id=3
    EXPECT_EQ(outBatch->GetValueAt<int64_t>(5, 1), 2); // rank=2

    // name column assertion via string view
    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto* nameVec = static_cast<VarcharVector*>(outBatch->Get(1));
    EXPECT_EQ(nameVec->GetValue(0), std::string_view("alice"));
    EXPECT_EQ(nameVec->GetValue(1), std::string_view("bob"));

    op->close();
    delete output;
}

// R8: rows whose window already fired (watermark past the window end) are dropped.
TEST(WindowRankProcessorTest, R8LateDataDropped)
{
    const std::string desc = MakeDescription(
        {1}, {2}, {false}, {true}, 1, 2, true, 3, R"(["BIGINT","BIGINT","BIGINT","BIGINT"])",
        R"(["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    // first batch: window W1 rows
    auto* batch1 = MakeBatch4Long({1, 3}, {1, 1}, {999, 200}, {W1, W1});
    op->processBatch(batch1);
    delete batch1;
    // push the watermark past W1 (drop the id=2 row of an already fired window)
    Watermark wm(W1 + 1);
    op->ProcessWatermark(&wm);
    auto* batch2 = MakeBatch4Long({2}, {1}, {100}, {W1});
    op->processBatch(batch2);
    delete batch2;

    FireWindow(op, MakeKey({1}), W1);

    auto rows = CollectLongRows(output);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0], std::vector<int64_t>({1, 1, 999, W1, 1}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({3, 1, 200, W1, 2}));

    op->close();
    delete output;
}

// R9: empty partition key (SINGLETON) — all rows share the arity-0 key.
TEST(WindowRankProcessorTest, R9EmptyPartitionKey)
{
    const std::string desc = MakeDescription(
        {}, {2}, {false}, {true}, 1, 2, true, 3, R"(["BIGINT","BIGINT","BIGINT","BIGINT"])",
        R"(["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    auto* batch = MakeBatch4Long({1, 2, 3, 4}, {1, 1, 2, 2}, {50, 300, 400, 200}, {W1, W1, W1, W1});
    op->processBatch(batch);
    delete batch;

    FireWindow(op, MakeKey({}), W1);

    auto rows = CollectLongRows(output);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0], std::vector<int64_t>({3, 2, 400, W1, 1}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({2, 1, 300, W1, 2}));

    op->close();
    delete output;
}

// R10: multi-column sort — val DESC with INTEGER id ASC as the tie-breaker.
TEST(WindowRankProcessorTest, R10MultiSortColumns)
{
    const std::string desc = MakeDescription(
        {1}, {2, 0}, {false, true}, {true, true}, 1, 3, true, 3,
        R"(["INTEGER","BIGINT","BIGINT","BIGINT"])",
        R"(["INTEGER","BIGINT","BIGINT","BIGINT","BIGINT"])");
    auto* output = new OutputTestVectorBatch();
    auto* op = OpenWindowRank(desc, output);
    auto* batch = MakeBatch4IntId({1, 2, 3, 4}, {1, 1, 1, 1}, {100, 100, 100, 200}, {W1, W1, W1, W1});
    op->processBatch(batch);
    delete batch;

    FireWindow(op, MakeKey({1}), W1);

    auto rows = CollectLongRows(output);
    ASSERT_EQ(rows.size(), 3u);
    // val=200 first; the three val=100 rows tie and are ordered by id ASC
    EXPECT_EQ(rows[0], std::vector<int64_t>({4, 1, 200, W1, 1}));
    EXPECT_EQ(rows[1], std::vector<int64_t>({1, 1, 100, W1, 2}));
    EXPECT_EQ(rows[2], std::vector<int64_t>({2, 1, 100, W1, 3}));

    op->close();
    delete output;
}
