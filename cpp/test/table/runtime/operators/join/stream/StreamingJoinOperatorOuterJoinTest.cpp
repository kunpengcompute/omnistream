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

// Description: unit tests for StreamingJoinOperator RIGHT/FULL outer join quadrants.
// The expected behaviors mirror Flink StreamingJoinOperator with leftIsOuter/rightIsOuter:
// RIGHT  -> leftIsOuter=false, rightIsOuter=true  (right rows eagerly null-padded)
// FULL   -> leftIsOuter=true,  rightIsOuter=true  (both sides eagerly null-padded)

#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <tuple>
#include "table/runtime/operators/join/stream/StreamingJoinOperator.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "table/types/logical/RowType.h"
#include "test/core/operators/OutputTest.h"
#include "core/typeutils/LongSerializer.h"

// Left: (BIGINT key, BIGINT val); Right: (BIGINT key, BIGINT rval); output: 4 BIGINT columns.
static std::string baseDesc()
{
    return R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["BIGINT", "BIGINT"],
  "rightInputTypes": ["BIGINT", "BIGINT"],
  "outputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "nonEquiCondition": null,
  "joinType": "<JOIN_TYPE>",
  "filterNulls": [true],
  "leftInputSpec": "NoUniqueKey",
  "rightInputSpec": "NoUniqueKey",
  "leftUniqueKeys": [],
  "rightUniqueKeys": []
})delimiter";
}

static std::string withJoinType(const std::string& joinType)
{
    std::string desc = baseDesc();
    return desc.replace(desc.find("<JOIN_TYPE>"), std::string("<JOIN_TYPE>").size(), joinType);
}

using namespace omnistream;

// Left rows: two rows (key1, val1), (key2, val2) with the given row kinds.
static omnistream::VectorBatch* getLeftBatch(int64_t key1, int64_t val1, int64_t key2, int64_t val2,
                                             RowKind kind1 = RowKind::INSERT, RowKind kind2 = RowKind::INSERT)
{
    auto vb = new omnistream::VectorBatch(2);
    auto key = new omniruntime::vec::Vector<int64_t>(2);
    key->SetValue(0, key1);
    key->SetValue(1, key2);
    vb->Append(key);
    auto val = new omniruntime::vec::Vector<int64_t>(2);
    val->SetValue(0, val1);
    val->SetValue(1, val2);
    vb->Append(val);
    vb->setRowKind(0, kind1);
    vb->setRowKind(1, kind2);
    return vb;
}

// Right rows: two rows (key1, rval1), (key2, rval2) with the given row kinds.
static omnistream::VectorBatch* getRightBatch(int64_t key1, int64_t rval1, int64_t key2, int64_t rval2,
                                              RowKind kind1 = RowKind::INSERT, RowKind kind2 = RowKind::INSERT)
{
    auto vb = new omnistream::VectorBatch(2);
    auto key = new omniruntime::vec::Vector<int64_t>(2);
    key->SetValue(0, key1);
    key->SetValue(1, key2);
    vb->Append(key);
    auto val = new omniruntime::vec::Vector<int64_t>(2);
    val->SetValue(0, rval1);
    val->SetValue(1, rval2);
    vb->Append(val);
    vb->setRowKind(0, kind1);
    vb->setRowKind(1, kind2);
    return vb;
}

// Right rows: single row (key, rval), optionally null payload, with the given row kind.
static omnistream::VectorBatch* getSingleRightBatch(int64_t key, bool nullRval,
                                                    RowKind kind = RowKind::INSERT)
{
    auto vb = new omnistream::VectorBatch(1);
    auto keyVec = new omniruntime::vec::Vector<int64_t>(1);
    keyVec->SetValue(0, key);
    vb->Append(keyVec);
    auto valVec = new omniruntime::vec::Vector<int64_t>(1);
    if (nullRval) {
        valVec->SetNull(0);
    } else {
        valVec->SetValue(0, 100);
    }
    vb->Append(valVec);
    vb->setRowKind(0, kind);
    return vb;
}

// Build operator + runtime env; returns objects via params. Caller frees op/out.
static StreamingJoinOperator<long>* makeOp(const std::string& desc, OutputTestVectorBatch* out)
{
    nlohmann::json parsed = nlohmann::json::parse(desc);
    auto* op = new StreamingJoinOperator<long>(parsed, out);
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
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

// Checks one output batch: every row matches (rowKind, c0..c3) in order.
static ::testing::AssertionResult checkBatch(
    omnistream::VectorBatch* batch, const std::vector<std::tuple<RowKind, std::optional<int64_t>,
        std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>>>& expected)
{
    if (batch->GetRowCount() != (int)expected.size()) {
        return ::testing::AssertionFailure()
            << "row count " << batch->GetRowCount() << " != " << expected.size();
    }
    for (size_t i = 0; i < expected.size(); i++) {
        if (batch->getRowKind(i) != std::get<0>(expected[i])) {
            return ::testing::AssertionFailure()
                << "row " << i << " kind mismatch";
        }
        for (int col = 0; col < 4; col++) {
            auto* vec = static_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(col));
            const auto& want = col == 0 ? std::get<1>(expected[i])
                : col == 1 ? std::get<2>(expected[i])
                : col == 2 ? std::get<3>(expected[i])
                : std::get<4>(expected[i]);
            if (!want.has_value()) {
                if (!vec->IsNull(i)) {
                    return ::testing::AssertionFailure()
                        << "row " << i << " col " << col << " expected null";
                }
            } else if (vec->IsNull(i) || vec->GetValue(i) != want.value()) {
                return ::testing::AssertionFailure()
                    << "row " << i << " col " << col << " value mismatch";
            }
        }
    }
    return ::testing::AssertionSuccess();
}

// ============================================================================
// RIGHT OUTER JOIN: leftIsOuter=false, rightIsOuter=true
// ============================================================================

// Right arrives first (no left match) -> eager null padding; left arrives late ->
// real match + retract of the padding row. Mirrors Flink pseudo code:
// "+I[record+null]" then "-D[null+other]" + "+I[record+other]".
TEST(StreamingJoinOperatorOuterJoinTest, RightEagerPaddingThenRetractOnLeftArrival)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("RightOuterJoin"), out);

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    ASSERT_EQ(out->getAll().size(), 1u);
    EXPECT_TRUE(checkBatch(out->getAll()[0], {
        {RowKind::INSERT, std::nullopt, std::nullopt, 1, 100},
        {RowKind::INSERT, std::nullopt, std::nullopt, 3, 300},
    }));

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    ASSERT_EQ(out->getAll().size(), 2u);
    // (1,10) matches right (1,100): retract padding then emit real pair.
    // (2,20) has no right match: left side is not outer -> no output.
    EXPECT_TRUE(checkBatch(out->getAll()[1], {
        {RowKind::DELETE, std::nullopt, std::nullopt, 1, 100},
        {RowKind::INSERT, 1, 10, 1, 100},
    }));

    op->close();
    delete op;
    delete out;
}

// Left arrives first (no output: left side is inner); right arrives with a left
// match already present -> real match emitted directly, no padding round trip.
TEST(StreamingJoinOperatorOuterJoinTest, RightLeftFirstNoPaddingRoundTrip)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("RightOuterJoin"), out);

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    EXPECT_EQ(out->getAll().size(), 0u);

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    ASSERT_EQ(out->getAll().size(), 1u);
    EXPECT_TRUE(checkBatch(out->getAll()[0], {
        {RowKind::INSERT, 1, 10, 1, 100},
        {RowKind::INSERT, std::nullopt, std::nullopt, 3, 300},
    }));

    op->close();
    delete op;
    delete out;
}

// Right row retract with a match present -> retract the matched pair;
// right row retract without a match -> retract its padding row.
TEST(StreamingJoinOperatorOuterJoinTest, RightRetractRemovesMatchedPairAndPadding)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("RightOuterJoin"), out);

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    ASSERT_EQ(out->getAll().size(), 1u);

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300, RowKind::DELETE, RowKind::DELETE)));
    ASSERT_EQ(out->getAll().size(), 2u);
    EXPECT_TRUE(checkBatch(out->getAll()[1], {
        {RowKind::DELETE, 1, 10, 1, 100},   // matched pair retracted
        {RowKind::DELETE, std::nullopt, std::nullopt, 3, 300}, // padding row retracted
    }));

    op->close();
    delete op;
    delete out;
}

// Left row retract drops the last association of a right row (numAssociate 1 -> 0)
// -> the right row's null padding is restored.
TEST(StreamingJoinOperatorOuterJoinTest, RightLeftRetractRestoresPadding)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("RightOuterJoin"), out);

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    ASSERT_EQ(out->getAll().size(), 2u);

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20, RowKind::DELETE, RowKind::DELETE)));
    ASSERT_EQ(out->getAll().size(), 3u);
    // (1,10) retract: -D real pair, right (1,100) numAssoc 1->0 -> +I padding restored.
    // (2,20) retract: no right match, not outer -> no output.
    EXPECT_TRUE(checkBatch(out->getAll()[2], {
        {RowKind::DELETE, 1, 10, 1, 100},
        {RowKind::INSERT, std::nullopt, std::nullopt, 1, 100},
    }));

    op->close();
    delete op;
    delete out;
}

// Empty left source: every right row keeps its padding row, in arrival order.
TEST(StreamingJoinOperatorOuterJoinTest, RightEmptyLeftAllPadded)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("RightOuterJoin"), out);

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    ASSERT_EQ(out->getAll().size(), 1u);
    EXPECT_TRUE(checkBatch(out->getAll()[0], {
        {RowKind::INSERT, std::nullopt, std::nullopt, 1, 100},
        {RowKind::INSERT, std::nullopt, std::nullopt, 3, 300},
    }));

    op->close();
    delete op;
    delete out;
}

// Duplicate right rows (multiset semantics): each arrival emits one padding row;
// a later left row matches the duplicated entry count times (retract padding x2,
// emit pair x2); retracting the left row restores both padding rows.
TEST(StreamingJoinOperatorOuterJoinTest, RightDuplicateRowsMultiset)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("RightOuterJoin"), out);

    // Two identical right rows arrive while left is empty.
    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 1, 100)));
    ASSERT_EQ(out->getAll().size(), 1u);
    EXPECT_TRUE(checkBatch(out->getAll()[0], {
        {RowKind::INSERT, std::nullopt, std::nullopt, 1, 100},
        {RowKind::INSERT, std::nullopt, std::nullopt, 1, 100},
    }));

    // One left row matches the duplicated right entry count times.
    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    ASSERT_EQ(out->getAll().size(), 2u);
    EXPECT_TRUE(checkBatch(out->getAll()[1], {
        {RowKind::DELETE, std::nullopt, std::nullopt, 1, 100},
        {RowKind::DELETE, std::nullopt, std::nullopt, 1, 100},
        {RowKind::INSERT, 1, 10, 1, 100},
        {RowKind::INSERT, 1, 10, 1, 100},
    }));

    // Retracting one right row instance only decrements the multiset count.
    op->processBatch2(new StreamRecord(getSingleRightBatch(1, false, RowKind::DELETE)));
    ASSERT_EQ(out->getAll().size(), 3u);
    EXPECT_TRUE(checkBatch(out->getAll()[2], {
        {RowKind::DELETE, 1, 10, 1, 100},  // match against the remaining instance
    }));

    op->close();
    delete op;
    delete out;
}

// Null payload on the right side is preserved in the joined output
// (matched row keeps the null value; padding nulls stay null).
TEST(StreamingJoinOperatorOuterJoinTest, RightNullPayloadPreserved)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("RightOuterJoin"), out);

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    op->processBatch2(new StreamRecord(getSingleRightBatch(1, true)));
    ASSERT_EQ(out->getAll().size(), 1u);
    EXPECT_TRUE(checkBatch(out->getAll()[0], {
        {RowKind::INSERT, 1, 10, 1, std::nullopt},
    }));

    op->close();
    delete op;
    delete out;
}

// State layout must mirror Flink OuterJoinRecordStateViews.InputSideHasNoUniqueKey:
// right-records is MapState<RowData, Tuple2<count, numAssociate>> and left-records is
// MapState<RowData, Integer>.
TEST(StreamingJoinOperatorOuterJoinTest, RightStateLayoutMatchesFlinkMapLayout)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("RightOuterJoin"), out);

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    op->setCurrentKey(1L);

    auto* rightType =
        InternalTypeInfo::ofRowType(new omnistream::RowType(true, std::vector<std::string>{"BIGINT", "BIGINT"}));
    auto* rightDescriptor = new MapStateDescriptor<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>(
        "right-records", rightType->createTypeSerializer(), new JoinTupleSerializer());
    rightDescriptor->setKeyValueBackendTypeId(BackendDataType::SHARED_ROW_BK, BackendDataType::TUPLE_INT32_INT32);
    auto* rightState =
        op->getRuntimeContext()->template getMapState<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>(
            rightDescriptor);
    auto* rightEntries = rightState->entries();
    ASSERT_NE(rightEntries, nullptr);
    ASSERT_EQ(rightEntries->size(), 1u);
    for (const auto& entry : *rightEntries) {
        EXPECT_EQ(*entry.first->getLong(0), 1);
        EXPECT_EQ(*entry.first->getLong(1), 100);
        EXPECT_EQ(std::get<0>(entry.second), 1);  // duplicate count
        EXPECT_EQ(std::get<1>(entry.second), 1);  // associated with left row (1,10)
    }

    auto* leftType =
        InternalTypeInfo::ofRowType(new omnistream::RowType(true, std::vector<std::string>{"BIGINT", "BIGINT"}));
    auto* leftDescriptor = new MapStateDescriptor<std::shared_ptr<RowData>, int32_t>(
        "left-records", leftType->createTypeSerializer(), new IntSerializer());
    leftDescriptor->setKeyValueBackendTypeId(BackendDataType::SHARED_ROW_BK, BackendDataType::INT_BK);
    auto* leftState =
        op->getRuntimeContext()->template getMapState<std::shared_ptr<RowData>, int32_t>(leftDescriptor);
    auto* leftEntries = leftState->entries();
    ASSERT_NE(leftEntries, nullptr);
    ASSERT_EQ(leftEntries->size(), 1u);
    for (const auto& entry : *leftEntries) {
        EXPECT_EQ(*entry.first->getLong(0), 1);
        EXPECT_EQ(*entry.first->getLong(1), 10);
        EXPECT_EQ(entry.second, 1);
    }

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// FULL OUTER JOIN: leftIsOuter=true, rightIsOuter=true
// ============================================================================

// Left rows arrive first -> eager left padding; right rows arrive ->
// real match + retract of the left padding; unmatched right row -> its own padding.
TEST(StreamingJoinOperatorOuterJoinTest, FullBothSidesPaddingAndRetractOnMatch)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("FullOuterJoin"), out);

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    ASSERT_EQ(out->getAll().size(), 1u);
    EXPECT_TRUE(checkBatch(out->getAll()[0], {
        {RowKind::INSERT, 1, 10, std::nullopt, std::nullopt},
        {RowKind::INSERT, 2, 20, std::nullopt, std::nullopt},
    }));

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    ASSERT_EQ(out->getAll().size(), 2u);
    EXPECT_TRUE(checkBatch(out->getAll()[1], {
        {RowKind::DELETE, 1, 10, std::nullopt, std::nullopt}, // retract left padding first
        {RowKind::INSERT, 1, 10, 1, 100},   // then the real match
        {RowKind::INSERT, std::nullopt, std::nullopt, 3, 300}, // right padding
    }));

    op->close();
    delete op;
    delete out;
}

// FULL symmetric restore: right row retract drops the last association of a left
// row (numAssociate 1 -> 0) -> the left row's null padding is restored.
TEST(StreamingJoinOperatorOuterJoinTest, FullSymmetricRestoreOnRightRetract)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("FullOuterJoin"), out);

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    ASSERT_EQ(out->getAll().size(), 2u);

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300, RowKind::DELETE, RowKind::DELETE)));
    ASSERT_EQ(out->getAll().size(), 3u);
    // Retract right (1,100): -D real pair, left (1,10) numAssoc 1->0 -> padding restored.
    // Retract right (3,300): its own padding row retracted.
    EXPECT_TRUE(checkBatch(out->getAll()[2], {
        {RowKind::DELETE, 1, 10, 1, 100},
        {RowKind::INSERT, 1, 10, std::nullopt, std::nullopt},
        {RowKind::DELETE, std::nullopt, std::nullopt, 3, 300},
    }));

    op->close();
    delete op;
    delete out;
}

// FULL: right rows arrive first -> eager right padding; left arrives late ->
// real match + retract of the right padding (mirror of the left-first case).
TEST(StreamingJoinOperatorOuterJoinTest, FullRightFirstPaddingThenLeftArrival)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("FullOuterJoin"), out);

    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    ASSERT_EQ(out->getAll().size(), 1u);
    EXPECT_TRUE(checkBatch(out->getAll()[0], {
        {RowKind::INSERT, std::nullopt, std::nullopt, 1, 100},
        {RowKind::INSERT, std::nullopt, std::nullopt, 3, 300},
    }));

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    ASSERT_EQ(out->getAll().size(), 2u);
    EXPECT_TRUE(checkBatch(out->getAll()[1], {
        {RowKind::DELETE, std::nullopt, std::nullopt, 1, 100}, // retract right padding
        {RowKind::INSERT, 1, 10, 1, 100},   // real match
        {RowKind::INSERT, 2, 20, std::nullopt, std::nullopt},  // left padding
    }));

    op->close();
    delete op;
    delete out;
}

// FULL: both state views carry the outer layout
// MapState<RowData, Tuple2<count, numAssociate>> on both sides.
TEST(StreamingJoinOperatorOuterJoinTest, FullStateLayoutBothOuterViews)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(withJoinType("FullOuterJoin"), out);

    op->processBatch1(new StreamRecord(getLeftBatch(1, 10, 2, 20)));
    op->processBatch2(new StreamRecord(getRightBatch(1, 100, 3, 300)));
    op->setCurrentKey(1L);

    auto* rightType =
        InternalTypeInfo::ofRowType(new omnistream::RowType(true, std::vector<std::string>{"BIGINT", "BIGINT"}));
    auto* rightDescriptor = new MapStateDescriptor<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>(
        "right-records", rightType->createTypeSerializer(), new JoinTupleSerializer());
    rightDescriptor->setKeyValueBackendTypeId(BackendDataType::SHARED_ROW_BK, BackendDataType::TUPLE_INT32_INT32);
    auto* rightState =
        op->getRuntimeContext()->template getMapState<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>(
            rightDescriptor);
    auto* rightEntries = rightState->entries();
    ASSERT_NE(rightEntries, nullptr);
    ASSERT_EQ(rightEntries->size(), 1u);
    for (const auto& entry : *rightEntries) {
        EXPECT_EQ(std::get<0>(entry.second), 1);
        EXPECT_EQ(std::get<1>(entry.second), 1); // associated with left (1,10)
    }

    auto* leftType =
        InternalTypeInfo::ofRowType(new omnistream::RowType(true, std::vector<std::string>{"BIGINT", "BIGINT"}));
    auto* leftDescriptor = new MapStateDescriptor<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>(
        "left-records", leftType->createTypeSerializer(), new JoinTupleSerializer());
    leftDescriptor->setKeyValueBackendTypeId(BackendDataType::SHARED_ROW_BK, BackendDataType::TUPLE_INT32_INT32);
    auto* leftState =
        op->getRuntimeContext()->template getMapState<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>(
            leftDescriptor);
    auto* leftEntries = leftState->entries();
    ASSERT_NE(leftEntries, nullptr);
    ASSERT_EQ(leftEntries->size(), 1u);
    for (const auto& entry : *leftEntries) {
        EXPECT_EQ(std::get<0>(entry.second), 1);
        EXPECT_EQ(std::get<1>(entry.second), 1); // associated with right (1,100)
    }

    op->close();
    delete op;
    delete out;
}
