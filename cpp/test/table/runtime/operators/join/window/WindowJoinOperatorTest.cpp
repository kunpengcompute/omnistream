#include <gtest/gtest.h>
#include <memory>
#include "table/runtime/operators/join/window/WindowJoinOperator.h"
#include "table/runtime/operators/join/window/InnerJoinOperator.h"
#include "table/runtime/operators/join/window/LeftOuterJoinOperator.h"
#include "table/runtime/operators/join/window/RightOuterJoinOperator.h"
#include "table/runtime/operators/join/window/FullOuterJoinOperator.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "test/core/operators/OutputTest.h"
#include "core/typeutils/LongSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
std::string testDescriptionEqui = R"delimiter({
  "originDescription": null,
  "leftInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "rightInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "outputTypes": [
    "INT",
    "BIGINT",
    "INT",
    "INT",
    "BIGINT",
    "INT"
  ],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": null,
  "joinType": "InnerJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

using namespace omnistream;
nlohmann::json parsedJsonEqui = nlohmann::json::parse(testDescriptionEqui);

// Left outer join with a residual non-equi predicate on the value column
// (left value < right value). Records are grouped by the single join key [0].
std::string testDescriptionLeftOuterNonEqui = R"delimiter({
  "originDescription": null,
  "leftInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "rightInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "outputTypes": [
    "INT",
    "BIGINT",
    "INT",
    "INT",
    "BIGINT",
    "INT"
  ],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": {"exprType":"BINARY","returnType":4,"operator":"LESS_THAN","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":2},"right":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":5}},
  "joinType": "LeftOuterJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

nlohmann::json parsedJsonLeftOuterNonEqui = nlohmann::json::parse(testDescriptionLeftOuterNonEqui);

template <typename T>
bool compareCol(omniruntime::vec::BaseVector* col1, omniruntime::vec::BaseVector* col2)
{
    if (col1->GetSize() != col2->GetSize()) {
        return false;
    }

    if (col1->GetTypeId() != col2->GetTypeId()) {
        return false;
    }

    omniruntime::vec::Vector<T>* col1cast = static_cast<omniruntime::vec::Vector<T>*>(col1);
    omniruntime::vec::Vector<T>* col2cast = static_cast<omniruntime::vec::Vector<T>*>(col2);

    if (col1cast->GetNullCount() != col2cast->GetNullCount()) {
        return false;
    }

    for (int i = 0; i < col1cast->GetSize(); i++) {
        auto c1v = col1cast->GetValue(i);
        auto c2v = col2cast->GetValue(i);
        auto c1null = col1cast->IsNull(i);
        auto c2null = col2cast->IsNull(i);
        if (c1null == c2null && c1null == true) {
            return true;
        }

        if (c1v != c2v) {
            return false;
        }
    }
    return true;
};

bool inOutput(omnistream::VectorBatch* batch, OutputTestVectorBatch* out)
{
    for (auto outBatch : out->getAll()) {
        if (omniruntime::TestUtil::VecBatchMatch(outBatch, batch)) {
            return true;
        }
    }
    return false;
};

omnistream::VectorBatch* getLeftBatch()
{
    // Batch left
    /*
    (int) , (long)       , (int)
    KeyCol, WindowEndTime, value
    0     , 1000         , 12
    1     , 1000         , 24
    2     , 1000         , 36
    */

    auto vbatchLeft = new omnistream::VectorBatch(3);

    // Key Column
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(3);
    for (int j = 0; j < 3; j++) {
        vKeyLeft->SetValue(j, j);
    }
    vbatchLeft->Append(vKeyLeft);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(3);
    for (int j = 0; j < 3; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000);
    }
    vbatchLeft->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(3);
    for (int j = 1; j < 4; j++) {
        vValLeft->SetValue(j - 1, j * 12);
    }
    vbatchLeft->Append(vValLeft);
    return vbatchLeft;
}

omnistream::VectorBatch* getRightBatch()
{
    // Batch right
    /*
    (int) , (long)       , (int)
    KeyCol, WindowEndTime, value
    0     , 1000         , 100
    1     , 1000         , 200
    1     , 1000         , 300
    3     , 1000         , 400
    */

    auto vbatchRight = new omnistream::VectorBatch(4);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(4);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 1);
    vKeyRight->SetValue(2, 1);
    vKeyRight->SetValue(3, 3);
    vbatchRight->Append(vKeyRight);

    auto vWindowEndTimeRight = new omniruntime::vec::Vector<int64_t>(4);
    for (int i = 0; i < 4; i++) {
        vWindowEndTimeRight->SetValue(i, 1000);
    }
    vbatchRight->Append(vWindowEndTimeRight);

    auto vValRight = new omniruntime::vec::Vector<int32_t>(4);
    vValRight->SetValue(0, 100);
    vValRight->SetValue(1, 200);
    vValRight->SetValue(2, 300);
    vValRight->SetValue(3, 400);

    vbatchRight->Append(vValRight);
    return vbatchRight;
}

TEST(WindowJoinOperatorTest, DISABLED_InnerJoinTest)
{
    auto vbatchLeft = getLeftBatch();
    auto vbatchRight = getRightBatch();

    auto* out = new OutputTestVectorBatch();
    auto op = new InnerJoinOperator<int32_t>(parsedJsonEqui, out, new LongSerializer(), new LongSerializer());
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
    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));

    op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Output Batch 1
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    0     , 1000         , 12   , 0     , 1000         , 100

    Output Batch 2
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    1     , 1000         , 24   , 1     , 1000         , 200
    1     , 1000         , 24   , 1     , 1000         , 300
    */

    auto v11 = new omniruntime::vec::Vector<int32_t>(1);
    v11->SetValue(0, 0);
    auto v12 = new omniruntime::vec::Vector<int64_t>(1);
    v12->SetValue(0, 1000);
    auto v13 = new omniruntime::vec::Vector<int32_t>(1);
    v13->SetValue(0, 12);
    auto v14 = new omniruntime::vec::Vector<int32_t>(1);
    v14->SetValue(0, 0);
    auto v15 = new omniruntime::vec::Vector<int64_t>(1);
    v15->SetValue(0, 1000);
    auto v16 = new omniruntime::vec::Vector<int32_t>(1);
    v16->SetValue(0, 100);
    auto b1 = new omnistream::VectorBatch(1);
    b1->Append(v11);
    b1->Append(v12);
    b1->Append(v13);
    b1->Append(v14);
    b1->Append(v15);
    b1->Append(v16);

    EXPECT_TRUE(inOutput(b1, out));

    auto v21 = new omniruntime::vec::Vector<int32_t>(2);
    v21->SetValue(0, 1);
    v21->SetValue(1, 1);
    auto v22 = new omniruntime::vec::Vector<int64_t>(2);
    v22->SetValue(0, 1000);
    v22->SetValue(1, 1000);
    auto v23 = new omniruntime::vec::Vector<int32_t>(2);
    v23->SetValue(0, 24);
    v23->SetValue(1, 24);
    auto v24 = new omniruntime::vec::Vector<int32_t>(2);
    v24->SetValue(0, 1);
    v24->SetValue(1, 1);
    auto v25 = new omniruntime::vec::Vector<int64_t>(2);
    v25->SetValue(0, 1000);
    v25->SetValue(1, 1000);
    auto v26 = new omniruntime::vec::Vector<int32_t>(2);
    v26->SetValue(0, 200);
    v26->SetValue(1, 300);
    auto b2 = new omnistream::VectorBatch(2);
    b2->Append(v21);
    b2->Append(v22);
    b2->Append(v23);
    b2->Append(v24);
    b2->Append(v25);
    b2->Append(v26);

    EXPECT_TRUE(inOutput(b2, out));

    op->close();
    //    delete op;
    delete initializer;
    delete out;
    delete b1;
    delete b2;
}

TEST(WindowJoinOperatorTest, DISABLED_InnerJoinTest_BinaryRowDataKey)
{
    auto vbatchLeft = getLeftBatch();
    auto vbatchRight = getRightBatch();

    auto* out = new OutputTestVectorBatch();
    auto op = new InnerJoinOperator<std::shared_ptr<RowData>>(
        parsedJsonEqui, out, new LongSerializer(), new LongSerializer());
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
    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));

    op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Output Batch 1
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    0     , 1000         , 12   , 0     , 1000         , 100

    Output Batch 2
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    1     , 1000         , 24   , 1     , 1000         , 200
    1     , 1000         , 24   , 1     , 1000         , 300
    */

    auto v11 = new omniruntime::vec::Vector<int32_t>(1);
    v11->SetValue(0, 0);
    auto v12 = new omniruntime::vec::Vector<int64_t>(1);
    v12->SetValue(0, 1000);
    auto v13 = new omniruntime::vec::Vector<int32_t>(1);
    v13->SetValue(0, 12);
    auto v14 = new omniruntime::vec::Vector<int32_t>(1);
    v14->SetValue(0, 0);
    auto v15 = new omniruntime::vec::Vector<int64_t>(1);
    v15->SetValue(0, 1000);
    auto v16 = new omniruntime::vec::Vector<int32_t>(1);
    v16->SetValue(0, 100);
    auto b1 = new omnistream::VectorBatch(1);
    b1->Append(v11);
    b1->Append(v12);
    b1->Append(v13);
    b1->Append(v14);
    b1->Append(v15);
    b1->Append(v16);

    EXPECT_TRUE(inOutput(b1, out));

    auto v21 = new omniruntime::vec::Vector<int32_t>(2);
    v21->SetValue(0, 1);
    v21->SetValue(1, 1);
    auto v22 = new omniruntime::vec::Vector<int64_t>(2);
    v22->SetValue(0, 1000);
    v22->SetValue(1, 1000);
    auto v23 = new omniruntime::vec::Vector<int32_t>(2);
    v23->SetValue(0, 24);
    v23->SetValue(1, 24);
    auto v24 = new omniruntime::vec::Vector<int32_t>(2);
    v24->SetValue(0, 1);
    v24->SetValue(1, 1);
    auto v25 = new omniruntime::vec::Vector<int64_t>(2);
    v25->SetValue(0, 1000);
    v25->SetValue(1, 1000);
    auto v26 = new omniruntime::vec::Vector<int32_t>(2);
    v26->SetValue(0, 200);
    v26->SetValue(1, 300);
    auto b2 = new omnistream::VectorBatch(2);
    b2->Append(v21);
    b2->Append(v22);
    b2->Append(v23);
    b2->Append(v24);
    b2->Append(v25);
    b2->Append(v26);

    EXPECT_TRUE(inOutput(b2, out));

    op->close();
    //    delete op;
    delete initializer;
    delete out;
    delete b1;
    delete b2;
}

// One expected output row for the LeftOuterJoin equi tests. When rightNull is true the three
// right-side columns are emitted as null (null-padded unmatched left row).
struct LeftOuterJoinRowSpec {
    int32_t leftKey;
    int64_t leftWindowEnd;
    int32_t leftValue;
    int32_t rightKey;
    int64_t rightWindowEnd;
    int32_t rightValue;
    bool rightNull;
};

omnistream::VectorBatch* buildLeftOuterExpectedBatch(const std::vector<LeftOuterJoinRowSpec>& specs)
{
    auto numRows = static_cast<int>(specs.size());
    auto batch = new omnistream::VectorBatch(numRows);
    auto v0 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v1 = new omniruntime::vec::Vector<int64_t>(numRows);
    auto v2 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v3 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v4 = new omniruntime::vec::Vector<int64_t>(numRows);
    auto v5 = new omniruntime::vec::Vector<int32_t>(numRows);
    int row = 0;
    for (const auto& spec : specs) {
        v0->SetValue(row, spec.leftKey);
        v1->SetValue(row, spec.leftWindowEnd);
        v2->SetValue(row, spec.leftValue);
        if (spec.rightNull) {
            v3->SetNull(row);
            v4->SetNull(row);
            v5->SetNull(row);
        } else {
            v3->SetValue(row, spec.rightKey);
            v4->SetValue(row, spec.rightWindowEnd);
            v5->SetValue(row, spec.rightValue);
        }
        row++;
    }
    batch->Append(v0);
    batch->Append(v1);
    batch->Append(v2);
    batch->Append(v3);
    batch->Append(v4);
    batch->Append(v5);
    return batch;
}

struct LeftOuterJoinTestContext {
    LeftOuterJoinOperator<int32_t>* op = nullptr;
    OutputTestVectorBatch* out = nullptr;
    StreamTaskStateInitializerImpl* initializer = nullptr;
    omnistream::RuntimeEnvironmentV2* env2 = nullptr;
    TaskInformationPOD* taskInfo = nullptr;
};

// Creates a ready-to-run LeftOuterJoinOperator using the same HashMapStateBackend setup
// shared by all LeftOuterJoin tests.
LeftOuterJoinTestContext createLeftOuterJoinTestContext(const nlohmann::json& desc)
{
    LeftOuterJoinTestContext ctx;
    ctx.out = new OutputTestVectorBatch();
    ctx.env2 = new omnistream::RuntimeEnvironmentV2();
    ctx.taskInfo = new TaskInformationPOD();
    ctx.taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = ctx.taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        ctx.taskInfo->setStreamConfigPOD(configPOD);
    }
    ctx.env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    ctx.env2->setTaskConfiguration(*ctx.taskInfo);
    ctx.initializer = new StreamTaskStateInitializerImpl(ctx.env2);
    ctx.op = new LeftOuterJoinOperator<int32_t>(desc, ctx.out, new LongSerializer(), new LongSerializer());
    ctx.op->setup();
    ctx.op->initializeState(ctx.initializer, new IntSerializer());
    ctx.op->open();
    return ctx;
}

void destroyLeftOuterJoinTestContext(LeftOuterJoinTestContext& ctx)
{
    if (ctx.op != nullptr) {
        ctx.op->close();
        delete ctx.op;
    }
    delete ctx.initializer;
    delete ctx.out;
    // env2/taskInfo are intentionally not freed, matching the existing tests in this file.
}

TEST(WindowJoinOperatorTest, LeftOuterJoinTest)
{
    // Same input batches and identical expected output for both the equi description and
    // the non-equi (left.value < right.value) description, so the two join paths are
    // verified together with the same assertions.
    auto b1 = buildLeftOuterExpectedBatch({LeftOuterJoinRowSpec{0, 1000, 12, 0, 1000, 100, false}});
    auto b2 = buildLeftOuterExpectedBatch(
        {LeftOuterJoinRowSpec{1, 1000, 24, 1, 1000, 200, false},
         LeftOuterJoinRowSpec{1, 1000, 24, 1, 1000, 300, false}});
    auto b3 = buildLeftOuterExpectedBatch({LeftOuterJoinRowSpec{2, 1000, 36, 0, 0, 0, true}});

    for (const auto& desc : {parsedJsonEqui, parsedJsonLeftOuterNonEqui}) {
        auto ctx = createLeftOuterJoinTestContext(desc);
        ctx.op->processBatch1(new StreamRecord(getLeftBatch()));
        ctx.op->processBatch2(new StreamRecord(getRightBatch()));
        ctx.op->getInternalTimerService()->advanceWatermark(100000);

        EXPECT_TRUE(inOutput(b1, ctx.out));
        EXPECT_TRUE(inOutput(b2, ctx.out));
        EXPECT_TRUE(inOutput(b3, ctx.out));

        destroyLeftOuterJoinTestContext(ctx);
    }

    delete b1;
    delete b2;
    delete b3;
}

TEST(WindowJoinOperatorTest, LeftOuterJoinWindowSeparationTest)
{
    // Left batch:
    //   (key=0, windowEnd=1000, value=12) -> matches right (0,1000,100)
    //   (key=1, windowEnd=2000, value=24) -> left-only window -> right columns null-padded
    auto vbatchLeft = new omnistream::VectorBatch(2);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(2);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vWindLeft = new omniruntime::vec::Vector<int64_t>(2);
    vWindLeft->SetValue(0, 1000);
    vWindLeft->SetValue(1, 2000);
    vbatchLeft->Append(vWindLeft);
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(2);
    vValLeft->SetValue(0, 12);
    vValLeft->SetValue(1, 24);
    vbatchLeft->Append(vValLeft);

    // Right batch:
    //   (key=0, windowEnd=1000, value=100) -> matches left (0,1000,12)
    //   (key=2, windowEnd=3000, value=300) -> right-only window -> LeftOuterJoin emits nothing
    auto vbatchRight = new omnistream::VectorBatch(2);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(2);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 2);
    vbatchRight->Append(vKeyRight);
    auto vWindRight = new omniruntime::vec::Vector<int64_t>(2);
    vWindRight->SetValue(0, 1000);
    vWindRight->SetValue(1, 3000);
    vbatchRight->Append(vWindRight);
    auto vValRight = new omniruntime::vec::Vector<int32_t>(2);
    vValRight->SetValue(0, 100);
    vValRight->SetValue(1, 300);
    vbatchRight->Append(vValRight);

    auto ctx = createLeftOuterJoinTestContext(parsedJsonEqui);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    // Watermark 1500 fires only window 1000; windows 2000/3000 stay pending.
    ctx.op->getInternalTimerService()->advanceWatermark(1500);

    auto b1 = buildLeftOuterExpectedBatch({LeftOuterJoinRowSpec{0, 1000, 12, 0, 1000, 100, false}});
    auto b2 = buildLeftOuterExpectedBatch({LeftOuterJoinRowSpec{1, 2000, 24, 0, 0, 0, true}});

    EXPECT_TRUE(inOutput(b1, ctx.out));
    EXPECT_TRUE(!inOutput(b2, ctx.out));
    ASSERT_EQ(static_cast<size_t>(1), ctx.out->getAll().size());

    // Watermark 2500 fires window 2000 (left-only) -> right columns null-padded.
    ctx.op->getInternalTimerService()->advanceWatermark(2500);
    EXPECT_TRUE(inOutput(b2, ctx.out));
    ASSERT_EQ(static_cast<size_t>(2), ctx.out->getAll().size());

    // Watermark 3500 fires window 3000 (right-only) -> LeftOuterJoin emits nothing.
    ctx.op->getInternalTimerService()->advanceWatermark(3500);
    ASSERT_EQ(static_cast<size_t>(2), ctx.out->getAll().size());

    destroyLeftOuterJoinTestContext(ctx);
    delete b1;
    delete b2;
}

TEST(WindowJoinOperatorTest, LeftOuterJoinLateDataTest)
{
    auto ctx = createLeftOuterJoinTestContext(parsedJsonEqui);
    // Advance the watermark first so window 1000 is already fired; any row whose window end
    // has already been triggered arriving afterwards is late and must be dropped.
    ctx.op->getInternalTimerService()->advanceWatermark(1500);

    // Late rows for window 1000 (dropped) plus valid rows for window 2000.
    auto vbatchLeft = new omnistream::VectorBatch(2);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(2);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vWindLeft = new omniruntime::vec::Vector<int64_t>(2);
    vWindLeft->SetValue(0, 1000);
    vWindLeft->SetValue(1, 2000);
    vbatchLeft->Append(vWindLeft);
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(2);
    vValLeft->SetValue(0, 12);
    vValLeft->SetValue(1, 24);
    vbatchLeft->Append(vValLeft);

    auto vbatchRight = new omnistream::VectorBatch(2);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(2);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 1);
    vbatchRight->Append(vKeyRight);
    auto vWindRight = new omniruntime::vec::Vector<int64_t>(2);
    vWindRight->SetValue(0, 1000);
    vWindRight->SetValue(1, 2000);
    vbatchRight->Append(vWindRight);
    auto vValRight = new omniruntime::vec::Vector<int32_t>(2);
    vValRight->SetValue(0, 100);
    vValRight->SetValue(1, 200);
    vbatchRight->Append(vValRight);

    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    // Only window 2000 fires; the late window 1000 rows must never be emitted.
    ctx.op->getInternalTimerService()->advanceWatermark(2500);

    auto b1 = buildLeftOuterExpectedBatch({LeftOuterJoinRowSpec{1, 2000, 24, 1, 2000, 200, false}});
    ASSERT_EQ(static_cast<size_t>(1), ctx.out->getAll().size());
    EXPECT_TRUE(inOutput(b1, ctx.out));

    destroyLeftOuterJoinTestContext(ctx);
    delete b1;
}

TEST(WindowJoinOperatorTest, LeftOuterJoinNullKeyTest)
{
    // Null join keys must not produce any matched pair (Flink JoinConditionWithNullFilters
    // semantics): even though both sides have records in the same key group, the left rows
    // are emitted with the right columns null-padded.
    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetNull(0);
    vbatchLeft->Append(vKeyLeft);
    auto vWindLeft = new omniruntime::vec::Vector<int64_t>(1);
    vWindLeft->SetValue(0, 1000);
    vbatchLeft->Append(vWindLeft);
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(1);
    vValLeft->SetValue(0, 1);
    vbatchLeft->Append(vValLeft);

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetNull(0);
    vbatchRight->Append(vKeyRight);
    auto vWindRight = new omniruntime::vec::Vector<int64_t>(1);
    vWindRight->SetValue(0, 1000);
    vbatchRight->Append(vWindRight);
    auto vValRight = new omniruntime::vec::Vector<int32_t>(1);
    vValRight->SetValue(0, 2);
    vbatchRight->Append(vValRight);

    auto ctx = createLeftOuterJoinTestContext(parsedJsonEqui);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(1), batches.size());
    auto batch = batches[0];
    ASSERT_EQ(1, batch->GetRowCount());
    auto* lKey = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(0));
    auto* lWind = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(1));
    auto* lVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(2));
    auto* rKey = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(3));
    auto* rWind = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(4));
    auto* rVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(5));
    EXPECT_TRUE(lKey->IsNull(0));
    EXPECT_FALSE(lWind->IsNull(0));
    EXPECT_EQ(1000, lWind->GetValue(0));
    EXPECT_FALSE(lVal->IsNull(0));
    EXPECT_EQ(1, lVal->GetValue(0));
    EXPECT_TRUE(rKey->IsNull(0));
    EXPECT_TRUE(rWind->IsNull(0));
    EXPECT_TRUE(rVal->IsNull(0));

    destroyLeftOuterJoinTestContext(ctx);
}

// Builds a 3-column INT / BIGINT / INT batch (join key, window end, value) from parallel
// vectors, shrinking the input construction boilerplate in the join operator tests.
omnistream::VectorBatch* makeInt3Batch(
    const std::vector<int32_t>& keys, const std::vector<int64_t>& windowEnds, const std::vector<int32_t>& values)
{
    auto numRows = static_cast<int>(keys.size());
    auto batch = new omnistream::VectorBatch(numRows);
    auto vKey = new omniruntime::vec::Vector<int32_t>(numRows);
    auto vWindowEnd = new omniruntime::vec::Vector<int64_t>(numRows);
    auto vValue = new omniruntime::vec::Vector<int32_t>(numRows);
    for (int i = 0; i < numRows; i++) {
        vKey->SetValue(i, keys[i]);
        vWindowEnd->SetValue(i, windowEnds[i]);
        vValue->SetValue(i, values[i]);
    }
    batch->Append(vKey);
    batch->Append(vWindowEnd);
    batch->Append(vValue);
    return batch;
}

std::string testDescriptionRightOuterNonEqui = R"delimiter({
  "originDescription": null,
  "leftInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "rightInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "outputTypes": [
    "INT",
    "BIGINT",
    "INT",
    "INT",
    "BIGINT",
    "INT"
  ],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": {"exprType":"BINARY","returnType":4,"operator":"LESS_THAN","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":2},"right":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":5}},
  "joinType": "RightOuterJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

nlohmann::json parsedJsonRightOuterNonEqui = nlohmann::json::parse(testDescriptionRightOuterNonEqui);

std::string testDescriptionRightOuterVarchar = R"delimiter({
  "originDescription": null,
  "leftInputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "rightInputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "outputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)",
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": null,
  "joinType": "RightOuterJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

nlohmann::json parsedJsonRightOuterVarchar = nlohmann::json::parse(testDescriptionRightOuterVarchar);

// One expected output row for the RightOuterJoin tests. When leftNull is true the three
// left-side columns are emitted as null (null-padded unmatched right row).
struct RightOuterJoinRowSpec {
    int32_t leftKey;
    int64_t leftWindowEnd;
    int32_t leftValue;
    int32_t rightKey;
    int64_t rightWindowEnd;
    int32_t rightValue;
    bool leftNull;
};

omnistream::VectorBatch* buildRightOuterExpectedBatch(const std::vector<RightOuterJoinRowSpec>& specs)
{
    auto numRows = static_cast<int>(specs.size());
    auto batch = new omnistream::VectorBatch(numRows);
    auto v0 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v1 = new omniruntime::vec::Vector<int64_t>(numRows);
    auto v2 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v3 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v4 = new omniruntime::vec::Vector<int64_t>(numRows);
    auto v5 = new omniruntime::vec::Vector<int32_t>(numRows);
    int row = 0;
    for (const auto& spec : specs) {
        if (spec.leftNull) {
            v0->SetNull(row);
            v1->SetNull(row);
            v2->SetNull(row);
        } else {
            v0->SetValue(row, spec.leftKey);
            v1->SetValue(row, spec.leftWindowEnd);
            v2->SetValue(row, spec.leftValue);
        }
        v3->SetValue(row, spec.rightKey);
        v4->SetValue(row, spec.rightWindowEnd);
        v5->SetValue(row, spec.rightValue);
        row++;
    }
    batch->Append(v0);
    batch->Append(v1);
    batch->Append(v2);
    batch->Append(v3);
    batch->Append(v4);
    batch->Append(v5);
    return batch;
}

struct RightOuterJoinTestContext {
    RightOuterJoinOperator<int32_t>* op = nullptr;
    OutputTestVectorBatch* out = nullptr;
    StreamTaskStateInitializerImpl* initializer = nullptr;
    omnistream::RuntimeEnvironmentV2* env2 = nullptr;
    TaskInformationPOD* taskInfo = nullptr;
};

// Creates a ready-to-run RightOuterJoinOperator using the same HashMapStateBackend setup
// shared by all RightOuterJoin tests.
RightOuterJoinTestContext createRightOuterJoinTestContext(const nlohmann::json& desc)
{
    RightOuterJoinTestContext ctx;
    ctx.out = new OutputTestVectorBatch();
    ctx.env2 = new omnistream::RuntimeEnvironmentV2();
    ctx.taskInfo = new TaskInformationPOD();
    ctx.taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = ctx.taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        ctx.taskInfo->setStreamConfigPOD(configPOD);
    }
    ctx.env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    ctx.env2->setTaskConfiguration(*ctx.taskInfo);
    ctx.initializer = new StreamTaskStateInitializerImpl(ctx.env2);
    ctx.op = new RightOuterJoinOperator<int32_t>(desc, ctx.out, new LongSerializer(), new LongSerializer());
    ctx.op->setup();
    ctx.op->initializeState(ctx.initializer, new IntSerializer());
    ctx.op->open();
    return ctx;
}

void destroyRightOuterJoinTestContext(RightOuterJoinTestContext& ctx)
{
    if (ctx.op != nullptr) {
        ctx.op->close();
        delete ctx.op;
    }
    delete ctx.initializer;
    delete ctx.out;
    // env2/taskInfo are intentionally not freed, matching the existing tests in this file.
}

TEST(WindowJoinOperatorTest, RightOuterJoinTest)
{
    // Left: key-1 group with three rows (many-to-many with the right key-1 group), plus a
    // left-only key-2 group. Right: the key-1 group with two rows, plus a right-only key-3
    // group. This covers the equi and non-equi paths, right-major ordering, per-right-record
    // emission and left-null padding for right-only records in one scenario.
    auto bPad = buildRightOuterExpectedBatch({RightOuterJoinRowSpec{0, 0, 0, 3, 1000, 400, true}});

    // Equi description: the key-1 group emits a single right-major 6-row batch
    // (10,100),(20,100),(30,100),(10,200),(20,200),(30,200); the key-2 left-only group emits
    // nothing; the key-3 right-only group emits one left-null-padded row.
    {
        auto ctx = createRightOuterJoinTestContext(parsedJsonEqui);
        ctx.op->processBatch1(new StreamRecord(makeInt3Batch({1, 1, 1, 2}, {1000, 1000, 1000, 1000}, {10, 20, 30, 36})));
        ctx.op->processBatch2(new StreamRecord(makeInt3Batch({1, 1, 3}, {1000, 1000, 1000}, {100, 200, 400})));
        ctx.op->getInternalTimerService()->advanceWatermark(100000);

        auto& batches = ctx.out->getAll();
        ASSERT_EQ(static_cast<size_t>(2), batches.size());
        omnistream::VectorBatch* orderBatch = nullptr;
        omnistream::VectorBatch* padBatch = nullptr;
        for (auto b : batches) {
            if (b->GetRowCount() == 6) {
                orderBatch = b;
            } else {
                padBatch = b;
            }
        }
        ASSERT_NE(orderBatch, nullptr);
        ASSERT_NE(padBatch, nullptr);

        auto* lVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(orderBatch->Get(2));
        auto* rVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(orderBatch->Get(5));
        const std::vector<int32_t> expectedLVal = {10, 20, 30, 10, 20, 30};
        const std::vector<int32_t> expectedRVal = {100, 100, 100, 200, 200, 200};
        for (int i = 0; i < 6; i++) {
            EXPECT_FALSE(lVal->IsNull(i)) << "left value null at row " << i;
            EXPECT_FALSE(rVal->IsNull(i)) << "right value null at row " << i;
            EXPECT_EQ(expectedLVal[i], lVal->GetValue(i)) << "left value mismatch at row " << i;
            EXPECT_EQ(expectedRVal[i], rVal->GetValue(i)) << "right value mismatch at row " << i;
        }

        EXPECT_TRUE(inOutput(bPad, ctx.out));
        destroyRightOuterJoinTestContext(ctx);
    }

    // Non-equi description (left.value < right.value): every candidate pair still matches, so
    // the emitted rows are the same, but the key-1 group is emitted per right record as two
    // 3-row batches: (10,100),(20,100),(30,100) and (10,200),(20,200),(30,200).
    {
        auto ctx = createRightOuterJoinTestContext(parsedJsonRightOuterNonEqui);
        ctx.op->processBatch1(new StreamRecord(makeInt3Batch({1, 1, 1, 2}, {1000, 1000, 1000, 1000}, {10, 20, 30, 36})));
        ctx.op->processBatch2(new StreamRecord(makeInt3Batch({1, 1, 3}, {1000, 1000, 1000}, {100, 200, 400})));
        ctx.op->getInternalTimerService()->advanceWatermark(100000);

        auto& batches = ctx.out->getAll();
        ASSERT_EQ(static_cast<size_t>(3), batches.size());
        bool foundRight100 = false;
        bool foundRight200 = false;
        for (auto b : batches) {
            if (b->GetRowCount() == 1) {
                EXPECT_TRUE(inOutput(bPad, ctx.out));
                continue;
            }
            auto* lVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(b->Get(2));
            auto* rVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(b->Get(5));
            ASSERT_EQ(3, b->GetRowCount());
            for (int i = 0; i < 3; i++) {
                EXPECT_FALSE(lVal->IsNull(i)) << "left value null at row " << i;
                EXPECT_FALSE(rVal->IsNull(i)) << "right value null at row " << i;
                EXPECT_EQ(10 + i * 10, lVal->GetValue(i)) << "left value mismatch at row " << i;
            }
            if (rVal->GetValue(0) == 100) {
                foundRight100 = true;
                for (int i = 0; i < 3; i++) {
                    EXPECT_EQ(100, rVal->GetValue(i)) << "right value mismatch at row " << i;
                }
            } else if (rVal->GetValue(0) == 200) {
                foundRight200 = true;
                for (int i = 0; i < 3; i++) {
                    EXPECT_EQ(200, rVal->GetValue(i)) << "right value mismatch at row " << i;
                }
            } else {
                FAIL() << "unexpected right value in 3-row batch";
            }
        }
        EXPECT_TRUE(foundRight100);
        EXPECT_TRUE(foundRight200);
        destroyRightOuterJoinTestContext(ctx);
    }

    delete bPad;
}

TEST(WindowJoinOperatorTest, RightOuterJoinWindowBehaviorTest)
{
    // Left: (key=0, windowEnd=1000, value=12) matches right (0,1000,100); (key=1,
    // windowEnd=2000, value=24) is a left-only window. Right: (2, windowEnd=3000, value=300)
    // is a right-only window. Watermark is advanced stepwise to fire each window in turn, and
    // late rows for already-fired windows are injected between steps to verify they are dropped.
    auto ctx = createRightOuterJoinTestContext(parsedJsonEqui);
    ctx.op->processBatch1(new StreamRecord(makeInt3Batch({0, 1}, {1000, 2000}, {12, 24})));
    ctx.op->processBatch2(new StreamRecord(makeInt3Batch({0, 2}, {1000, 3000}, {100, 300})));

    auto b1 = buildRightOuterExpectedBatch({RightOuterJoinRowSpec{0, 1000, 12, 0, 1000, 100, false}});
    auto b2 = buildRightOuterExpectedBatch({RightOuterJoinRowSpec{0, 0, 0, 2, 3000, 300, true}});

    // Watermark 1500 fires only window 1000 (matched pair); windows 2000/3000 stay pending.
    ctx.op->getInternalTimerService()->advanceWatermark(1500);
    EXPECT_TRUE(inOutput(b1, ctx.out));
    EXPECT_TRUE(!inOutput(b2, ctx.out));
    ASSERT_EQ(static_cast<size_t>(1), ctx.out->getAll().size());

    // Late rows for window 1000 are dropped.
    ctx.op->processBatch1(new StreamRecord(makeInt3Batch({0}, {1000}, {12})));
    ctx.op->processBatch2(new StreamRecord(makeInt3Batch({0}, {1000}, {100})));
    ASSERT_EQ(static_cast<size_t>(1), ctx.out->getAll().size());

    // Watermark 2500 fires window 2000 (left-only) -> RightOuterJoin emits nothing.
    ctx.op->getInternalTimerService()->advanceWatermark(2500);
    ASSERT_EQ(static_cast<size_t>(1), ctx.out->getAll().size());

    // Late rows for window 2000 are dropped.
    ctx.op->processBatch1(new StreamRecord(makeInt3Batch({1}, {2000}, {24})));
    ctx.op->processBatch2(new StreamRecord(makeInt3Batch({1}, {2000}, {200})));
    ASSERT_EQ(static_cast<size_t>(1), ctx.out->getAll().size());

    // Watermark 3500 fires window 3000 (right-only) -> left columns null-padded.
    ctx.op->getInternalTimerService()->advanceWatermark(3500);
    EXPECT_TRUE(inOutput(b2, ctx.out));
    ASSERT_EQ(static_cast<size_t>(2), ctx.out->getAll().size());

    destroyRightOuterJoinTestContext(ctx);
    delete b1;
    delete b2;
}

TEST(WindowJoinOperatorTest, RightOuterJoinNullKeyTest)
{
    // Null join keys must not produce any matched pair (Flink JoinConditionWithNullFilters
    // semantics): even though both sides have records in the same key group, all right rows
    // are emitted with the left columns null-padded.
    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetNull(0);
    vbatchLeft->Append(vKeyLeft);
    auto vWindLeft = new omniruntime::vec::Vector<int64_t>(1);
    vWindLeft->SetValue(0, 1000);
    vbatchLeft->Append(vWindLeft);
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(1);
    vValLeft->SetValue(0, 1);
    vbatchLeft->Append(vValLeft);

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetNull(0);
    vbatchRight->Append(vKeyRight);
    auto vWindRight = new omniruntime::vec::Vector<int64_t>(1);
    vWindRight->SetValue(0, 1000);
    vbatchRight->Append(vWindRight);
    auto vValRight = new omniruntime::vec::Vector<int32_t>(1);
    vValRight->SetValue(0, 2);
    vbatchRight->Append(vValRight);

    auto ctx = createRightOuterJoinTestContext(parsedJsonEqui);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(1), batches.size());
    auto batch = batches[0];
    ASSERT_EQ(1, batch->GetRowCount());
    auto* lKey = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(0));
    auto* lWind = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(1));
    auto* lVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(2));
    auto* rKey = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(3));
    auto* rVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(5));
    EXPECT_TRUE(lKey->IsNull(0));
    EXPECT_TRUE(lWind->IsNull(0));
    EXPECT_TRUE(lVal->IsNull(0));
    EXPECT_TRUE(rKey->IsNull(0));
    EXPECT_FALSE(rVal->IsNull(0));
    EXPECT_EQ(2, rVal->GetValue(0));

    destroyRightOuterJoinTestContext(ctx);
}

// Builds a 2-row INT / BIGINT / INT batch for the mixed null-key tests: the row at
// `nullRowIdx` carries a NULL join key (raw value 0) and the other row carries a non-null
// key 0. Both rows share the same window end. The NULL-key row always carries `nullKeyValue`
// and the key-0 row always carries `okKeyValue`, so the expected output is independent of
// which row index holds the NULL key.
omnistream::VectorBatch* makeMixedNullKeyInt3Batch(
    int nullRowIdx, int64_t windowEnd, int32_t nullKeyValue, int32_t okKeyValue)
{
    auto batch = new omnistream::VectorBatch(2);
    auto vKey = new omniruntime::vec::Vector<int32_t>(2);
    auto vWindowEnd = new omniruntime::vec::Vector<int64_t>(2);
    auto vValue = new omniruntime::vec::Vector<int32_t>(2);
    for (int i = 0; i < 2; i++) {
        bool isNullRow = (i == nullRowIdx);
        vKey->SetValue(i, 0);
        if (isNullRow) {
            vKey->SetNull(i);
        }
        vWindowEnd->SetValue(i, windowEnd);
        vValue->SetValue(i, isNullRow ? nullKeyValue : okKeyValue);
    }
    batch->Append(vKey);
    batch->Append(vWindowEnd);
    batch->Append(vValue);
    return batch;
}

// Asserts one row of a mixed-null-key join result. For each side, `pad` means every column of
// that side is NULL (the row of the other side was null-padded); otherwise the side carries the
// given value with the key matching `keyNull` and window end 1000.
void checkMixedNullKeyRow(
    omnistream::VectorBatch* batch, int row, bool leftPad, bool leftKeyNull, int32_t leftValue,
    bool rightPad, bool rightKeyNull, int32_t rightValue)
{
    auto* lKey = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(0));
    auto* lWind = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(1));
    auto* lVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(2));
    auto* rKey = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(3));
    auto* rWind = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(4));
    auto* rVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(5));
    if (leftPad) {
        EXPECT_TRUE(lKey->IsNull(row));
        EXPECT_TRUE(lWind->IsNull(row));
        EXPECT_TRUE(lVal->IsNull(row));
    } else {
        EXPECT_EQ(leftKeyNull, lKey->IsNull(row));
        EXPECT_FALSE(lWind->IsNull(row));
        EXPECT_EQ(1000, lWind->GetValue(row));
        EXPECT_FALSE(lVal->IsNull(row));
        EXPECT_EQ(leftValue, lVal->GetValue(row));
    }
    if (rightPad) {
        EXPECT_TRUE(rKey->IsNull(row));
        EXPECT_TRUE(rWind->IsNull(row));
        EXPECT_TRUE(rVal->IsNull(row));
    } else {
        EXPECT_EQ(rightKeyNull, rKey->IsNull(row));
        EXPECT_FALSE(rWind->IsNull(row));
        EXPECT_EQ(1000, rWind->GetValue(row));
        EXPECT_FALSE(rVal->IsNull(row));
        EXPECT_EQ(rightValue, rVal->GetValue(row));
    }
}

TEST(WindowJoinOperatorTest, RightOuterJoinMixedNullKeyTest)
{
    // A NULL join key and a non-null key 0 collapse into the same key group when the key is a
    // scalar (int32_t): both produce raw key 0. Flink keeps them in separate groups, so the
    // output must be the union of the null-key group (right rows left-null-padded) and the
    // key-0 group (matched pairs), with no spurious NULL<->non-null matches. This must hold
    // regardless of the input order of the NULL-key row.
    for (int nullRowIdx = 0; nullRowIdx < 2; nullRowIdx++) {
        auto ctx = createRightOuterJoinTestContext(parsedJsonEqui);
        // Left: [NULL-key value 10, key-0 value 20]; Right: [NULL-key value 100, key-0 value 200].
        ctx.op->processBatch1(new StreamRecord(makeMixedNullKeyInt3Batch(nullRowIdx, 1000, 10, 20)));
        ctx.op->processBatch2(new StreamRecord(makeMixedNullKeyInt3Batch(nullRowIdx, 1000, 100, 200)));
        ctx.op->getInternalTimerService()->advanceWatermark(100000);

        auto& batches = ctx.out->getAll();
        ASSERT_EQ(static_cast<size_t>(2), batches.size());
        bool foundPad = false;
        bool foundMatch = false;
        for (auto b : batches) {
            ASSERT_EQ(1, b->GetRowCount());
            auto* rVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(b->Get(5));
            if (rVal->GetValue(0) == 100) {
                ASSERT_FALSE(foundPad) << "duplicate null-padded row in iteration " << nullRowIdx;
                foundPad = true;
                checkMixedNullKeyRow(b, 0, true, false, 0, false, true, 100);
            } else {
                ASSERT_FALSE(foundMatch) << "duplicate matched row in iteration " << nullRowIdx;
                foundMatch = true;
                checkMixedNullKeyRow(b, 0, false, false, 20, false, false, 200);
            }
        }
        EXPECT_TRUE(foundPad) << "missing null-padded row in iteration " << nullRowIdx;
        EXPECT_TRUE(foundMatch) << "missing matched row in iteration " << nullRowIdx;

        destroyRightOuterJoinTestContext(ctx);
    }
}

TEST(WindowJoinOperatorTest, LeftOuterJoinMixedNullKeyTest)
{
    // Mirror of RightOuterJoinMixedNullKeyTest for the left outer join: the null-key group
    // emits its left rows with right columns null-padded and the key-0 group emits the matched
    // pair, independent of the input order of the NULL-key row.
    for (int nullRowIdx = 0; nullRowIdx < 2; nullRowIdx++) {
        auto ctx = createLeftOuterJoinTestContext(parsedJsonEqui);
        ctx.op->processBatch1(new StreamRecord(makeMixedNullKeyInt3Batch(nullRowIdx, 1000, 10, 20)));
        ctx.op->processBatch2(new StreamRecord(makeMixedNullKeyInt3Batch(nullRowIdx, 1000, 100, 200)));
        ctx.op->getInternalTimerService()->advanceWatermark(100000);

        auto& batches = ctx.out->getAll();
        ASSERT_EQ(static_cast<size_t>(2), batches.size());
        bool foundPad = false;
        bool foundMatch = false;
        for (auto b : batches) {
            ASSERT_EQ(1, b->GetRowCount());
            auto* lVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(b->Get(2));
            if (lVal->GetValue(0) == 10) {
                ASSERT_FALSE(foundPad) << "duplicate null-padded row in iteration " << nullRowIdx;
                foundPad = true;
                checkMixedNullKeyRow(b, 0, false, true, 10, true, false, 0);
            } else {
                ASSERT_FALSE(foundMatch) << "duplicate matched row in iteration " << nullRowIdx;
                foundMatch = true;
                checkMixedNullKeyRow(b, 0, false, false, 20, false, false, 200);
            }
        }
        EXPECT_TRUE(foundPad) << "missing null-padded row in iteration " << nullRowIdx;
        EXPECT_TRUE(foundMatch) << "missing matched row in iteration " << nullRowIdx;

        destroyLeftOuterJoinTestContext(ctx);
    }
}

TEST(WindowJoinOperatorTest, RightOuterJoinNonEquiOrderTest)
{
    // Single key group (key=0) with a non-equi condition: left.value < right.value.
    // RightOuterJoin emits per right record in input order: a right record's matched
    // pairs are emitted as one batch (left rows in input order), and a right record
    // that matches nothing is emitted as a single left-null-padded row.
    auto vbatchLeft = makeInt3Batch({0, 0, 0}, {1000, 1000, 1000}, {10, 30, 20});
    auto vbatchRight = makeInt3Batch({0, 0}, {1000, 1000}, {25, 5});

    auto ctx = createRightOuterJoinTestContext(parsedJsonRightOuterNonEqui);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Left rows in order: value 10, 30, 20. Right rows: value 25, 5.
    Condition: left.value < right.value.

    RightOuterJoin emits per right record:
      right 25 -> matched pairs (10,25),(20,25)
      right 5  -> no match -> null-padded (null, 5)

    Expected output batches (in emission order):
      batch0: 2 rows: (10,25),(20,25)
      batch1: 1 row : (null, 5)
    */
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(2), batches.size());

    auto checkRow = [](omnistream::VectorBatch* batch, int row, int32_t leftVal, int32_t rightVal, bool leftNull) {
        auto* l = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(2));
        auto* r = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(5));
        if (leftNull) {
            EXPECT_TRUE(l->IsNull(row)) << "left value should be null at batch row " << row;
        } else {
            EXPECT_FALSE(l->IsNull(row)) << "left value should not be null at batch row " << row;
            EXPECT_EQ(leftVal, l->GetValue(row)) << "left value mismatch at batch row " << row;
        }
        EXPECT_FALSE(r->IsNull(row)) << "right value should not be null at batch row " << row;
        EXPECT_EQ(rightVal, r->GetValue(row)) << "right value mismatch at batch row " << row;
    };

    auto b0 = batches[0];
    ASSERT_EQ(2, b0->GetRowCount());
    checkRow(b0, 0, 10, 25, false);
    checkRow(b0, 1, 20, 25, false);

    auto b1 = batches[1];
    ASSERT_EQ(1, b1->GetRowCount());
    checkRow(b1, 0, 0, 5, true);

    destroyRightOuterJoinTestContext(ctx);
}

TEST(WindowJoinOperatorTest, RightOuterJoinVarcharTest)
{
    // Left: 3 rows in the same join key group (key=1) and same window (windowEnd=1000).
    // RightOuterJoin emits the equi cartesian product in right-major order, so the
    // right-side VARCHAR values repeat per left row and the left-side VARCHAR values
    // cycle within each right record's block.
    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto vbatchLeft = new omnistream::VectorBatch(3);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(3);
    vKeyLeft->SetValue(0, 1);
    vKeyLeft->SetValue(1, 1);
    vKeyLeft->SetValue(2, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vWindowEndLeft = new omniruntime::vec::Vector<int64_t>(3);
    vWindowEndLeft->SetValue(0, 1000);
    vWindowEndLeft->SetValue(1, 1000);
    vWindowEndLeft->SetValue(2, 1000);
    vbatchLeft->Append(vWindowEndLeft);
    auto vNameLeft = new VarcharVector(3);
    vNameLeft->SetValue(0, std::string_view("a"));
    vNameLeft->SetValue(1, std::string_view("b"));
    vNameLeft->SetValue(2, std::string_view("c"));
    vbatchLeft->Append(vNameLeft);

    // Right: 2 rows in the same key group and window.
    auto vbatchRight = new omnistream::VectorBatch(2);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(2);
    vKeyRight->SetValue(0, 1);
    vKeyRight->SetValue(1, 1);
    vbatchRight->Append(vKeyRight);
    auto vWindowEndRight = new omniruntime::vec::Vector<int64_t>(2);
    vWindowEndRight->SetValue(0, 1000);
    vWindowEndRight->SetValue(1, 1000);
    vbatchRight->Append(vWindowEndRight);
    auto vNameRight = new VarcharVector(2);
    vNameRight->SetValue(0, std::string_view("x"));
    vNameRight->SetValue(1, std::string_view("y"));
    vbatchRight->Append(vNameRight);

    auto ctx = createRightOuterJoinTestContext(parsedJsonRightOuterVarchar);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    // 3 left x 2 right = 6 rows in one output batch, right-major:
    //   right x: L_name a,b,c ; right y: L_name a,b,c
    // L_name: a,b,c,a,b,c ; R_name: x,x,x,y,y,y
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(1), batches.size());
    auto batch = batches[0];
    ASSERT_EQ(6, batch->GetRowCount());

    auto* lName = reinterpret_cast<VarcharVector*>(batch->Get(2));
    auto* rName = reinterpret_cast<VarcharVector*>(batch->Get(5));
    const std::vector<std::string> expectedLName = {"a", "b", "c", "a", "b", "c"};
    const std::vector<std::string> expectedRName = {"x", "x", "x", "y", "y", "y"};
    for (int i = 0; i < 6; i++) {
        EXPECT_FALSE(lName->IsNull(i)) << "L_name null at row " << i;
        EXPECT_FALSE(rName->IsNull(i)) << "R_name null at row " << i;
        EXPECT_EQ(expectedLName[i], std::string(lName->GetValue(i))) << "L_name mismatch at row " << i;
        EXPECT_EQ(expectedRName[i], std::string(rName->GetValue(i))) << "R_name mismatch at row " << i;
    }

    destroyRightOuterJoinTestContext(ctx);
}

// Full outer join with a residual non-equi predicate on the value column
// (left value < right value). Records are grouped by the single join key [0].
std::string testDescriptionFullOuterNonEqui = R"delimiter({
  "originDescription": null,
  "leftInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "rightInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "outputTypes": [
    "INT",
    "BIGINT",
    "INT",
    "INT",
    "BIGINT",
    "INT"
  ],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": {"exprType":"BINARY","returnType":4,"operator":"LESS_THAN","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":2},"right":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":5}},
  "joinType": "FullOuterJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

nlohmann::json parsedJsonFullOuterNonEqui = nlohmann::json::parse(testDescriptionFullOuterNonEqui);

std::string testDescriptionFullOuterVarchar = R"delimiter({
  "originDescription": null,
  "leftInputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "rightInputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "outputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)",
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": null,
  "joinType": "FullOuterJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

nlohmann::json parsedJsonFullOuterVarchar = nlohmann::json::parse(testDescriptionFullOuterVarchar);

// One expected output row for the FullOuterJoin tests. When rightNull is true the three
// right-side columns are emitted as null (null-padded unmatched left row); when leftNull is
// true the three left-side columns are emitted as null (null-padded unmatched right row).
struct FullOuterJoinRowSpec {
    int32_t leftKey;
    int64_t leftWindowEnd;
    int32_t leftValue;
    int32_t rightKey;
    int64_t rightWindowEnd;
    int32_t rightValue;
    bool leftNull;
    bool rightNull;
};

omnistream::VectorBatch* buildFullOuterExpectedBatch(const std::vector<FullOuterJoinRowSpec>& specs)
{
    auto numRows = static_cast<int>(specs.size());
    auto batch = new omnistream::VectorBatch(numRows);
    auto v0 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v1 = new omniruntime::vec::Vector<int64_t>(numRows);
    auto v2 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v3 = new omniruntime::vec::Vector<int32_t>(numRows);
    auto v4 = new omniruntime::vec::Vector<int64_t>(numRows);
    auto v5 = new omniruntime::vec::Vector<int32_t>(numRows);
    int row = 0;
    for (const auto& spec : specs) {
        if (spec.leftNull) {
            v0->SetNull(row);
            v1->SetNull(row);
            v2->SetNull(row);
        } else {
            v0->SetValue(row, spec.leftKey);
            v1->SetValue(row, spec.leftWindowEnd);
            v2->SetValue(row, spec.leftValue);
        }
        if (spec.rightNull) {
            v3->SetNull(row);
            v4->SetNull(row);
            v5->SetNull(row);
        } else {
            v3->SetValue(row, spec.rightKey);
            v4->SetValue(row, spec.rightWindowEnd);
            v5->SetValue(row, spec.rightValue);
        }
        row++;
    }
    batch->Append(v0);
    batch->Append(v1);
    batch->Append(v2);
    batch->Append(v3);
    batch->Append(v4);
    batch->Append(v5);
    return batch;
}

struct FullOuterJoinTestContext {
    FullOuterJoinOperator<int32_t>* op = nullptr;
    OutputTestVectorBatch* out = nullptr;
    StreamTaskStateInitializerImpl* initializer = nullptr;
    omnistream::RuntimeEnvironmentV2* env2 = nullptr;
    TaskInformationPOD* taskInfo = nullptr;
};

// Creates a ready-to-run FullOuterJoinOperator using the same HashMapStateBackend setup
// shared by the other window join tests.
FullOuterJoinTestContext createFullOuterJoinTestContext(const nlohmann::json& desc)
{
    FullOuterJoinTestContext ctx;
    ctx.out = new OutputTestVectorBatch();
    ctx.env2 = new omnistream::RuntimeEnvironmentV2();
    ctx.taskInfo = new TaskInformationPOD();
    ctx.taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = ctx.taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        ctx.taskInfo->setStreamConfigPOD(configPOD);
    }
    ctx.env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    ctx.env2->setTaskConfiguration(*ctx.taskInfo);
    ctx.initializer = new StreamTaskStateInitializerImpl(ctx.env2);
    ctx.op = new FullOuterJoinOperator<int32_t>(desc, ctx.out, new LongSerializer(), new LongSerializer());
    ctx.op->setup();
    ctx.op->initializeState(ctx.initializer, new IntSerializer());
    ctx.op->open();
    return ctx;
}

void destroyFullOuterJoinTestContext(FullOuterJoinTestContext& ctx)
{
    if (ctx.op != nullptr) {
        ctx.op->close();
        delete ctx.op;
    }
    delete ctx.initializer;
    delete ctx.out;
    // env2/taskInfo are intentionally not freed, matching the existing tests in this file.
}

TEST(WindowJoinOperatorTest, FullOuterJoinTest)
{
    // Left: keys {0,1,2} window 1000 values {12,24,36}; Right: keys {0,1,1,3} window 1000
    // values {100,200,300,400}. The key-0 group matches (0,1000,12) with (0,1000,100), the
    // key-1 group matches (1,1000,24) with both (1,1000,200/300), the key-2 group is left-only
    // and the key-3 group is right-only, so both are null-padded on the missing side. The same
    // expected output holds for both the equi description and the non-equi (left.value <
    // right.value) description, so the two join paths are verified together.
    auto b1 = buildFullOuterExpectedBatch({FullOuterJoinRowSpec{0, 1000, 12, 0, 1000, 100, false, false}});
    auto b2 = buildFullOuterExpectedBatch(
        {FullOuterJoinRowSpec{1, 1000, 24, 1, 1000, 200, false, false},
         FullOuterJoinRowSpec{1, 1000, 24, 1, 1000, 300, false, false}});
    auto b3 = buildFullOuterExpectedBatch({FullOuterJoinRowSpec{2, 1000, 36, 0, 0, 0, false, true}});
    auto b4 = buildFullOuterExpectedBatch({FullOuterJoinRowSpec{0, 0, 0, 3, 1000, 400, true, false}});

    for (const auto& desc : {parsedJsonEqui, parsedJsonFullOuterNonEqui}) {
        auto ctx = createFullOuterJoinTestContext(desc);
        ctx.op->processBatch1(new StreamRecord(getLeftBatch()));
        ctx.op->processBatch2(new StreamRecord(getRightBatch()));
        ctx.op->getInternalTimerService()->advanceWatermark(100000);

        EXPECT_TRUE(inOutput(b1, ctx.out));
        EXPECT_TRUE(inOutput(b2, ctx.out));
        EXPECT_TRUE(inOutput(b3, ctx.out));
        EXPECT_TRUE(inOutput(b4, ctx.out));

        destroyFullOuterJoinTestContext(ctx);
    }

    delete b1;
    delete b2;
    delete b3;
    delete b4;
}

TEST(WindowJoinOperatorTest, FullOuterJoinWindowBehaviorTest)
{
    // Left: (key=0, windowEnd=1000, value=12) matches right (0,1000,100); (key=1,
    // windowEnd=2000, value=24) is a left-only window -> right columns null-padded. Right:
    // (key=2, windowEnd=3000, value=300) is a right-only window -> left columns null-padded.
    // Watermark is advanced stepwise to fire each window in turn, and late rows for
    // already-fired windows are injected between steps to verify they are dropped.
    auto ctx = createFullOuterJoinTestContext(parsedJsonEqui);
    ctx.op->processBatch1(new StreamRecord(makeInt3Batch({0, 1}, {1000, 2000}, {12, 24})));
    ctx.op->processBatch2(new StreamRecord(makeInt3Batch({0, 2}, {1000, 3000}, {100, 300})));

    auto b1 = buildFullOuterExpectedBatch({FullOuterJoinRowSpec{0, 1000, 12, 0, 1000, 100, false, false}});
    auto b2 = buildFullOuterExpectedBatch({FullOuterJoinRowSpec{1, 2000, 24, 0, 0, 0, false, true}});
    auto b3 = buildFullOuterExpectedBatch({FullOuterJoinRowSpec{0, 0, 0, 2, 3000, 300, true, false}});

    // Watermark 1500 fires only window 1000 (matched pair); windows 2000/3000 stay pending.
    ctx.op->getInternalTimerService()->advanceWatermark(1500);
    EXPECT_TRUE(inOutput(b1, ctx.out));
    EXPECT_TRUE(!inOutput(b2, ctx.out));
    ASSERT_EQ(static_cast<size_t>(1), ctx.out->getAll().size());

    // Late rows for window 1000 are dropped.
    ctx.op->processBatch1(new StreamRecord(makeInt3Batch({0}, {1000}, {12})));
    ctx.op->processBatch2(new StreamRecord(makeInt3Batch({0}, {1000}, {100})));
    ASSERT_EQ(static_cast<size_t>(1), ctx.out->getAll().size());

    // Watermark 2500 fires window 2000 (left-only) -> right columns null-padded.
    ctx.op->getInternalTimerService()->advanceWatermark(2500);
    EXPECT_TRUE(inOutput(b2, ctx.out));
    ASSERT_EQ(static_cast<size_t>(2), ctx.out->getAll().size());

    // Late rows for window 2000 are dropped.
    ctx.op->processBatch1(new StreamRecord(makeInt3Batch({1}, {2000}, {24})));
    ctx.op->processBatch2(new StreamRecord(makeInt3Batch({1}, {2000}, {200})));
    ASSERT_EQ(static_cast<size_t>(2), ctx.out->getAll().size());

    // Watermark 3500 fires window 3000 (right-only) -> left columns null-padded.
    ctx.op->getInternalTimerService()->advanceWatermark(3500);
    EXPECT_TRUE(inOutput(b3, ctx.out));
    ASSERT_EQ(static_cast<size_t>(3), ctx.out->getAll().size());

    destroyFullOuterJoinTestContext(ctx);
    delete b1;
    delete b2;
    delete b3;
}

TEST(WindowJoinOperatorTest, FullOuterJoinNullKeyTest)
{
    // Scenario 1: the key group holds only NULL-key rows on both sides. No pair matches (Flink
    // JoinConditionWithNullFilters semantics), so the left null-key row is emitted with the
    // right columns null-padded and the right null-key row with the left columns null-padded.
    {
        auto vbatchLeft = new omnistream::VectorBatch(1);
        auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
        vKeyLeft->SetValue(0, 0);
        vKeyLeft->SetNull(0);
        vbatchLeft->Append(vKeyLeft);
        auto vWindLeft = new omniruntime::vec::Vector<int64_t>(1);
        vWindLeft->SetValue(0, 1000);
        vbatchLeft->Append(vWindLeft);
        auto vValLeft = new omniruntime::vec::Vector<int32_t>(1);
        vValLeft->SetValue(0, 1);
        vbatchLeft->Append(vValLeft);

        auto vbatchRight = new omnistream::VectorBatch(1);
        auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
        vKeyRight->SetValue(0, 0);
        vKeyRight->SetNull(0);
        vbatchRight->Append(vKeyRight);
        auto vWindRight = new omniruntime::vec::Vector<int64_t>(1);
        vWindRight->SetValue(0, 1000);
        vbatchRight->Append(vWindRight);
        auto vValRight = new omniruntime::vec::Vector<int32_t>(1);
        vValRight->SetValue(0, 2);
        vbatchRight->Append(vValRight);

        auto ctx = createFullOuterJoinTestContext(parsedJsonEqui);
        ctx.op->processBatch1(new StreamRecord(vbatchLeft));
        ctx.op->processBatch2(new StreamRecord(vbatchRight));
        ctx.op->getInternalTimerService()->advanceWatermark(100000);

        auto& batches = ctx.out->getAll();
        ASSERT_EQ(static_cast<size_t>(2), batches.size());
        bool foundLeftPad = false;
        bool foundRightPad = false;
        for (auto batch : batches) {
            ASSERT_EQ(1, batch->GetRowCount());
            auto* rVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(5));
            if (rVal->IsNull(0)) {
                ASSERT_FALSE(foundLeftPad) << "duplicate left null-key padded row";
                foundLeftPad = true;
                checkMixedNullKeyRow(batch, 0, false, true, 1, true, false, 0);
            } else {
                ASSERT_FALSE(foundRightPad) << "duplicate right null-key padded row";
                foundRightPad = true;
                checkMixedNullKeyRow(batch, 0, true, false, 0, false, true, 2);
            }
        }
        EXPECT_TRUE(foundLeftPad);
        EXPECT_TRUE(foundRightPad);

        destroyFullOuterJoinTestContext(ctx);
    }

    // Scenario 2: a NULL join key and a non-null key 0 collapse into the same key group when the
    // key is a scalar (int32_t): both produce raw key 0. Flink keeps them in separate key groups,
    // so the correct output is the union of the null-key group and the key-0 group: the left
    // null-key row is right-null-padded, the key-0 pair matches, and the right null-key row is
    // left-null-padded. This must hold regardless of the input order of the NULL-key row.
    for (int nullRowIdx = 0; nullRowIdx < 2; nullRowIdx++) {
        auto ctx = createFullOuterJoinTestContext(parsedJsonEqui);
        // Left: [NULL-key value 10, key-0 value 20]; Right: [NULL-key value 100, key-0 value 200].
        ctx.op->processBatch1(new StreamRecord(makeMixedNullKeyInt3Batch(nullRowIdx, 1000, 10, 20)));
        ctx.op->processBatch2(new StreamRecord(makeMixedNullKeyInt3Batch(nullRowIdx, 1000, 100, 200)));
        ctx.op->getInternalTimerService()->advanceWatermark(100000);

        auto& batches = ctx.out->getAll();
        ASSERT_EQ(static_cast<size_t>(3), batches.size());
        bool foundLeftPad = false;
        bool foundMatch = false;
        bool foundRightPad = false;
        for (auto b : batches) {
            ASSERT_EQ(1, b->GetRowCount());
            auto* lVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(b->Get(2));
            auto* rVal = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(b->Get(5));
            if (lVal->IsNull(0)) {
                ASSERT_FALSE(foundRightPad) << "duplicate right null-key padded row in iteration " << nullRowIdx;
                foundRightPad = true;
                checkMixedNullKeyRow(b, 0, true, false, 0, false, true, 100);
            } else if (rVal->IsNull(0)) {
                ASSERT_FALSE(foundLeftPad) << "duplicate left null-key padded row in iteration " << nullRowIdx;
                foundLeftPad = true;
                checkMixedNullKeyRow(b, 0, false, true, 10, true, false, 0);
            } else {
                ASSERT_FALSE(foundMatch) << "duplicate matched row in iteration " << nullRowIdx;
                foundMatch = true;
                checkMixedNullKeyRow(b, 0, false, false, 20, false, false, 200);
            }
        }
        EXPECT_TRUE(foundLeftPad) << "missing left null-key padded row in iteration " << nullRowIdx;
        EXPECT_TRUE(foundMatch) << "missing matched row in iteration " << nullRowIdx;
        EXPECT_TRUE(foundRightPad) << "missing right null-key padded row in iteration " << nullRowIdx;

        destroyFullOuterJoinTestContext(ctx);
    }
}

TEST(WindowJoinOperatorTest, FullOuterJoinNonEquiOrderTest)
{
    // Single key group (key=0) with a non-equi condition: left.value < right.value.
    // FullOuterJoin emits per left record in input order (a left record's matched pairs as one
    // batch, a matching-nothing left record as a right-null-padded row), then emits every
    // right record that never matched as a left-null-padded row.
    auto vbatchLeft = makeInt3Batch({0, 0, 0}, {1000, 1000, 1000}, {10, 30, 20});
    auto vbatchRight = makeInt3Batch({0, 0}, {1000, 1000}, {25, 5});

    auto ctx = createFullOuterJoinTestContext(parsedJsonFullOuterNonEqui);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Left rows in order: value 10, 30, 20. Right rows: value 25, 5.
    Condition: left.value < right.value.

      left 10 -> matches right 25 -> (10,25)
      left 30 -> no match -> right-null-padded (30, null)
      left 20 -> matches right 25 -> (20,25)
      right 5  -> never matched -> left-null-padded (null, 5)

    Expected output batches (in emission order):
      batch0: 1 row : (10,25)
      batch1: 1 row : (30, null)
      batch2: 1 row : (20,25)
      batch3: 1 row : (null, 5)
    */
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(4), batches.size());

    auto checkRow = [](omnistream::VectorBatch* batch, int row, int32_t leftVal, int32_t rightVal, bool leftNull,
                       bool rightNull) {
        auto* l = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(2));
        auto* r = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(5));
        if (leftNull) {
            EXPECT_TRUE(l->IsNull(row)) << "left value should be null at batch row " << row;
        } else {
            EXPECT_FALSE(l->IsNull(row)) << "left value should not be null at batch row " << row;
            EXPECT_EQ(leftVal, l->GetValue(row)) << "left value mismatch at batch row " << row;
        }
        if (rightNull) {
            EXPECT_TRUE(r->IsNull(row)) << "right value should be null at batch row " << row;
        } else {
            EXPECT_FALSE(r->IsNull(row)) << "right value should not be null at batch row " << row;
            EXPECT_EQ(rightVal, r->GetValue(row)) << "right value mismatch at batch row " << row;
        }
    };

    checkRow(batches[0], 0, 10, 25, false, false);
    checkRow(batches[1], 0, 30, 0, false, true);
    checkRow(batches[2], 0, 20, 25, false, false);
    checkRow(batches[3], 0, 0, 5, true, false);

    destroyFullOuterJoinTestContext(ctx);
}

TEST(WindowJoinOperatorTest, FullOuterJoinVarcharTest)
{
    // Left: 3 rows in the same join key group (key=1) and same window (windowEnd=1000), plus a
    // left-only row (key=2). Right: 2 rows in the key=1 group and same window, plus a right-only
    // row (key=3). The key=1 group emits the equi cartesian product in left-major order; the
    // left-only/right-only rows are null-padded on the missing side (VARCHAR nulls included).
    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto vbatchLeft = new omnistream::VectorBatch(4);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(4);
    vKeyLeft->SetValue(0, 1);
    vKeyLeft->SetValue(1, 1);
    vKeyLeft->SetValue(2, 1);
    vKeyLeft->SetValue(3, 2);
    vbatchLeft->Append(vKeyLeft);
    auto vWindowEndLeft = new omniruntime::vec::Vector<int64_t>(4);
    vWindowEndLeft->SetValue(0, 1000);
    vWindowEndLeft->SetValue(1, 1000);
    vWindowEndLeft->SetValue(2, 1000);
    vWindowEndLeft->SetValue(3, 1000);
    vbatchLeft->Append(vWindowEndLeft);
    auto vNameLeft = new VarcharVector(4);
    vNameLeft->SetValue(0, std::string_view("a"));
    vNameLeft->SetValue(1, std::string_view("b"));
    vNameLeft->SetValue(2, std::string_view("c"));
    vNameLeft->SetValue(3, std::string_view("lone"));
    vbatchLeft->Append(vNameLeft);

    auto vbatchRight = new omnistream::VectorBatch(3);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(3);
    vKeyRight->SetValue(0, 1);
    vKeyRight->SetValue(1, 1);
    vKeyRight->SetValue(2, 3);
    vbatchRight->Append(vKeyRight);
    auto vWindowEndRight = new omniruntime::vec::Vector<int64_t>(3);
    vWindowEndRight->SetValue(0, 1000);
    vWindowEndRight->SetValue(1, 1000);
    vWindowEndRight->SetValue(2, 1000);
    vbatchRight->Append(vWindowEndRight);
    auto vNameRight = new VarcharVector(3);
    vNameRight->SetValue(0, std::string_view("x"));
    vNameRight->SetValue(1, std::string_view("y"));
    vNameRight->SetValue(2, std::string_view("ronly"));
    vbatchRight->Append(vNameRight);

    auto ctx = createFullOuterJoinTestContext(parsedJsonFullOuterVarchar);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    // Key=1 group: 3 left x 2 right = 6 rows in one output batch, left-major:
    //   L_name: a,a,b,b,c,c ; R_name: x,y,x,y,x,y
    // Key=2 left-only: 1 row with right VARCHAR columns null.
    // Key=3 right-only: 1 row with left VARCHAR columns null.
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(3), batches.size());

    omnistream::VectorBatch* cartesian = nullptr;
    omnistream::VectorBatch* leftPad = nullptr;
    omnistream::VectorBatch* rightPad = nullptr;
    for (auto b : batches) {
        if (b->GetRowCount() == 6) {
            cartesian = b;
        } else {
            auto* lName = reinterpret_cast<VarcharVector*>(b->Get(2));
            if (lName->IsNull(0)) {
                rightPad = b;
            } else {
                leftPad = b;
            }
        }
    }
    ASSERT_NE(cartesian, nullptr);
    ASSERT_NE(leftPad, nullptr);
    ASSERT_NE(rightPad, nullptr);

    auto* lName = reinterpret_cast<VarcharVector*>(cartesian->Get(2));
    auto* rName = reinterpret_cast<VarcharVector*>(cartesian->Get(5));
    const std::vector<std::string> expectedLName = {"a", "a", "b", "b", "c", "c"};
    const std::vector<std::string> expectedRName = {"x", "y", "x", "y", "x", "y"};
    for (int i = 0; i < 6; i++) {
        EXPECT_FALSE(lName->IsNull(i)) << "L_name null at row " << i;
        EXPECT_FALSE(rName->IsNull(i)) << "R_name null at row " << i;
        EXPECT_EQ(expectedLName[i], std::string(lName->GetValue(i))) << "L_name mismatch at row " << i;
        EXPECT_EQ(expectedRName[i], std::string(rName->GetValue(i))) << "R_name mismatch at row " << i;
    }

    // Key=2 left-only: left key 2, window 1000, name "lone"; right columns null.
    auto* lKey = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(leftPad->Get(0));
    auto* lWind = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(leftPad->Get(1));
    auto* lNamePad = reinterpret_cast<VarcharVector*>(leftPad->Get(2));
    auto* rKeyPad = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(leftPad->Get(3));
    auto* rWindPad = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(leftPad->Get(4));
    auto* rNamePad = reinterpret_cast<VarcharVector*>(leftPad->Get(5));
    EXPECT_FALSE(lKey->IsNull(0));
    EXPECT_EQ(2, lKey->GetValue(0));
    EXPECT_FALSE(lWind->IsNull(0));
    EXPECT_EQ(1000, lWind->GetValue(0));
    EXPECT_FALSE(lNamePad->IsNull(0));
    EXPECT_EQ("lone", std::string(lNamePad->GetValue(0)));
    EXPECT_TRUE(rKeyPad->IsNull(0));
    EXPECT_TRUE(rWindPad->IsNull(0));
    EXPECT_TRUE(rNamePad->IsNull(0));

    // Key=3 right-only: left columns null; right key 3, window 1000, name "ronly".
    auto* rKey = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(rightPad->Get(3));
    auto* rWind = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(rightPad->Get(4));
    auto* rNameOnly = reinterpret_cast<VarcharVector*>(rightPad->Get(5));
    auto* lKeyPad = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(rightPad->Get(0));
    auto* lWindPad = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(rightPad->Get(1));
    auto* lNamePad2 = reinterpret_cast<VarcharVector*>(rightPad->Get(2));
    EXPECT_TRUE(lKeyPad->IsNull(0));
    EXPECT_TRUE(lWindPad->IsNull(0));
    EXPECT_TRUE(lNamePad2->IsNull(0));
    EXPECT_FALSE(rKey->IsNull(0));
    EXPECT_EQ(3, rKey->GetValue(0));
    EXPECT_FALSE(rWind->IsNull(0));
    EXPECT_EQ(1000, rWind->GetValue(0));
    EXPECT_FALSE(rNameOnly->IsNull(0));
    EXPECT_EQ("ronly", std::string(rNameOnly->GetValue(0)));

    destroyFullOuterJoinTestContext(ctx);
}

TEST(WindowJoinOperatorTest, WindowSeparationTest)
{
    // Batch left
    /*
    (int) , (long)       , (int)
    KeyCol, WindowEndTime, value
    0     , 1000         , 12
    1     , 2000         , 24
    */

    auto vbatchLeft = new omnistream::VectorBatch(2);

    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(2);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 1);
    vbatchLeft->Append(vKeyLeft);

    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(2);
    vWindowEndTimeLeft->SetValue(0, 1000);
    vWindowEndTimeLeft->SetValue(1, 2000);
    vbatchLeft->Append(vWindowEndTimeLeft);

    auto vValLeft = new omniruntime::vec::Vector<int32_t>(2);
    vValLeft->SetValue(0, 12);
    vValLeft->SetValue(1, 24);
    vbatchLeft->Append(vValLeft);

    // Batch right
    /*
    (int) , (long)       , (int)
    KeyCol, WindowEndTime, value
    0     , 1000         , 100
    1     , 2000         , 200
    */

    auto vbatchRight = new omnistream::VectorBatch(2);

    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(2);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 1);
    vbatchRight->Append(vKeyRight);

    auto vWindowEndTimeRight = new omniruntime::vec::Vector<int64_t>(2);
    vWindowEndTimeRight->SetValue(0, 1000);
    vWindowEndTimeRight->SetValue(1, 2000);
    vbatchRight->Append(vWindowEndTimeRight);

    auto vValRight = new omniruntime::vec::Vector<int32_t>(2);
    vValRight->SetValue(0, 100);
    vValRight->SetValue(1, 200);
    vbatchRight->Append(vValRight);

    auto* out = new OutputTestVectorBatch();
    auto op = new InnerJoinOperator<int32_t>(parsedJsonEqui, out, new LongSerializer(), new LongSerializer());
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
    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));

    op->getInternalTimerService()->advanceWatermark(1500);

    /*
    Output Batch 1
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    0     , 1000         , 12   , 0     , 1000         , 100

    Output Batch 2
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    1     , 2000         , 24   , 1     , 2000         , 200
    */

    auto v11 = new omniruntime::vec::Vector<int32_t>(1);
    v11->SetValue(0, 0);
    auto v12 = new omniruntime::vec::Vector<int64_t>(1);
    v12->SetValue(0, 1000);
    auto v13 = new omniruntime::vec::Vector<int32_t>(1);
    v13->SetValue(0, 12);
    auto v14 = new omniruntime::vec::Vector<int32_t>(1);
    v14->SetValue(0, 0);
    auto v15 = new omniruntime::vec::Vector<int64_t>(1);
    v15->SetValue(0, 1000);
    auto v16 = new omniruntime::vec::Vector<int32_t>(1);
    v16->SetValue(0, 100);
    auto b1 = new omnistream::VectorBatch(1);
    b1->Append(v11);
    b1->Append(v12);
    b1->Append(v13);
    b1->Append(v14);
    b1->Append(v15);
    b1->Append(v16);

    EXPECT_TRUE(inOutput(b1, out));

    auto v21 = new omniruntime::vec::Vector<int32_t>(1);
    v21->SetValue(0, 1);
    auto v22 = new omniruntime::vec::Vector<int64_t>(1);
    v22->SetValue(0, 2000);
    auto v23 = new omniruntime::vec::Vector<int32_t>(1);
    v23->SetValue(0, 24);
    auto v24 = new omniruntime::vec::Vector<int32_t>(1);
    v24->SetValue(0, 1);
    auto v25 = new omniruntime::vec::Vector<int64_t>(1);
    v25->SetValue(0, 2000);
    auto v26 = new omniruntime::vec::Vector<int32_t>(1);
    v26->SetValue(0, 200);
    auto b2 = new omnistream::VectorBatch(1);
    b2->Append(v21);
    b2->Append(v22);
    b2->Append(v23);
    b2->Append(v24);
    b2->Append(v25);
    b2->Append(v26);

    EXPECT_TRUE(!inOutput(b2, out));

    op->getInternalTimerService()->advanceWatermark(2500);

    EXPECT_TRUE(inOutput(b2, out));

    op->close();
    delete op;
    delete initializer;
    delete out;
    delete b1;
    delete b2;
}

TEST(WindowJoinOperatorTest, DISABLED_WindowSeparationWithRocksdbTest)
{
    // Batch left
    /*
    (int) , (long)       , (int)
    KeyCol, WindowEndTime, value
    0     , 1000         , 12
    1     , 2000         , 24
    */

    auto vbatchLeft = new omnistream::VectorBatch(2);

    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(2);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 1);
    vbatchLeft->Append(vKeyLeft);

    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(2);
    vWindowEndTimeLeft->SetValue(0, 1000);
    vWindowEndTimeLeft->SetValue(1, 2000);
    vbatchLeft->Append(vWindowEndTimeLeft);

    auto vValLeft = new omniruntime::vec::Vector<int32_t>(2);
    vValLeft->SetValue(0, 12);
    vValLeft->SetValue(1, 24);
    vbatchLeft->Append(vValLeft);

    // Batch right
    /*
    (int) , (long)       , (int)
    KeyCol, WindowEndTime, value
    0     , 1000         , 100
    1     , 2000         , 200
    */

    auto vbatchRight = new omnistream::VectorBatch(2);

    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(2);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 1);
    vbatchRight->Append(vKeyRight);

    auto vWindowEndTimeRight = new omniruntime::vec::Vector<int64_t>(2);
    vWindowEndTimeRight->SetValue(0, 1000);
    vWindowEndTimeRight->SetValue(1, 2000);
    vbatchRight->Append(vWindowEndTimeRight);

    auto vValRight = new omniruntime::vec::Vector<int32_t>(2);
    vValRight->SetValue(0, 100);
    vValRight->SetValue(1, 200);
    vbatchRight->Append(vValRight);

    auto* out = new OutputTestVectorBatch();
    auto op = new InnerJoinOperator<int64_t>(parsedJsonEqui, out, new LongSerializer(), new LongSerializer());
    std::vector<std::string> backendHomes = {"/tmp/rocksdb_ut/WindowJoinOperatorTest/"};
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
    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));

    op->getInternalTimerService()->advanceWatermark(1500);

    /*
    Output Batch 1
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    0     , 1000         , 12   , 0     , 1000         , 100

    Output Batch 2
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    1     , 2000         , 24   , 1     , 2000         , 200
    */

    auto v11 = new omniruntime::vec::Vector<int32_t>(1);
    v11->SetValue(0, 0);
    auto v12 = new omniruntime::vec::Vector<int64_t>(1);
    v12->SetValue(0, 1000);
    auto v13 = new omniruntime::vec::Vector<int32_t>(1);
    v13->SetValue(0, 12);
    auto v14 = new omniruntime::vec::Vector<int32_t>(1);
    v14->SetValue(0, 0);
    auto v15 = new omniruntime::vec::Vector<int64_t>(1);
    v15->SetValue(0, 1000);
    auto v16 = new omniruntime::vec::Vector<int32_t>(1);
    v16->SetValue(0, 100);
    auto b1 = new omnistream::VectorBatch(1);
    b1->Append(v11);
    b1->Append(v12);
    b1->Append(v13);
    b1->Append(v14);
    b1->Append(v15);
    b1->Append(v16);

    EXPECT_TRUE(inOutput(b1, out));

    auto v21 = new omniruntime::vec::Vector<int32_t>(1);
    v21->SetValue(0, 1);
    auto v22 = new omniruntime::vec::Vector<int64_t>(1);
    v22->SetValue(0, 2000);
    auto v23 = new omniruntime::vec::Vector<int32_t>(1);
    v23->SetValue(0, 24);
    auto v24 = new omniruntime::vec::Vector<int32_t>(1);
    v24->SetValue(0, 1);
    auto v25 = new omniruntime::vec::Vector<int64_t>(1);
    v25->SetValue(0, 2000);
    auto v26 = new omniruntime::vec::Vector<int32_t>(1);
    v26->SetValue(0, 200);
    auto b2 = new omnistream::VectorBatch(1);
    b2->Append(v21);
    b2->Append(v22);
    b2->Append(v23);
    b2->Append(v24);
    b2->Append(v25);
    b2->Append(v26);

    EXPECT_TRUE(!inOutput(b2, out));

    op->getInternalTimerService()->advanceWatermark(2500);

    EXPECT_TRUE(inOutput(b2, out));

    op->close();
    delete op;
    delete initializer;
    delete out;
    delete b1;
    delete b2;
}

TEST(WindowJoinOperatorTest, DISABLED_SemiJoinTest)
{
    auto vbatchLeft = getLeftBatch();
    auto vbatchRight = getRightBatch();

    auto* out = new OutputTestVectorBatch();
    auto op = new SemiAntiJoinOperator<int32_t>(parsedJsonEqui, out, new LongSerializer(), new LongSerializer(), false);
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
    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));

    op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Output Batch 1
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    0     , 1000         , 12   , 0     , 1000         , 100

    Output Batch 2
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    1     , 1000         , 24   , 1     , 1000         , 200
    1     , 1000         , 24   , 1     , 1000         , 300
    */

    auto v11 = new omniruntime::vec::Vector<int32_t>(1);
    v11->SetValue(0, 0);
    auto v12 = new omniruntime::vec::Vector<int64_t>(1);
    v12->SetValue(0, 1000);
    auto v13 = new omniruntime::vec::Vector<int32_t>(1);
    v13->SetValue(0, 12);
    auto v14 = new omniruntime::vec::Vector<int32_t>(1);
    v14->SetValue(0, 0);
    auto v15 = new omniruntime::vec::Vector<int64_t>(1);
    v15->SetValue(0, 1000);
    auto v16 = new omniruntime::vec::Vector<int32_t>(1);
    v16->SetValue(0, 100);

    EXPECT_TRUE(compareCol<int32_t>(v11, out->getAll()[0]->Get(0)));
    EXPECT_TRUE(compareCol<int32_t>(v12, out->getAll()[0]->Get(1)));
    EXPECT_TRUE(compareCol<int32_t>(v13, out->getAll()[0]->Get(2)));
    EXPECT_TRUE(compareCol<int32_t>(v14, out->getAll()[0]->Get(3)));
    EXPECT_TRUE(compareCol<int32_t>(v15, out->getAll()[0]->Get(4)));
    EXPECT_TRUE(compareCol<int32_t>(v16, out->getAll()[0]->Get(5)));

    auto v21 = new omniruntime::vec::Vector<int32_t>(2);
    v21->SetValue(0, 1);
    v21->SetValue(1, 1);
    auto v22 = new omniruntime::vec::Vector<int64_t>(2);
    v22->SetValue(0, 1000);
    v22->SetValue(1, 1000);
    auto v23 = new omniruntime::vec::Vector<int32_t>(2);
    v23->SetValue(0, 24);
    v23->SetValue(1, 24);
    auto v24 = new omniruntime::vec::Vector<int32_t>(2);
    v24->SetValue(0, 1);
    v24->SetValue(1, 1);
    auto v25 = new omniruntime::vec::Vector<int64_t>(2);
    v25->SetValue(0, 1000);
    v25->SetValue(1, 1000);
    auto v26 = new omniruntime::vec::Vector<int32_t>(2);
    v26->SetValue(0, 200);
    v26->SetValue(1, 300);

    EXPECT_TRUE(compareCol<int32_t>(v21, out->getAll()[1]->Get(0)));
    EXPECT_TRUE(compareCol<int32_t>(v22, out->getAll()[1]->Get(1)));
    EXPECT_TRUE(compareCol<int32_t>(v23, out->getAll()[1]->Get(2)));
    EXPECT_TRUE(compareCol<int32_t>(v24, out->getAll()[1]->Get(3)));
    EXPECT_TRUE(compareCol<int32_t>(v25, out->getAll()[1]->Get(4)));
    EXPECT_TRUE(compareCol<int32_t>(v26, out->getAll()[1]->Get(5)));

    op->close();
    delete op;
    delete out;
}

std::string testDescriptionNonEqui = R"delimiter({
  "originDescription": null,
  "leftInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "rightInputTypes": [
    "INT",
    "BIGINT",
    "INT"
  ],
  "outputTypes": [
    "INT",
    "BIGINT",
    "INT",
    "INT",
    "BIGINT",
    "INT"
  ],
  "leftJoinKey": [],
  "rightJoinKey": [],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": {"exprType":"BINARY","returnType": 4,"operator":"GREATER_THAN_OR_EQUAL","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},"right":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":3}},
  "joinType": "InnerJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

nlohmann::json parsedJsonNonEqui = nlohmann::json::parse(testDescriptionNonEqui);

TEST(WindowJoinOperatorTest, DISABLED_InnerJoinNonEquiTest)
{
    auto vbatchLeft = getLeftBatch();
    auto vbatchRight = getRightBatch();

    auto* out = new OutputTestVectorBatch();
    auto op = new InnerJoinOperator<int32_t>(parsedJsonNonEqui, out, new LongSerializer(), new LongSerializer());
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
    op->initializeState(initializer, new VoidNamespaceSerializer());
    op->open();
    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));

    op->getInternalTimerService()->advanceWatermark(100000);

    auto v11 = new omniruntime::vec::Vector<int32_t>(7);
    v11->SetValue(0, 0);
    v11->SetValue(1, 1);
    v11->SetValue(2, 1);
    v11->SetValue(3, 1);
    v11->SetValue(4, 2);
    v11->SetValue(5, 2);
    v11->SetValue(6, 2);
    auto v12 = new omniruntime::vec::Vector<int64_t>(7);
    v12->SetValue(0, 1000);
    v12->SetValue(1, 1000);
    v12->SetValue(2, 1000);
    v12->SetValue(3, 1000);
    v12->SetValue(4, 1000);
    v12->SetValue(5, 1000);
    v12->SetValue(6, 1000);
    auto v13 = new omniruntime::vec::Vector<int32_t>(7);
    v13->SetValue(0, 12);
    v13->SetValue(1, 24);
    v13->SetValue(2, 24);
    v13->SetValue(3, 24);
    v13->SetValue(4, 36);
    v13->SetValue(5, 36);
    v13->SetValue(6, 36);
    auto v14 = new omniruntime::vec::Vector<int32_t>(7);
    v14->SetValue(0, 0);
    v14->SetValue(1, 0);
    v14->SetValue(2, 1);
    v14->SetValue(3, 1);
    v14->SetValue(4, 0);
    v14->SetValue(5, 1);
    v14->SetValue(6, 1);
    auto v15 = new omniruntime::vec::Vector<int64_t>(7);
    v15->SetValue(0, 1000);
    v15->SetValue(1, 1000);
    v15->SetValue(2, 1000);
    v15->SetValue(3, 1000);
    v15->SetValue(4, 1000);
    v15->SetValue(5, 1000);
    v15->SetValue(6, 1000);
    auto v16 = new omniruntime::vec::Vector<int32_t>(7);
    v16->SetValue(0, 100);
    v16->SetValue(1, 100);
    v16->SetValue(2, 200);
    v16->SetValue(3, 300);
    v16->SetValue(4, 100);
    v16->SetValue(5, 200);
    v16->SetValue(6, 300);
    auto b1 = new omnistream::VectorBatch(7);
    b1->Append(v11);
    b1->Append(v12);
    b1->Append(v13);
    b1->Append(v14);
    b1->Append(v15);
    b1->Append(v16);

    EXPECT_TRUE(inOutput(b1, out));

    op->close();
    delete op;
    delete initializer;
    delete out;
}

TEST(WindowJoinOperatorTest, LeftOuterJoinNonEquiOrderTest)
{
    // Single join key group (key=0) with a non-equi condition: left.value < right.value.
    // Verifies that output rows are emitted per left row in input order, i.e. a left row's
    // matched pairs are emitted before the next left row, and an unmatched left row is
    // null-padded right when its turn comes (matching Flink's interleaved row order).
    auto vbatchLeft = new omnistream::VectorBatch(3);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(3);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 0);
    vKeyLeft->SetValue(2, 0);
    vbatchLeft->Append(vKeyLeft);
    auto vWindLeft = new omniruntime::vec::Vector<int64_t>(3);
    vWindLeft->SetValue(0, 1000);
    vWindLeft->SetValue(1, 1000);
    vWindLeft->SetValue(2, 1000);
    vbatchLeft->Append(vWindLeft);
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(3);
    vValLeft->SetValue(0, 10);
    vValLeft->SetValue(1, 30);
    vValLeft->SetValue(2, 20);
    vbatchLeft->Append(vValLeft);

    auto vbatchRight = new omnistream::VectorBatch(2);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(2);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 0);
    vbatchRight->Append(vKeyRight);
    auto vWindRight = new omniruntime::vec::Vector<int64_t>(2);
    vWindRight->SetValue(0, 1000);
    vWindRight->SetValue(1, 1000);
    vbatchRight->Append(vWindRight);
    auto vValRight = new omniruntime::vec::Vector<int32_t>(2);
    vValRight->SetValue(0, 25);
    vValRight->SetValue(1, 15);
    vbatchRight->Append(vValRight);

    auto ctx = createLeftOuterJoinTestContext(parsedJsonLeftOuterNonEqui);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Left rows in order: value 10, 30, 20. Right rows: value 25, 15.
    Condition: left.value < right.value.

    Flink emits per left row:
      left 10 -> matched pairs (10,15),(10,25)
      left 30 -> no match -> null-padded (30, null)
      left 20 -> matched pair (20,25)

    Expected output batches (in emission order):
      batch0: 2 rows: (10,25),(10,15)
      batch1: 1 row : (30, null)
      batch2: 1 row : (20,25)
    */
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(3), batches.size());

    auto checkRow = [](omnistream::VectorBatch* batch, int row, int32_t leftVal, int32_t rightVal, bool rightNull) {
        auto* l = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(2));
        auto* r = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(5));
        EXPECT_EQ(leftVal, l->GetValue(row)) << "left value mismatch at batch row " << row;
        if (rightNull) {
            EXPECT_TRUE(r->IsNull(row)) << "right value should be null at batch row " << row;
        } else {
            EXPECT_FALSE(r->IsNull(row)) << "right value should not be null at batch row " << row;
            EXPECT_EQ(rightVal, r->GetValue(row)) << "right value mismatch at batch row " << row;
        }
    };

    auto b0 = batches[0];
    ASSERT_EQ(2, b0->GetRowCount());
    checkRow(b0, 0, 10, 25, false);
    checkRow(b0, 1, 10, 15, false);

    auto b1 = batches[1];
    ASSERT_EQ(1, b1->GetRowCount());
    checkRow(b1, 0, 30, 0, true);

    auto b2 = batches[2];
    ASSERT_EQ(1, b2->GetRowCount());
    checkRow(b2, 0, 20, 25, false);

    destroyLeftOuterJoinTestContext(ctx);
}

// Helper to build a LeftOuterJoin description whose residual (non-equi) predicate references
// CHAR, DATE and DECIMAL(38,10) columns, so the JIT filter exercises those types.
nlohmann::json buildMixedTypesLeftOuterDescription()
{
    nlohmann::json desc;
    desc["originDescription"] = nullptr;
    desc["leftInputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["rightInputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["outputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)",
                           "INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["leftJoinKey"] = {0};
    desc["rightJoinKey"] = {0};
    desc["leftWindowEndIndex"] = 1;
    desc["rightWindowEndIndex"] = 1;
    desc["joinType"] = "LeftOuterJoin";
    desc["leftWindowing"] = "TUMBLE(size=[10 s])";
    desc["leftTimeAttributeType"] = 2;
    desc["rightWindowing"] = "TUMBLE(size=[10 s])";
    desc["rightTimeAttributeType"] = 2;

    const int32_t charType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("CHAR(10)"));
    const int32_t dateType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("DATE"));
    const int32_t decType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("DECIMAL(38,10)"));

    auto fieldRef = [&](int32_t typeId, int32_t col) {
        nlohmann::json f;
        f["exprType"] = "FIELD_REFERENCE";
        f["dataType"] = typeId;
        f["colVal"] = col;
        return f;
    };

    nlohmann::json charEq = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "EQUAL"},
        {"left", fieldRef(charType, 2)}, {"right", fieldRef(charType, 7)}};
    // string FIELD_REFERENCEs accept an optional width (JSONParser::ParseJSONFieldRef)
    charEq["left"]["width"] = 10;
    charEq["right"]["width"] = 10;
    nlohmann::json dateLt = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "LESS_THAN"},
        {"left", fieldRef(dateType, 3)}, {"right", fieldRef(dateType, 8)}};
    nlohmann::json decGt = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "GREATER_THAN"},
        {"left", fieldRef(decType, 4)}, {"right", fieldRef(decType, 9)}};
    // decimal FIELD_REFERENCE requires precision/scale (see JSONParser::ParseJSONFieldRef)
    decGt["left"]["precision"] = 38;
    decGt["left"]["scale"] = 10;
    decGt["right"]["precision"] = 38;
    decGt["right"]["scale"] = 10;

    desc["nonEquiCondition"] = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "AND"},
        {"left", {{"exprType", "BINARY"}, {"returnType", 4}, {"operator", "AND"},
                  {"left", charEq}, {"right", dateLt}}},
        {"right", decGt}};
    return desc;
}

TEST(WindowJoinOperatorTest, LeftOuterJoinNonEquiMixedTypesTest)
{
    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    using DecimalVector = omniruntime::vec::Vector<omniruntime::type::Decimal128>;

    // Left rows: (key=0, windowEnd=1000, char, date, dec)
    //   ("aa", 1, 10), ("bb", 3, 40), ("cc", 5, 60)
    auto vbatchLeft = new omnistream::VectorBatch(3);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(3);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 0);
    vKeyLeft->SetValue(2, 0);
    vbatchLeft->Append(vKeyLeft);
    auto vWindLeft = new omniruntime::vec::Vector<int64_t>(3);
    vWindLeft->SetValue(0, 1000);
    vWindLeft->SetValue(1, 1000);
    vWindLeft->SetValue(2, 1000);
    vbatchLeft->Append(vWindLeft);
    auto vCharLeft = new VarcharVector(3);
    vCharLeft->SetValue(0, std::string_view("aa"));
    vCharLeft->SetValue(1, std::string_view("bb"));
    vCharLeft->SetValue(2, std::string_view("cc"));
    vbatchLeft->Append(vCharLeft);
    auto vDateLeft = new omniruntime::vec::Vector<int32_t>(3);
    vDateLeft->SetValue(0, 1);
    vDateLeft->SetValue(1, 3);
    vDateLeft->SetValue(2, 5);
    vbatchLeft->Append(vDateLeft);
    auto vDecLeft = new DecimalVector(3);
    vDecLeft->SetValue(0, omniruntime::type::Decimal128("10"));
    vDecLeft->SetValue(1, omniruntime::type::Decimal128("40"));
    vDecLeft->SetValue(2, omniruntime::type::Decimal128("60"));
    vbatchLeft->Append(vDecLeft);

    // Right rows: ("aa", 2, 5), ("bb", 4, 30), ("bb", 6, 50)
    auto vbatchRight = new omnistream::VectorBatch(3);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(3);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 0);
    vKeyRight->SetValue(2, 0);
    vbatchRight->Append(vKeyRight);
    auto vWindRight = new omniruntime::vec::Vector<int64_t>(3);
    vWindRight->SetValue(0, 1000);
    vWindRight->SetValue(1, 1000);
    vWindRight->SetValue(2, 1000);
    vbatchRight->Append(vWindRight);
    auto vCharRight = new VarcharVector(3);
    vCharRight->SetValue(0, std::string_view("aa"));
    vCharRight->SetValue(1, std::string_view("bb"));
    vCharRight->SetValue(2, std::string_view("bb"));
    vbatchRight->Append(vCharRight);
    auto vDateRight = new omniruntime::vec::Vector<int32_t>(3);
    vDateRight->SetValue(0, 2);
    vDateRight->SetValue(1, 4);
    vDateRight->SetValue(2, 6);
    vbatchRight->Append(vDateRight);
    auto vDecRight = new DecimalVector(3);
    vDecRight->SetValue(0, omniruntime::type::Decimal128("5"));
    vDecRight->SetValue(1, omniruntime::type::Decimal128("30"));
    vDecRight->SetValue(2, omniruntime::type::Decimal128("50"));
    vbatchRight->Append(vDecRight);

    auto ctx = createLeftOuterJoinTestContext(buildMixedTypesLeftOuterDescription());
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Condition: left.char = right.char AND left.date < right.date AND left.dec > right.dec
      left ("aa",1,10) -> matches right ("aa",2,5)  -> 1 row
      left ("bb",3,40) -> matches right ("bb",4,30) -> 1 row
      left ("cc",5,60) -> no match -> null-padded
    Expected 3 batches of 1 row.
    */
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(3), batches.size());

    auto checkBatch =
        [](omnistream::VectorBatch* batch, const std::string& lChar, int32_t lDate, int32_t lDec,
           const std::string& rChar, int32_t rDate, int32_t rDec, bool rightNull) {
            ASSERT_EQ(1, batch->GetRowCount());
            auto* lCharVec = reinterpret_cast<VarcharVector*>(batch->Get(2));
            auto* lDateVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(3));
            auto* lDecVec = reinterpret_cast<DecimalVector*>(batch->Get(4));
            EXPECT_EQ(lChar, std::string(lCharVec->GetValue(0))) << "left char mismatch";
            EXPECT_EQ(lDate, lDateVec->GetValue(0)) << "left date mismatch";
            EXPECT_EQ(omniruntime::type::Decimal128(std::to_string(lDec)), lDecVec->GetValue(0))
                << "left decimal mismatch";
            auto* rCharVec = reinterpret_cast<VarcharVector*>(batch->Get(7));
            auto* rDateVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(8));
            auto* rDecVec = reinterpret_cast<DecimalVector*>(batch->Get(9));
            if (rightNull) {
                EXPECT_TRUE(rCharVec->IsNull(0)) << "right char should be null";
                EXPECT_TRUE(rDateVec->IsNull(0)) << "right date should be null";
                EXPECT_TRUE(rDecVec->IsNull(0)) << "right decimal should be null";
            } else {
                EXPECT_EQ(rChar, std::string(rCharVec->GetValue(0))) << "right char mismatch";
                EXPECT_EQ(rDate, rDateVec->GetValue(0)) << "right date mismatch";
                EXPECT_EQ(omniruntime::type::Decimal128(std::to_string(rDec)), rDecVec->GetValue(0))
                    << "right decimal mismatch";
            }
        };

    checkBatch(batches[0], "aa", 1, 10, "aa", 2, 5, false);
    checkBatch(batches[1], "bb", 3, 40, "bb", 4, 30, false);
    checkBatch(batches[2], "cc", 5, 60, "", 0, 0, true);

    destroyLeftOuterJoinTestContext(ctx);
}

// Helper to build a RightOuterJoin description whose residual (non-equi) predicate
// references CHAR, DATE and DECIMAL(38,10) columns, so the JIT filter exercises those types.
nlohmann::json buildMixedTypesRightOuterDescription()
{
    nlohmann::json desc;
    desc["originDescription"] = nullptr;
    desc["leftInputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["rightInputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["outputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)",
                           "INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["leftJoinKey"] = {0};
    desc["rightJoinKey"] = {0};
    desc["leftWindowEndIndex"] = 1;
    desc["rightWindowEndIndex"] = 1;
    desc["joinType"] = "RightOuterJoin";
    desc["leftWindowing"] = "TUMBLE(size=[10 s])";
    desc["leftTimeAttributeType"] = 2;
    desc["rightWindowing"] = "TUMBLE(size=[10 s])";
    desc["rightTimeAttributeType"] = 2;

    const int32_t charType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("CHAR(10)"));
    const int32_t dateType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("DATE"));
    const int32_t decType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("DECIMAL(38,10)"));

    auto fieldRef = [&](int32_t typeId, int32_t col) {
        nlohmann::json f;
        f["exprType"] = "FIELD_REFERENCE";
        f["dataType"] = typeId;
        f["colVal"] = col;
        return f;
    };

    nlohmann::json charEq = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "EQUAL"},
        {"left", fieldRef(charType, 2)}, {"right", fieldRef(charType, 7)}};
    // string FIELD_REFERENCEs accept an optional width (JSONParser::ParseJSONFieldRef)
    charEq["left"]["width"] = 10;
    charEq["right"]["width"] = 10;
    nlohmann::json dateLt = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "LESS_THAN"},
        {"left", fieldRef(dateType, 3)}, {"right", fieldRef(dateType, 8)}};
    nlohmann::json decGt = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "GREATER_THAN"},
        {"left", fieldRef(decType, 4)}, {"right", fieldRef(decType, 9)}};
    // decimal FIELD_REFERENCE requires precision/scale (see JSONParser::ParseJSONFieldRef)
    decGt["left"]["precision"] = 38;
    decGt["left"]["scale"] = 10;
    decGt["right"]["precision"] = 38;
    decGt["right"]["scale"] = 10;

    desc["nonEquiCondition"] = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "AND"},
        {"left", {{"exprType", "BINARY"}, {"returnType", 4}, {"operator", "AND"},
                  {"left", charEq}, {"right", dateLt}}},
        {"right", decGt}};
    return desc;
}

TEST(WindowJoinOperatorTest, RightOuterJoinNonEquiMixedTypesTest)
{
    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    using DecimalVector = omniruntime::vec::Vector<omniruntime::type::Decimal128>;

    // Left rows: (key=0, windowEnd=1000, char, date, dec)
    //   ("aa", 1, 10), ("bb", 3, 40), ("cc", 5, 60)
    auto vbatchLeft = new omnistream::VectorBatch(3);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(3);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 0);
    vKeyLeft->SetValue(2, 0);
    vbatchLeft->Append(vKeyLeft);
    auto vWindLeft = new omniruntime::vec::Vector<int64_t>(3);
    vWindLeft->SetValue(0, 1000);
    vWindLeft->SetValue(1, 1000);
    vWindLeft->SetValue(2, 1000);
    vbatchLeft->Append(vWindLeft);
    auto vCharLeft = new VarcharVector(3);
    vCharLeft->SetValue(0, std::string_view("aa"));
    vCharLeft->SetValue(1, std::string_view("bb"));
    vCharLeft->SetValue(2, std::string_view("cc"));
    vbatchLeft->Append(vCharLeft);
    auto vDateLeft = new omniruntime::vec::Vector<int32_t>(3);
    vDateLeft->SetValue(0, 1);
    vDateLeft->SetValue(1, 3);
    vDateLeft->SetValue(2, 5);
    vbatchLeft->Append(vDateLeft);
    auto vDecLeft = new DecimalVector(3);
    vDecLeft->SetValue(0, omniruntime::type::Decimal128("10"));
    vDecLeft->SetValue(1, omniruntime::type::Decimal128("40"));
    vDecLeft->SetValue(2, omniruntime::type::Decimal128("60"));
    vbatchLeft->Append(vDecLeft);

    // Right rows: ("aa", 2, 5), ("bb", 4, 30), ("bb", 6, 50)
    auto vbatchRight = new omnistream::VectorBatch(3);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(3);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 0);
    vKeyRight->SetValue(2, 0);
    vbatchRight->Append(vKeyRight);
    auto vWindRight = new omniruntime::vec::Vector<int64_t>(3);
    vWindRight->SetValue(0, 1000);
    vWindRight->SetValue(1, 1000);
    vWindRight->SetValue(2, 1000);
    vbatchRight->Append(vWindRight);
    auto vCharRight = new VarcharVector(3);
    vCharRight->SetValue(0, std::string_view("aa"));
    vCharRight->SetValue(1, std::string_view("bb"));
    vCharRight->SetValue(2, std::string_view("bb"));
    vbatchRight->Append(vCharRight);
    auto vDateRight = new omniruntime::vec::Vector<int32_t>(3);
    vDateRight->SetValue(0, 2);
    vDateRight->SetValue(1, 4);
    vDateRight->SetValue(2, 6);
    vbatchRight->Append(vDateRight);
    auto vDecRight = new DecimalVector(3);
    vDecRight->SetValue(0, omniruntime::type::Decimal128("5"));
    vDecRight->SetValue(1, omniruntime::type::Decimal128("30"));
    vDecRight->SetValue(2, omniruntime::type::Decimal128("50"));
    vbatchRight->Append(vDecRight);

    auto ctx = createRightOuterJoinTestContext(buildMixedTypesRightOuterDescription());
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Condition: left.char = right.char AND left.date < right.date AND left.dec > right.dec
    RightOuterJoin emits per right record:
      right ("aa",2,5)  -> matches left ("aa",1,10)      -> 1 row
      right ("bb",4,30) -> matches left ("bb",3,40)      -> 1 row
      right ("bb",6,50) -> no match -> left-null-padded  -> 1 row
    Expected 3 batches of 1 row.
    */
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(3), batches.size());

    auto checkBatch =
        [](omnistream::VectorBatch* batch, const std::string& lChar, int32_t lDate, int32_t lDec,
           const std::string& rChar, int32_t rDate, int32_t rDec, bool leftNull) {
            ASSERT_EQ(1, batch->GetRowCount());
            auto* lCharVec = reinterpret_cast<VarcharVector*>(batch->Get(2));
            auto* lDateVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(3));
            auto* lDecVec = reinterpret_cast<DecimalVector*>(batch->Get(4));
            if (leftNull) {
                EXPECT_TRUE(lCharVec->IsNull(0)) << "left char should be null";
                EXPECT_TRUE(lDateVec->IsNull(0)) << "left date should be null";
                EXPECT_TRUE(lDecVec->IsNull(0)) << "left decimal should be null";
            } else {
                EXPECT_EQ(lChar, std::string(lCharVec->GetValue(0))) << "left char mismatch";
                EXPECT_EQ(lDate, lDateVec->GetValue(0)) << "left date mismatch";
                EXPECT_EQ(omniruntime::type::Decimal128(std::to_string(lDec)), lDecVec->GetValue(0))
                    << "left decimal mismatch";
            }
            auto* rCharVec = reinterpret_cast<VarcharVector*>(batch->Get(7));
            auto* rDateVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(8));
            auto* rDecVec = reinterpret_cast<DecimalVector*>(batch->Get(9));
            EXPECT_EQ(rChar, std::string(rCharVec->GetValue(0))) << "right char mismatch";
            EXPECT_EQ(rDate, rDateVec->GetValue(0)) << "right date mismatch";
            EXPECT_EQ(omniruntime::type::Decimal128(std::to_string(rDec)), rDecVec->GetValue(0))
                << "right decimal mismatch";
        };

    checkBatch(batches[0], "aa", 1, 10, "aa", 2, 5, false);
    checkBatch(batches[1], "bb", 3, 40, "bb", 4, 30, false);
    checkBatch(batches[2], "cc", 5, 60, "bb", 6, 50, true);

    destroyRightOuterJoinTestContext(ctx);
}

// Helper to build a FullOuterJoin description whose residual (non-equi) predicate
// references CHAR, DATE and DECIMAL(38,10) columns, so the JIT filter exercises those types.
nlohmann::json buildMixedTypesFullOuterDescription()
{
    nlohmann::json desc;
    desc["originDescription"] = nullptr;
    desc["leftInputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["rightInputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["outputTypes"] = {"INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)",
                           "INT", "BIGINT", "CHAR(10)", "DATE", "DECIMAL(38,10)"};
    desc["leftJoinKey"] = {0};
    desc["rightJoinKey"] = {0};
    desc["leftWindowEndIndex"] = 1;
    desc["rightWindowEndIndex"] = 1;
    desc["joinType"] = "FullOuterJoin";
    desc["leftWindowing"] = "TUMBLE(size=[10 s])";
    desc["leftTimeAttributeType"] = 2;
    desc["rightWindowing"] = "TUMBLE(size=[10 s])";
    desc["rightTimeAttributeType"] = 2;

    const int32_t charType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("CHAR(10)"));
    const int32_t dateType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("DATE"));
    const int32_t decType = static_cast<int32_t>(LogicalType::flinkTypeToOmniTypeId("DECIMAL(38,10)"));

    auto fieldRef = [&](int32_t typeId, int32_t col) {
        nlohmann::json f;
        f["exprType"] = "FIELD_REFERENCE";
        f["dataType"] = typeId;
        f["colVal"] = col;
        return f;
    };

    nlohmann::json charEq = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "EQUAL"},
        {"left", fieldRef(charType, 2)}, {"right", fieldRef(charType, 7)}};
    // string FIELD_REFERENCEs accept an optional width (JSONParser::ParseJSONFieldRef)
    charEq["left"]["width"] = 10;
    charEq["right"]["width"] = 10;
    nlohmann::json dateLt = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "LESS_THAN"},
        {"left", fieldRef(dateType, 3)}, {"right", fieldRef(dateType, 8)}};
    nlohmann::json decGt = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "GREATER_THAN"},
        {"left", fieldRef(decType, 4)}, {"right", fieldRef(decType, 9)}};
    // decimal FIELD_REFERENCE requires precision/scale (see JSONParser::ParseJSONFieldRef)
    decGt["left"]["precision"] = 38;
    decGt["left"]["scale"] = 10;
    decGt["right"]["precision"] = 38;
    decGt["right"]["scale"] = 10;

    desc["nonEquiCondition"] = {
        {"exprType", "BINARY"}, {"returnType", 4}, {"operator", "AND"},
        {"left", {{"exprType", "BINARY"}, {"returnType", 4}, {"operator", "AND"},
                  {"left", charEq}, {"right", dateLt}}},
        {"right", decGt}};
    return desc;
}

TEST(WindowJoinOperatorTest, FullOuterJoinNonEquiMixedTypesTest)
{
    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    using DecimalVector = omniruntime::vec::Vector<omniruntime::type::Decimal128>;

    // Left rows: (key=0, windowEnd=1000, char, date, dec)
    //   ("aa", 1, 10), ("bb", 3, 40), ("cc", 5, 60)
    auto vbatchLeft = new omnistream::VectorBatch(3);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(3);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 0);
    vKeyLeft->SetValue(2, 0);
    vbatchLeft->Append(vKeyLeft);
    auto vWindLeft = new omniruntime::vec::Vector<int64_t>(3);
    vWindLeft->SetValue(0, 1000);
    vWindLeft->SetValue(1, 1000);
    vWindLeft->SetValue(2, 1000);
    vbatchLeft->Append(vWindLeft);
    auto vCharLeft = new VarcharVector(3);
    vCharLeft->SetValue(0, std::string_view("aa"));
    vCharLeft->SetValue(1, std::string_view("bb"));
    vCharLeft->SetValue(2, std::string_view("cc"));
    vbatchLeft->Append(vCharLeft);
    auto vDateLeft = new omniruntime::vec::Vector<int32_t>(3);
    vDateLeft->SetValue(0, 1);
    vDateLeft->SetValue(1, 3);
    vDateLeft->SetValue(2, 5);
    vbatchLeft->Append(vDateLeft);
    auto vDecLeft = new DecimalVector(3);
    vDecLeft->SetValue(0, omniruntime::type::Decimal128("10"));
    vDecLeft->SetValue(1, omniruntime::type::Decimal128("40"));
    vDecLeft->SetValue(2, omniruntime::type::Decimal128("60"));
    vbatchLeft->Append(vDecLeft);

    // Right rows: ("aa", 2, 5), ("bb", 4, 30), ("bb", 6, 50)
    auto vbatchRight = new omnistream::VectorBatch(3);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(3);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 0);
    vKeyRight->SetValue(2, 0);
    vbatchRight->Append(vKeyRight);
    auto vWindRight = new omniruntime::vec::Vector<int64_t>(3);
    vWindRight->SetValue(0, 1000);
    vWindRight->SetValue(1, 1000);
    vWindRight->SetValue(2, 1000);
    vbatchRight->Append(vWindRight);
    auto vCharRight = new VarcharVector(3);
    vCharRight->SetValue(0, std::string_view("aa"));
    vCharRight->SetValue(1, std::string_view("bb"));
    vCharRight->SetValue(2, std::string_view("bb"));
    vbatchRight->Append(vCharRight);
    auto vDateRight = new omniruntime::vec::Vector<int32_t>(3);
    vDateRight->SetValue(0, 2);
    vDateRight->SetValue(1, 4);
    vDateRight->SetValue(2, 6);
    vbatchRight->Append(vDateRight);
    auto vDecRight = new DecimalVector(3);
    vDecRight->SetValue(0, omniruntime::type::Decimal128("5"));
    vDecRight->SetValue(1, omniruntime::type::Decimal128("30"));
    vDecRight->SetValue(2, omniruntime::type::Decimal128("50"));
    vbatchRight->Append(vDecRight);

    auto ctx = createFullOuterJoinTestContext(buildMixedTypesFullOuterDescription());
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    /*
    Condition: left.char = right.char AND left.date < right.date AND left.dec > right.dec
      left ("aa",1,10) -> matches right ("aa",2,5)  -> 1 row
      left ("bb",3,40) -> matches right ("bb",4,30) -> 1 row
      left ("cc",5,60) -> no match -> right-null-padded -> 1 row
      right ("bb",6,50) -> never matched -> left-null-padded -> 1 row
    Expected 4 batches of 1 row.
    */
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(4), batches.size());

    auto checkBatch =
        [](omnistream::VectorBatch* batch, const std::string& lChar, int32_t lDate, int32_t lDec,
           const std::string& rChar, int32_t rDate, int32_t rDec, bool leftNull, bool rightNull) {
            ASSERT_EQ(1, batch->GetRowCount());
            auto* lCharVec = reinterpret_cast<VarcharVector*>(batch->Get(2));
            auto* lDateVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(3));
            auto* lDecVec = reinterpret_cast<DecimalVector*>(batch->Get(4));
            if (leftNull) {
                EXPECT_TRUE(lCharVec->IsNull(0)) << "left char should be null";
                EXPECT_TRUE(lDateVec->IsNull(0)) << "left date should be null";
                EXPECT_TRUE(lDecVec->IsNull(0)) << "left decimal should be null";
            } else {
                EXPECT_EQ(lChar, std::string(lCharVec->GetValue(0))) << "left char mismatch";
                EXPECT_EQ(lDate, lDateVec->GetValue(0)) << "left date mismatch";
                EXPECT_EQ(omniruntime::type::Decimal128(std::to_string(lDec)), lDecVec->GetValue(0))
                    << "left decimal mismatch";
            }
            auto* rCharVec = reinterpret_cast<VarcharVector*>(batch->Get(7));
            auto* rDateVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(8));
            auto* rDecVec = reinterpret_cast<DecimalVector*>(batch->Get(9));
            if (rightNull) {
                EXPECT_TRUE(rCharVec->IsNull(0)) << "right char should be null";
                EXPECT_TRUE(rDateVec->IsNull(0)) << "right date should be null";
                EXPECT_TRUE(rDecVec->IsNull(0)) << "right decimal should be null";
            } else {
                EXPECT_EQ(rChar, std::string(rCharVec->GetValue(0))) << "right char mismatch";
                EXPECT_EQ(rDate, rDateVec->GetValue(0)) << "right date mismatch";
                EXPECT_EQ(omniruntime::type::Decimal128(std::to_string(rDec)), rDecVec->GetValue(0))
                    << "right decimal mismatch";
            }
        };

    // Emission order: per-left (aa match, bb match, cc right-null-padded) then the unmatched
    // right record ("bb",6,50) left-null-padded.
    checkBatch(batches[0], "aa", 1, 10, "aa", 2, 5, false, false);
    checkBatch(batches[1], "bb", 3, 40, "bb", 4, 30, false, false);
    checkBatch(batches[2], "cc", 5, 60, "", 0, 0, false, true);
    checkBatch(batches[3], "", 0, 0, "bb", 6, 50, true, false);

    destroyFullOuterJoinTestContext(ctx);
}

std::string noKeyDescription = R"delimiter({
    "originDescription": null,
    "leftInputTypes": [
      "BIGINT",
      "BIGINT"
    ],
    "rightInputTypes": [
      "BIGINT",
      "BIGINT"
    ],
    "outputTypes": [
        "BIGINT", "BIGINT", "BIGINT", "BIGINT"
    ],
    "leftJoinKey": [],
    "rightJoinKey": [],
    "leftWindowEndIndex": 1,
    "rightWindowEndIndex": 1,
    "nonEquiCondition": null,
    "joinType": "InnerJoin",
    "leftWindowing": "TUMBLE(size=[10 s])",
    "leftTimeAttributeType": 2,
    "rightWindowing": "TUMBLE(size=[10 s])",
    "rightTimeAttributeType": 2
  })delimiter";

nlohmann::json parsedNoKeyDescription = nlohmann::json::parse(noKeyDescription);

TEST(WindowJoinOperatorTest, DISABLED_NoKeyJoinTest)
{
    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto v1 = new omniruntime::vec::Vector<int64_t>(1);
    auto v2 = new omniruntime::vec::Vector<int64_t>(1);
    v1->SetValue(0, 1);
    v2->SetValue(0, 1000);
    vbatchLeft->Append(v1);
    vbatchLeft->Append(v2);

    auto vbatchRight = new omnistream::VectorBatch(2);
    auto v3 = new omniruntime::vec::Vector<int64_t>(2);
    auto v4 = new omniruntime::vec::Vector<int64_t>(2);
    v3->SetValue(0, 2);
    v3->SetValue(1, 3);
    v4->SetValue(0, 1000);
    v4->SetValue(1, 1000);
    vbatchRight->Append(v3);
    vbatchRight->Append(v4);

    auto* out = new OutputTestVectorBatch();
    auto op = new InnerJoinOperator<int32_t>(parsedNoKeyDescription, out, new LongSerializer(), new LongSerializer());
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
    op->initializeState(initializer, new VoidNamespaceSerializer());
    op->open();
    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));

    op->getInternalTimerService()->advanceWatermark(100000);

    auto vbatchOut = new omnistream::VectorBatch(2);
    auto v5 = new omniruntime::vec::Vector<int64_t>(2);
    auto v6 = new omniruntime::vec::Vector<int64_t>(2);
    auto v7 = new omniruntime::vec::Vector<int64_t>(2);
    auto v8 = new omniruntime::vec::Vector<int64_t>(2);
    v5->SetValue(0, 1);
    v6->SetValue(0, 1000);
    v7->SetValue(0, 2);
    v8->SetValue(0, 1000);
    v5->SetValue(1, 1);
    v6->SetValue(1, 1000);
    v7->SetValue(1, 3);
    v8->SetValue(1, 1000);
    vbatchOut->Append(v5);
    vbatchOut->Append(v6);
    vbatchOut->Append(v7);
    vbatchOut->Append(v8);

    EXPECT_TRUE(inOutput(vbatchOut, out));

    op->close();
    delete op;
    delete initializer;
    delete out;
}

// Left outer join with VARCHAR columns and a many-to-many equi key group in one window.
// Right-side VARCHAR columns must be emitted in row-major order; pre-fix this exposed
// out-of-order writes into LargeStringContainer that corrupted the R side strings.
std::string testDescriptionLeftOuterVarchar = R"delimiter({
  "originDescription": null,
  "leftInputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "rightInputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "outputTypes": [
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)",
    "INT",
    "BIGINT",
    "VARCHAR(2147483647)"
  ],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": null,
  "joinType": "LeftOuterJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

nlohmann::json parsedJsonLeftOuterVarchar = nlohmann::json::parse(testDescriptionLeftOuterVarchar);

TEST(WindowJoinOperatorTest, LeftOuterJoinVarcharTest)
{
    // Left: 3 rows in the same join key group (key=1) and same window (windowEnd=1000).
    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto vbatchLeft = new omnistream::VectorBatch(3);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(3);
    vKeyLeft->SetValue(0, 1);
    vKeyLeft->SetValue(1, 1);
    vKeyLeft->SetValue(2, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vWindowEndLeft = new omniruntime::vec::Vector<int64_t>(3);
    vWindowEndLeft->SetValue(0, 1000);
    vWindowEndLeft->SetValue(1, 1000);
    vWindowEndLeft->SetValue(2, 1000);
    vbatchLeft->Append(vWindowEndLeft);
    auto vNameLeft = new VarcharVector(3);
    vNameLeft->SetValue(0, std::string_view("a"));
    vNameLeft->SetValue(1, std::string_view("b"));
    vNameLeft->SetValue(2, std::string_view("c"));
    vbatchLeft->Append(vNameLeft);

    // Right: 2 rows in the same key group and window.
    auto vbatchRight = new omnistream::VectorBatch(2);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(2);
    vKeyRight->SetValue(0, 1);
    vKeyRight->SetValue(1, 1);
    vbatchRight->Append(vKeyRight);
    auto vWindowEndRight = new omniruntime::vec::Vector<int64_t>(2);
    vWindowEndRight->SetValue(0, 1000);
    vWindowEndRight->SetValue(1, 1000);
    vbatchRight->Append(vWindowEndRight);
    auto vNameRight = new VarcharVector(2);
    vNameRight->SetValue(0, std::string_view("x"));
    vNameRight->SetValue(1, std::string_view("y"));
    vbatchRight->Append(vNameRight);

    auto ctx = createLeftOuterJoinTestContext(parsedJsonLeftOuterVarchar);
    ctx.op->processBatch1(new StreamRecord(vbatchLeft));
    ctx.op->processBatch2(new StreamRecord(vbatchRight));

    ctx.op->getInternalTimerService()->advanceWatermark(100000);

    // 3 left x 2 right = 6 rows in one output batch, left-major:
    // L_name: a,a,b,b,c,c ; R_name: x,y,x,y,x,y
    auto& batches = ctx.out->getAll();
    ASSERT_EQ(static_cast<size_t>(1), batches.size());
    auto batch = batches[0];
    ASSERT_EQ(6, batch->GetRowCount());

    auto* lName = reinterpret_cast<VarcharVector*>(batch->Get(2));
    auto* rName = reinterpret_cast<VarcharVector*>(batch->Get(5));
    const std::vector<std::string> expectedLName = {"a", "a", "b", "b", "c", "c"};
    const std::vector<std::string> expectedRName = {"x", "y", "x", "y", "x", "y"};
    for (int i = 0; i < 6; i++) {
        EXPECT_FALSE(lName->IsNull(i)) << "L_name null at row " << i;
        EXPECT_FALSE(rName->IsNull(i)) << "R_name null at row " << i;
        EXPECT_EQ(expectedLName[i], std::string(lName->GetValue(i))) << "L_name mismatch at row " << i;
        EXPECT_EQ(expectedRName[i], std::string(rName->GetValue(i))) << "R_name mismatch at row " << i;
    }

    destroyLeftOuterJoinTestContext(ctx);
}
