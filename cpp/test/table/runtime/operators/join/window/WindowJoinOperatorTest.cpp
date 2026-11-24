/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#include <gtest/gtest.h>
#include <memory>
#include "table/runtime/operators/join/window/WindowJoinOperator.h"
#include "table/runtime/operators/join/window/InnerJoinOperator.h"
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

namespace {
class IdleWindowJoinOutput : public OutputTestVectorBatch {
public:
    std::vector<int> statuses;
    void emitWatermarkStatus(WatermarkStatus* status) override
    {
        statuses.push_back(status->GetStatus());
    }
};

class IdleWindowJoinOperator : public InnerJoinOperator<int64_t> {
public:
    explicit IdleWindowJoinOperator(Output* output)
        : InnerJoinOperator<int64_t>(parsedJsonEqui, output, new LongSerializer(), new LongSerializer())
    {
        collector = static_cast<TimestampedCollector*>(this->output);
        combinedWatermark = new omnistream::IndexedCombinedWatermarkStatus(2);
    }
};
} // namespace

TEST(WindowJoinOperatorTest, IdleInputAdvancesCombinedWatermarkAndResumes)
{
    IdleWindowJoinOutput output;
    IdleWindowJoinOperator op(&output);
    TwoInputStreamOperator* input = &op;
    Watermark left(100);
    input->ProcessWatermark1(&left);
    EXPECT_EQ(output.getWatermark()->getTimestamp(), INT64_MIN);
    input->processWatermarkStatus2(WatermarkStatus::idle());
    EXPECT_EQ(output.getWatermark()->getTimestamp(), 100);
    EXPECT_TRUE(output.statuses.empty());
    input->processWatermarkStatus1(WatermarkStatus::idle());
    ASSERT_EQ(output.statuses.size(), 1);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::idleStatus);
    input->processWatermarkStatus2(WatermarkStatus::active());
    ASSERT_EQ(output.statuses.size(), 2);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::activeStatus);
    Watermark right(200);
    input->ProcessWatermark2(&right);
    EXPECT_EQ(output.getWatermark()->getTimestamp(), 200);
}

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

TEST(WindowJoinOperatorTest, DISABLED_LeftOuterJoinTest)
{
    auto vbatchLeft = getLeftBatch();
    auto vbatchRight = getRightBatch();

    auto* out = new OutputTestVectorBatch();
    auto op = new LeftOuterJoinOperator<int32_t>(parsedJsonEqui, out, new LongSerializer(), new LongSerializer());
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
    op->initializeState(initializer, new IntSerializer());
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

    Output Batch 3
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    2     , 1000         , 36   , null  , null         , null
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

    auto v31 = new omniruntime::vec::Vector<int32_t>(1);
    v31->SetValue(0, 2);
    auto v32 = new omniruntime::vec::Vector<int64_t>(1);
    v32->SetValue(0, 1000);
    auto v33 = new omniruntime::vec::Vector<int32_t>(1);
    v33->SetValue(0, 36);
    auto v34 = new omniruntime::vec::Vector<int32_t>(1);
    v34->SetNull(0);
    auto v35 = new omniruntime::vec::Vector<int64_t>(1);
    v35->SetNull(0);
    auto v36 = new omniruntime::vec::Vector<int32_t>(1);
    v36->SetNull(0);
    auto b3 = new omnistream::VectorBatch(1);
    b3->Append(v31);
    b3->Append(v32);
    b3->Append(v33);
    b3->Append(v34);
    b3->Append(v35);
    b3->Append(v36);

    EXPECT_TRUE(inOutput(b3, out));

    op->close();
    delete op;
    delete initializer;
    delete out;
    delete b1;
    delete b2;
    delete b3;
}

TEST(WindowJoinOperatorTest, DISABLED_RightOuterJoinTest)
{
    auto vbatchLeft = getLeftBatch();
    auto vbatchRight = getRightBatch();

    auto* out = new OutputTestVectorBatch();
    auto op = new RightOuterJoinOperator<int32_t>(parsedJsonEqui, out, new LongSerializer(), new LongSerializer());
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

    Output Batch 4
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    null  , null         , null , 3     , 1000         , 400
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

    auto v31 = new omniruntime::vec::Vector<int32_t>(1);
    v31->SetNull(0);
    auto v32 = new omniruntime::vec::Vector<int64_t>(1);
    v32->SetNull(0);
    auto v33 = new omniruntime::vec::Vector<int32_t>(1);
    v33->SetNull(0);
    auto v34 = new omniruntime::vec::Vector<int32_t>(1);
    v34->SetValue(0, 3);
    auto v35 = new omniruntime::vec::Vector<int64_t>(1);
    v35->SetValue(0, 1000);
    auto v36 = new omniruntime::vec::Vector<int32_t>(1);
    v36->SetValue(0, 400);
    auto b3 = new omnistream::VectorBatch(1);
    b3->Append(v31);
    b3->Append(v32);
    b3->Append(v33);
    b3->Append(v34);
    b3->Append(v35);
    b3->Append(v36);

    EXPECT_TRUE(inOutput(b3, out));

    op->close();
    delete op;
    delete initializer;
    delete out;
    delete b1;
    delete b2;
    delete b3;
}

TEST(WindowJoinOperatorTest, DISABLED_FullOuterJoinTest)
{
    auto vbatchLeft = getLeftBatch();
    auto vbatchRight = getRightBatch();

    auto* out = new OutputTestVectorBatch();
    auto op = new FullOuterJoinOperator<int32_t>(parsedJsonEqui, out, new LongSerializer(), new LongSerializer());
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

    Output Batch 3
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    2     , 1000         , 36   , null  , null         , null

    Output Batch 3
    (int) , (long)       , (int), (int) , (long)       , (int)
    KeyCol, WindowEndTime, value, KeyCol, WindowEndTime, value
    null  , null         , null , 3     , 1000         , 400
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

    auto v31 = new omniruntime::vec::Vector<int32_t>(1);
    v31->SetValue(0, 2);
    auto v32 = new omniruntime::vec::Vector<int64_t>(1);
    v32->SetValue(0, 1000);
    auto v33 = new omniruntime::vec::Vector<int32_t>(1);
    v33->SetValue(0, 36);
    auto v34 = new omniruntime::vec::Vector<int32_t>(1);
    v34->SetNull(0);
    auto v35 = new omniruntime::vec::Vector<int64_t>(1);
    v35->SetNull(0);
    auto v36 = new omniruntime::vec::Vector<int32_t>(1);
    v36->SetNull(0);
    auto b3 = new omnistream::VectorBatch(1);
    b3->Append(v31);
    b3->Append(v32);
    b3->Append(v33);
    b3->Append(v34);
    b3->Append(v35);
    b3->Append(v36);

    EXPECT_TRUE(inOutput(b3, out));

    auto v41 = new omniruntime::vec::Vector<int32_t>(1);
    v41->SetNull(0);
    auto v42 = new omniruntime::vec::Vector<int64_t>(1);
    v42->SetNull(0);
    auto v43 = new omniruntime::vec::Vector<int32_t>(1);
    v43->SetNull(0);
    auto v44 = new omniruntime::vec::Vector<int32_t>(1);
    v44->SetValue(0, 3);
    auto v45 = new omniruntime::vec::Vector<int64_t>(1);
    v45->SetValue(0, 1000);
    auto v46 = new omniruntime::vec::Vector<int32_t>(1);
    v46->SetValue(0, 400);
    auto b4 = new omnistream::VectorBatch(1);
    b4->Append(v41);
    b4->Append(v42);
    b4->Append(v43);
    b4->Append(v44);
    b4->Append(v45);
    b4->Append(v46);

    EXPECT_TRUE(inOutput(b4, out));

    op->close();
    delete op;
    delete initializer;
    delete out;
    delete b1;
    delete b2;
    delete b3;
    delete b4;
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
    delete b1;
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
