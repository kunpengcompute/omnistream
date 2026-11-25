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

#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/heap/StateTable.h"
#include "core/typeutils/LongSerializer.h"
#include "OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "runtime/state/VoidNamespace.h"
#include "table/data/binary/BinaryRowData.h"
#include "runtime/state/TaskStateManager.h"

#include <gtest/gtest.h>

TEST(AbstractStreamOperatorTest, InitTest)
{
    AbstractStreamOperator<int>* op = new AbstractStreamOperator<int>();
    op->setup();
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
    env2->setTaskConfiguration(*taskInfo);
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    ASSERT_NO_THROW((op->setOutput(new OutputTest())));
    ASSERT_NO_THROW((op->open()));
    ASSERT_NO_THROW((op->initializeState(new StreamTaskStateInitializerImpl(env2), new IntSerializer())));
    ASSERT_NO_THROW((op->setCurrentKey(1)));
}

TEST(AbstractStreamOperatorTest, setAndGetCurrentKey)
{
    AbstractStreamOperator<int>* op = new AbstractStreamOperator<int>();
    op->setup();
    op->open();
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
    env2->setTaskConfiguration(*taskInfo);
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    op->initializeState(new StreamTaskStateInitializerImpl(env2), new IntSerializer());

    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setInt(0, 2L);
    row->setInt(1, 4L);
    // row->getInt(0) is the stateKeySelector here
    op->setCurrentKey(*row->getInt(0));

    int key = op->getCurrentKey();

    ASSERT_EQ(2L, key);
}

TEST(AbstractStreamOperatorTest, compositeKeys)
{
    AbstractStreamOperator<RowData*>* op = new AbstractStreamOperator<RowData*>();
    op->setup();
    op->open();
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
    env2->setTaskConfiguration(*taskInfo);
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    op->initializeState(new StreamTaskStateInitializerImpl(env2), new IntSerializer());

    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setInt(0, 8);
    row->setInt(1, 12);
    op->setCurrentKey(dynamic_cast<RowData*>(row));

    BinaryRowData* key = dynamic_cast<BinaryRowData*>(op->getCurrentKey());
    ASSERT_EQ(8, *key->getInt(0));
    ASSERT_EQ(12, *key->getInt(1));
}
