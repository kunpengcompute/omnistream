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

#include "streaming/api/operators/StreamOperatorStateHandler.h"
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/execution/Environment.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "runtime/state/TaskStateManager.h"

#include <gtest/gtest.h>

TEST(StreamOperatorStateHandlerTest, Init)
{
    auto env = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl initializer(env);
    StreamOperatorStateContextImpl<int>* context =
        initializer.streamOperatorStateContext<int>(new IntSerializer(), nullptr, nullptr);
    ASSERT_NO_THROW(context->keyedStateBackend());
    delete context;
    delete taskInfo;
    delete env;
}
