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

#include "OmniSourceOperatorStreamTask.h"
#include "streaming/runtime/io/OmniStreamOneInputProcessor.h"
#include "OmniAsyncDataOutputToOutput.h"
#include "streaming/runtime/io/OmniStreamTaskSourceInput.h"

namespace omnistream {

const int DATASTREAM_TASK_TYPE = 2;

void omnistream::OmniSourceOperatorStreamTask::init()
{
    OmniStreamTask::init();

    auto output = createDataOutput();
    auto input = createTaskInput();

    inputProcessor_ = new OmniStreamOneInputProcessor(input, output, operatorChain.get());
}

void omnistream::OmniSourceOperatorStreamTask::processInput(MailboxDefaultAction::Controller* controller)
{
    OmniStreamTask::processInput(controller);
}

const std::string omnistream::OmniSourceOperatorStreamTask::getName() const
{
    return OmniStreamTask::getName();
}

OmniPushingAsyncDataInput::OmniDataOutput* OmniSourceOperatorStreamTask::createDataOutput()
{
    bool isDataStream = false;
    if (taskType == DATASTREAM_TASK_TYPE) {
        isDataStream = true;
    }
    return new OmniAsyncDataOutputToOutput(operatorChain->GetMainOperatorOutput(), isDataStream);
}

OmniStreamTaskInput* OmniSourceOperatorStreamTask::createTaskInput()
{
    return new OmniStreamTaskSourceInput(mainOperator_, 0, 0);
}

void OmniSourceOperatorStreamTask::cancel()
{
    OmniStreamTask::cancel();
}
} // namespace omnistream
