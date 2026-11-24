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

#include "OmniStreamOneInputProcessor.h"
#include "io/recover/OmniRescalingStreamTaskNetworkInput.h"

namespace omnistream {
OmniStreamOneInputProcessor::OmniStreamOneInputProcessor(
    OmniStreamTaskInput* input, OmniStreamTaskInput::OmniDataOutput* output, OperatorChainV2* operatorChain)
    : input(input),
      output(output),
      endOfInputAware(operatorChain)
{
}

OmniStreamOneInputProcessor::~OmniStreamOneInputProcessor()
{
    if (input != nullptr) {
        delete input;
        input = nullptr;
    }
    if (output != nullptr) {
        delete output;
        output = nullptr;
    }
}

DataInputStatus OmniStreamOneInputProcessor::processInput()
{
    // LOG(">>>process Input")
    DataInputStatus status = input->emitNext(output);
    if (status == DataInputStatus::END_OF_RECOVERY) {
        auto recoverInput = dynamic_cast<OmniRescalingStreamTaskNetworkInput*>(input);
        if (recoverInput) {
            input = recoverInput->finishRecover();
        }
        return DataInputStatus::MORE_AVAILABLE;
    }
    LOG("emitNext return status: " << DataInputStatusHelper::mapToInt(status));
    return status;
}

std::shared_ptr<CompletableFuture> OmniStreamOneInputProcessor::GetAvailableFuture()
{
    return input->GetAvailableFuture();
}

OmniStreamTaskInput* OmniStreamOneInputProcessor::GetInput()
{
    return input;
}

std::shared_ptr<CompletableFutureV2<void>> OmniStreamOneInputProcessor::PrepareSnapshot(
    std::shared_ptr<ChannelStateWriter> writer, long checkpointID)
{
    LOG("OneInput prepare snapshot, checkpointID: " << checkpointID);
    return input->PrepareSnapshot(writer, checkpointID);
}

void OmniStreamOneInputProcessor::close()
{
    if (input != nullptr) {
        input->close();
    }
    if (output != nullptr) {
        output->close();
    }
}
} // namespace omnistream
