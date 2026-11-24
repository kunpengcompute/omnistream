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

#include "ChannelStatePendingResult.h"

namespace omnistream {

ChannelStatePendingResult::ChannelStatePendingResult(
    int subtaskIndex,
    int64_t checkpointId,
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> result,
    std::shared_ptr<ChannelStateSerializer> serializer)
    : subtaskIndex(subtaskIndex),
      checkpointId(checkpointId),
      result(result),
      serializer(serializer),
      allInputsReceived(false),
      allOutputsReceived(false)
{
}

bool ChannelStatePendingResult::IsAllInputsReceived() const
{
    return allInputsReceived;
}
bool ChannelStatePendingResult::IsAllOutputsReceived() const
{
    return allOutputsReceived;
}

std::map<InputChannelInfo, AbstractChannelStateHandle<InputChannelInfo>::StateContentMetaInfo>&
ChannelStatePendingResult::GetInputChannelOffsets()
{
    return inputChannelOffsets;
}

std::map<ResultSubpartitionInfoPOD, AbstractChannelStateHandle<ResultSubpartitionInfoPOD>::StateContentMetaInfo>&
ChannelStatePendingResult::GetResultSubpartitionOffsets()
{
    return resultSubpartitionOffsets;
}

void ChannelStatePendingResult::CompleteInput()
{
    if (allInputsReceived) {
        throw std::logic_error("Inputs already completed");
    }
    allInputsReceived = true;
}

void ChannelStatePendingResult::CompleteOutput()
{
    if (allOutputsReceived) {
        throw std::logic_error("Outputs already completed");
    }
    allOutputsReceived = true;
}

void ChannelStatePendingResult::FinishResult(std::shared_ptr<StreamStateHandle> stateHandle)
{
    NOT_IMPL_EXCEPTION;
}

void ChannelStatePendingResult::Fail(const std::exception_ptr& e)
{
    result->Fail(e);
}

bool ChannelStatePendingResult::IsDone() const
{
    return result->IsDone();
}

std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> ChannelStatePendingResult::GetResult()
{
    return result;
}

const int ChannelStatePendingResult::GetSubtaskIndex()
{
    return subtaskIndex;
}
const int64_t ChannelStatePendingResult::GetCheckpointId()
{
    return checkpointId;
}
} // namespace omnistream
