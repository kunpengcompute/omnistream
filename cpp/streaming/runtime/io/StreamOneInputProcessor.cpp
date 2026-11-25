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

#include "streaming/runtime/tasks/StreamTask.h"
#include "StreamOneInputProcessor.h"

namespace omnistream::datastream {
StreamOneInputProcessor::StreamOneInputProcessor(
    std::unique_ptr<StreamTaskNetworkOutput> streamTaskOutput, std::unique_ptr<StreamTaskNetworkInput> streamTaskInput)
    : streamTaskOutput(std::move(streamTaskOutput)),
      streamTaskInput(std::move(streamTaskInput))
{
}

StreamOneInputProcessor::~StreamOneInputProcessor()
{
}

int StreamOneInputProcessor::processInput(
    const uint8_t* input_buffer, size_t input_size, long channelInfo, int32_t& inputNumber)
{
    this->streamTaskInput->emitNextProcessBuffer(input_buffer, input_size, channelInfo);
    int status = this->streamTaskInput->emitNextProcessElement(*streamTaskOutput, inputNumber);
    return status;
}
} // namespace omnistream::datastream
