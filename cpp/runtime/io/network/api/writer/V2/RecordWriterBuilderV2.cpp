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

#include "RecordWriterBuilderV2.h"

#include "SimpleSelectorRecordWriterV2.h"

namespace omnistream {

RecordWriterBuilderV2::RecordWriterBuilderV2()
{
}

RecordWriterBuilderV2& RecordWriterBuilderV2::withWriter(std::shared_ptr<ResultPartitionWriter> writer)
{
    writer_ = writer;
    return *this;
}

RecordWriterBuilderV2& RecordWriterBuilderV2::withChannelSelector(ChannelSelectorV2<StreamRecord>* channelSelector)
{
    channelSelector_ = channelSelector;
    return *this;
}

RecordWriterBuilderV2& RecordWriterBuilderV2::withChannelSelector(
    datastream::StreamPartitioner<IOReadableWritable>* channelSelector)
{
    this->channelSelector = channelSelector;
    return *this;
}

RecordWriterBuilderV2& RecordWriterBuilderV2::withJobType(int jobType)
{
    this->jobType = jobType;
    return *this;
}

RecordWriterBuilderV2& RecordWriterBuilderV2::withTimeout(long timeout)
{
    timeout_ = timeout;
    return *this;
}

RecordWriterBuilderV2& RecordWriterBuilderV2::withTaskName(const std::string& taskName)
{
    taskName_ = taskName;
    return *this;
}

RecordWriterV2* RecordWriterBuilderV2::build()
{
    return new SimpleSelectorRecordWriterV2(writer_, channelSelector_, channelSelector, timeout_, taskName_, jobType);
}

} // namespace omnistream
