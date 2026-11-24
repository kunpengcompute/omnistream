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

#ifndef RECORDWRITERBUILDERV2_H
#define RECORDWRITERBUILDERV2_H
#include <streaming/runtime/partitioner/V2/ChannelSelectorV2.h>
#include <partition/ResultPartitionWriter.h>
#include <streaming/runtime/streamrecord/StreamRecord.h>

#include "RecordWriterV2.h"

#include <string>
#include <streaming/runtime/partitioner/StreamPartitioner.h>

namespace omnistream {

class RecordWriterBuilderV2 {
public:
    RecordWriterBuilderV2();

    RecordWriterBuilderV2& withWriter(std::shared_ptr<ResultPartitionWriter> writer);
    RecordWriterBuilderV2& withChannelSelector(ChannelSelectorV2<StreamRecord>* channelSelector);
    RecordWriterBuilderV2& withChannelSelector(datastream::StreamPartitioner<IOReadableWritable>* channelSelector);
    RecordWriterBuilderV2& withTimeout(long timeout);
    RecordWriterBuilderV2& withJobType(int jobType);
    RecordWriterBuilderV2& withTaskName(const std::string& taskName);

    RecordWriterV2* build();

private:
    // 1 native sql task, 2 native datastream task. 3 future - hybrid java+cpp source task
    int jobType;
    std::shared_ptr<ResultPartitionWriter> writer_ = nullptr;
    ChannelSelectorV2<StreamRecord>* channelSelector_ = nullptr;
    datastream::StreamPartitioner<IOReadableWritable>* channelSelector = nullptr;
    long timeout_ = 100;
    std::string taskName_ = "";
};

} // namespace omnistream

#endif // RECORDWRITERBUILDERV2_H
