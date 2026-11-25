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

#ifndef SIMPLESELECTORRECORDWRITERV2_H
#define SIMPLESELECTORRECORDWRITERV2_H

#include <core/utils/ByteBuffer.h>
#include <streaming/runtime/partitioner/StreamPartitioner.h>
#include <streaming/runtime/streamrecord/StreamRecord.h>

#include <streaming/runtime/partitioner/V2/ChannelSelectorV2.h>
#include <runtime/plugable/SerializationDelegate.h>

#include "RecordWriterV2.h"
#include "table/data/vectorbatch/VectorBatch.h"

namespace omnistream {
class SimpleSelectorRecordWriterV2 : public RecordWriterV2 {
public:
    SimpleSelectorRecordWriterV2(
        std::shared_ptr<ResultPartitionWriter> writer, // Raw pointer
        ChannelSelectorV2<StreamRecord>* channelSelector,
        datastream::StreamPartitioner<IOReadableWritable>* partitioner,
        long timeout,
        const std::string& taskName,
        int taskType = 0);

    ~SimpleSelectorRecordWriterV2() override
    {
        delete serializer;
    }

    void emit(StreamRecord* record) override;
    ByteBuffer* serializeRecord(DataOutputSerializer* serializer, IOReadableWritable* record);

    void broadcastEmit(Watermark* watermark) override;
    void broadcastEmit(WatermarkStatus* status) override;

protected:
    void emit(StreamRecord* record, int targetSubpartition) override;

private:
    ChannelSelectorV2<StreamRecord>* channelSelector;               // Raw pointer
    datastream::StreamPartitioner<IOReadableWritable>* partitioner; // Raw pointer
    DataOutputSerializer* serializer = nullptr;
};
} // namespace omnistream

#endif
