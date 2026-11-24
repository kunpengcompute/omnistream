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

#include "RecordWriter.h"
#include "runtime/plugable/SerializationDelegate.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

namespace omnistream::datastream {
RecordWriter::RecordWriter(uint8_t* address, int32_t capacity, StreamPartitioner<IOReadableWritable>* streamPartitioner)
    : streamPartitioner(streamPartitioner)
{
    serializer_.setBackendBuffer(address, capacity);
}

RecordWriter::RecordWriter(
    OutputBufferStatus* outputBufferStatus, StreamPartitioner<IOReadableWritable>* streamPartitioner)
    : streamPartitioner(streamPartitioner)
{
    serializer_.setBackendBuffer(outputBufferStatus);
}

RecordWriter::~RecordWriter()
{
    delete streamPartitioner;
}

void RecordWriter::clear()
{
    serializer_.clear();
    counter_ = 0;
}

void RecordWriter::emit(IOReadableWritable* record)
{
    const int channel = streamPartitioner->selectChannel(record);
    serializeRecord(serializer_, record, channel);
    counter_++;
}

void RecordWriter::serializeRecord(DataOutputSerializer& serializer, IOReadableWritable* record, int channel)
{
    int currentPos = serializer.length();
    // LOG(">>>> currentPos " + std::to_string(currentPos))
    serializer.setPositionUnsafe(4 + currentPos);

    // write data
    record->write(serializer);

    // write length
    serializer.writeIntUnsafe(serializer.length() - 4 - currentPos, currentPos);

    // write partition channel info at the end of each element
    serializer.writeInt(channel);
}

int RecordWriter::getCounter() const
{
    return counter_;
}

void RecordWriter::setCounter(int counter)
{
    counter_ = counter;
}

int32_t RecordWriter::getLength() const
{
    return serializer_.length();
}
} // namespace omnistream::datastream
