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

#include "SimpleSelectorRecordWriterV2.h"

#include <streaming/runtime/streamrecord/StreamElementSerializer.h>
namespace omnistream {
SimpleSelectorRecordWriterV2::SimpleSelectorRecordWriterV2(
    std::shared_ptr<ResultPartitionWriter> writer,
    ChannelSelectorV2<StreamRecord>* channelSelector,
    datastream::StreamPartitioner<IOReadableWritable>* partitioner,
    long timeout,
    const std::string& taskName,
    int taskType)
    : RecordWriterV2(writer, timeout, taskName, taskType)
{
    this->channelSelector = channelSelector; // for sql
    this->partitioner = partitioner;         // for datastream, it needs to be combined

    if (this->channelSelector != nullptr) {
        // check null before use
        this->channelSelector->setup(numberOfChannels);
    } else if (this->partitioner != nullptr) {
        this->partitioner->setup(numberOfChannels);
    } else {
        LOG("Error: ChannelSelector is null in ChannelSelectorRecordWriter constructor." << std::endl);
    }

    if (taskType == 2) {
        this->serializer = new DataOutputSerializer(128);
    }
}

void SimpleSelectorRecordWriterV2::emit(StreamRecord* record)
{
    if (taskType == 1) {
        if (record->getValueType() == RecordValueType::VECTOR_BATCH) {
            auto vectorBatch = static_cast<VectorBatch*>(record->getValue());
            size_t row_cnt = vectorBatch->GetRowCount();
            counter_ += row_cnt;
        } else {
            counter_++;
        }

        if (channelSelector != nullptr) {
            auto selectedChannel = channelSelector->selectChannel(record);

            // LOG(">>> before emit " << std::to_string(selectedChannel) << "  record is " <<
            // reinterpret_cast<long>(record))
            for (auto it = selectedChannel.begin(); it != selectedChannel.end(); ++it) {
                emit(it->second, it->first);
            }
        } else {
            std::cerr << "Error: ChannelSelector is null in ChannelSelectorRecordWriter::emit." << std::endl;
        }
    } else if (taskType == 2) {
        auto* value = static_cast<Object*>(record->getValue());
        this->serializationDelegate->setInstance(record);
        int channel = partitioner->selectChannel(serializationDelegate);

        auto byteBuffer = serializeRecord(serializer, serializationDelegate);

        record->setValue(byteBuffer);
        emit(record, channel);
        /*if (byteBuffer) {
            delete byteBuffer;
        }*/
        if (value) {
            value->putRefCount();
        }
    }
}

ByteBuffer* SimpleSelectorRecordWriterV2::serializeRecord(DataOutputSerializer* serializer, IOReadableWritable* record)
{
    // correspond to original flink logic for we will put it into memory segment
    serializer->setPositionUnsafe(4);

    record->write(*serializer);

    int length = serializer->length() - 4;
    serializer->writeIntUnsafe(length, 0);

    return serializer->wrapAsByteBuffer();
}

void SimpleSelectorRecordWriterV2::emit(StreamRecord* record, int targetSubpartitionIndex)
{
    LOG(">>> before emit  targetSubpartition " << std::to_string(targetSubpartitionIndex) << "  record is "
                                               << reinterpret_cast<long>(record));
    this->targetPartitionWriter_->emitRecord(record, targetSubpartitionIndex);
    if (flushAlways) {
        targetPartitionWriter_->flush(targetSubpartitionIndex);
    }
}

void SimpleSelectorRecordWriterV2::broadcastEmit(Watermark* watermark)
{
    if (taskType == 2) {
        this->serializationDelegate->setInstance(watermark);
        auto byteBuffer = serializeRecord(serializer, serializationDelegate);
        watermark->setValue(byteBuffer);
    }
    LOG(">>> before broadcast watermark " << std::to_string(watermark->getTimestamp()));
    for (int i = 0; i < numberOfChannels; i++) {
        auto* new_wm = new Watermark(*watermark);
        this->targetPartitionWriter_->emitRecord(reinterpret_cast<void*>(new_wm), i);
    }
    if (flushAlways) {
        targetPartitionWriter_->flushAll();
    }
}
void SimpleSelectorRecordWriterV2::broadcastEmit(WatermarkStatus* status)
{
    if (taskType == 2) {
        serializationDelegate->setInstance(status);
        status->setValue(serializeRecord(serializer, serializationDelegate));
    }
    for (int i = 0; i < numberOfChannels; ++i) {
        targetPartitionWriter_->emitRecord(new WatermarkStatus(*status), i);
    }
    if (flushAlways) {
        targetPartitionWriter_->flushAll();
    }
}
} // namespace omnistream
