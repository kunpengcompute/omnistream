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

#include "EventSerializer.h"

#include "io/network/api/EventAnnouncement.h"

#include <vector>
#include <cstring>
#include <buffer/EventBuffer.h>
#include <buffer/NetworkBuffer.h>
#include <event/EndOfData.h>
#include <event/EndOfPartitionEvent.h>
#include <memory/MemorySegment.h>
#include <buffer/EventDataBufferRecycler.h>
#include <memory/MemorySegmentFactory.h>

#include "buffer/MemoryBufferConsumer.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/event/EndOfChannelStateEvent.h"
#include "runtime/event/EndOfSegmentEvent.h"
#include "runtime/io/network/api/CancelCheckpointMarker.h"
#include "runtime/event/SubtaskConnectionDescriptor.h"
#include "event/InnerRecoverEvent.h"

namespace omnistream {
const int EventSerializer::INVALID_EVENT = -1;
const int EventSerializer::END_OF_PARTITION_EVENT = 0;
const int EventSerializer::CHECKPOINT_BARRIER_EVENT = 1;
const int EventSerializer::END_OF_SUPERSTEP_EVENT = 2;
const int EventSerializer::OTHER_EVENT = 3;
const int EventSerializer::CANCEL_CHECKPOINT_MARKER_EVENT = 4;
const int EventSerializer::END_OF_CHANNEL_STATE_EVENT = 5;
const int EventSerializer::ANNOUNCEMENT_EVENT = 6;
const int EventSerializer::VIRTUAL_CHANNEL_SELECTOR_EVENT = 7;
const int EventSerializer::END_OF_USER_RECORDS_EVENT = 8;
const int EventSerializer::INNER_RECOVER_EVENT = 9;

datastream::NetworkBuffer* EventSerializer::toBuffer(std::shared_ptr<AbstractEvent> event, bool hasPriority)
{
    MemorySegment* res = ToSerializedEvent(event);
    if (res == nullptr) {
        return nullptr;
    }
    ObjectBufferDataType dataType = ObjectBufferDataType::GetDataBufferType(hasPriority, event);
    NetworkBuffer* networkBuffer =
        new NetworkBuffer(res, res->getSize(), 0, EventDataBufferRecycler::GetInstance(), dataType, true);
    networkBuffer->SetReaderIndex(0);

    return networkBuffer;
}

std::shared_ptr<BufferConsumer> EventSerializer::ToBufferConsumer(
    std::shared_ptr<AbstractEvent> event, bool hasPriority)
{
    NetworkBuffer* buffer = toBuffer(event, hasPriority);
    if (buffer == nullptr) {
        return nullptr;
    }
    int eventSize = buffer->getMemorySegment()->getSize();
    std::shared_ptr<BufferConsumer> bufferConsumer =
        std::make_shared<datastream::MemoryBufferConsumer>(buffer, eventSize);

    return bufferConsumer;
}

std::shared_ptr<AbstractEvent> EventSerializer::fromBuffer(Buffer* buffer)
{
    return fromSerializedEvent(buffer);
}

std::shared_ptr<AbstractEvent> EventSerializer::fromBufferNotRecycle(Buffer* buffer)
{
    return fromSerializedEvent(buffer, false);
}

std::shared_ptr<AbstractEvent> EventSerializer::fromBuffer_V2(const std::shared_ptr<Buffer>& buffer)
{
    return fromSerializedEvent_V2(buffer);
}

MemorySegment* EventSerializer::ToSerializedEvent(std::shared_ptr<AbstractEvent> event)
{
    MemorySegment* memorySegment = nullptr;
    uint8_t* data = nullptr;
    if (dynamic_cast<EndOfPartitionEvent*>(event.get())) {
        data = new uint8_t[4]{0, 0, 0, END_OF_PARTITION_EVENT};
        memorySegment = new MemorySegment(data, 4);
        return memorySegment;
    } else if (dynamic_cast<EndOfData*>(event.get())) {
        EndOfData* endEvent = dynamic_cast<EndOfData*>(event.get());
        uint8_t ordinal = static_cast<int>(endEvent->getStopMode());
        data = new uint8_t[5]{0, 0, 0, END_OF_USER_RECORDS_EVENT, ordinal};
        memorySegment = new MemorySegment(data, 5);
        return memorySegment;
    } else if (dynamic_cast<CheckpointBarrier*>(event.get())) {
        memorySegment = SerializeCheckpointBarrier(std::dynamic_pointer_cast<CheckpointBarrier>(event));
        return memorySegment;
    } else if (auto cancelMarker = dynamic_cast<CancelCheckpointMarker*>(event.get())) {
        auto segment = std::make_unique<MemorySegment>(12);
        ByteBuffer byteBuffer(segment->getData(), segment->getSize());
        byteBuffer.putInt(CANCEL_CHECKPOINT_MARKER_EVENT);
        byteBuffer.putLong(cancelMarker->getCheckpointId());
        return segment.release();
    } else if (dynamic_cast<EventAnnouncement*>(event.get())) {
        auto ann = std::dynamic_pointer_cast<EventAnnouncement>(event);
        if (!ann) {
            throw std::runtime_error("Failed to cast event to EventAnnouncement.");
        }

        // Serialize the announced event (currently we only support announced CheckpointBarrier).
        std::unique_ptr<MemorySegment> announcedSeg(ToSerializedEvent(ann->GetAnnouncedEvent()));
        int byteSize = 4 /*type*/ + 4 /*sequenceNumber*/ + announcedSeg->getSize();

        ByteBuffer byteBuffer = ByteBuffer(byteSize);

        byteBuffer.putInt(ANNOUNCEMENT_EVENT);
        byteBuffer.putInt(ann->GetSequenceNumber());
        byteBuffer.putBytes(announcedSeg->getData(), announcedSeg->getSize());

        uint8_t* arr = new uint8_t[byteSize];
        memcpy_s(arr, byteSize, byteBuffer.getValue(), byteSize);
        memorySegment = new MemorySegment(arr, byteSize);
        return memorySegment;

    } else if (dynamic_cast<EndOfChannelStateEvent*>(event.get())) {
        data = new uint8_t[4]{0, 0, 0, END_OF_CHANNEL_STATE_EVENT};
        memorySegment = new MemorySegment(data, 4);
        return memorySegment;
    } else if (dynamic_cast<InnerRecoverEvent*>(event.get())) {
        data = new uint8_t[4]{0, 0, 0, INNER_RECOVER_EVENT};
        memorySegment = new MemorySegment(data, 4);
        return memorySegment;
    } else if (dynamic_cast<SubtaskConnectionDescriptor*>(event.get())) {
        auto selector = dynamic_cast<SubtaskConnectionDescriptor*>(event.get());
        ByteBuffer byteBuffer = ByteBuffer(12);
        byteBuffer.putInt(VIRTUAL_CHANNEL_SELECTOR_EVENT);
        byteBuffer.putInt(selector->getInputSubtaskIndex());
        byteBuffer.putInt(selector->getOutputSubtaskIndex());
        byteBuffer.flip();
        uint8_t* arr = new uint8_t[12];
        memcpy_s(arr, 12, byteBuffer.getValue(), 12);
        memorySegment = new MemorySegment(arr, 12);
        return memorySegment;
    }
    throw std::runtime_error("Unsupported event type");
}

std::shared_ptr<AbstractEvent> EventSerializer::fromSerializedEvent(Buffer* buffer, bool recycleEvent)
{
    LOG_DEBUG("fromSerializedEvent V1 !");
    if (buffer == nullptr || buffer->GetSize() < 4) {
        throw std::runtime_error("Buffer is null or too small to contain an event");
    }

    auto networkBuffer = dynamic_cast<datastream::NetworkBuffer*>(buffer);
    if (!networkBuffer) {
        LOG_DEBUG("find a cast error!");
        throw std::runtime_error("it is not netwokrk buffer, so it can not be converted to event.");
    }

    uint8_t* rawData = networkBuffer->getMemorySegment()->getData();
    ByteBuffer byteBuffer = ByteBuffer(rawData, networkBuffer->GetSize());
    int eventType = byteBuffer.getIntFromValue();
    if (eventType == END_OF_PARTITION_EVENT) {
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return EndOfPartitionEvent::getInstance();
    } else if (eventType == END_OF_USER_RECORDS_EVENT) {
        auto stopMode = static_cast<StopMode>(byteBuffer.getByte());
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return std::make_shared<EndOfData>(stopMode);
    } else if (eventType == CHECKPOINT_BARRIER_EVENT) {
        std::shared_ptr<CheckpointBarrier> checkpointBarrier = DeserializeCheckpointBarrier(byteBuffer);
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return checkpointBarrier;
    } else if (eventType == ANNOUNCEMENT_EVENT) {
        int seq = byteBuffer.getIntFromValue();
        int announcedType = byteBuffer.getIntFromValue();

        std::shared_ptr<AbstractEvent> announced;
        if (announcedType == CHECKPOINT_BARRIER_EVENT) {
            announced = DeserializeCheckpointBarrier(byteBuffer);
        } else {
            throw std::runtime_error("Unsupported announced event type in EventAnnouncement.");
        }
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return std::make_shared<EventAnnouncement>(announced, seq);
    } else if (eventType == CANCEL_CHECKPOINT_MARKER_EVENT) {
        auto checkpointId = byteBuffer.getLong();
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return std::make_shared<CancelCheckpointMarker>(checkpointId);
    } else if (eventType == END_OF_CHANNEL_STATE_EVENT) {
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return EndOfChannelStateEvent::getInstance();
    } else if (eventType == VIRTUAL_CHANNEL_SELECTOR_EVENT) {
        auto des =
            std::make_shared<SubtaskConnectionDescriptor>(byteBuffer.getIntFromValue(), byteBuffer.getIntFromValue());
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return des;
    } else if (eventType == INNER_RECOVER_EVENT) {
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return InnerRecoverEvent::getInstance();
    } else {
        if (recycleEvent) {
            buffer->RecycleBuffer();
        }
        return nullptr;
    }
}

std::shared_ptr<AbstractEvent> EventSerializer::fromSerializedEvent_V2(std::shared_ptr<Buffer> buffer)
{
    LOG_DEBUG("fromSerializedEvent V2 !");
    if (buffer == nullptr || buffer->GetSize() < 4) {
        throw std::runtime_error("Buffer is null or too small to contain an event");
    }

    auto networkBuffer = std::dynamic_pointer_cast<datastream::NetworkBuffer>(buffer);
    if (!networkBuffer) {
        LOG_DEBUG("find a cast error!");
        throw std::runtime_error("it is not netwokrk buffer, so it can not be converted to event.");
    }
    uint8_t* rawData = networkBuffer->getMemorySegment()->getData();
    ByteBuffer byteBuffer = ByteBuffer(rawData, networkBuffer->GetSize());
    int eventType = byteBuffer.getIntFromValue();
    if (eventType == END_OF_PARTITION_EVENT) {
        //            buffer->RecycleBuffer();
        return EndOfPartitionEvent::getInstance();
    } else if (eventType == END_OF_USER_RECORDS_EVENT) {
        auto stopMode = static_cast<StopMode>(byteBuffer.getByte());
        //            buffer->RecycleBuffer();
        return std::make_shared<EndOfData>(stopMode);
    } else if (eventType == CHECKPOINT_BARRIER_EVENT) {
        std::shared_ptr<CheckpointBarrier> checkpointBarrier = DeserializeCheckpointBarrier(byteBuffer);
        //            buffer->RecycleBuffer();
        return checkpointBarrier;
    } else if (eventType == ANNOUNCEMENT_EVENT) {
        int seq = byteBuffer.getIntFromValue();
        int announcedType = byteBuffer.getIntFromValue();

        std::shared_ptr<AbstractEvent> announced;
        if (announcedType == CHECKPOINT_BARRIER_EVENT) {
            announced = DeserializeCheckpointBarrier(byteBuffer);
        } else {
            throw std::runtime_error("Unsupported announced event type in EventAnnouncement.");
        }
        //            buffer->RecycleBuffer();
        return std::make_shared<EventAnnouncement>(announced, seq);
    } else if (eventType == CANCEL_CHECKPOINT_MARKER_EVENT) {
        auto checkpointId = byteBuffer.getLong();
        //            buffer->RecycleBuffer();
        return std::make_shared<CancelCheckpointMarker>(checkpointId);
    } else {
        LOG_DEBUG("find no support event type!");
        return nullptr;
    }
}

MemorySegment* EventSerializer::SerializeCheckpointBarrier(std::shared_ptr<CheckpointBarrier> checkpointBarrier)
{
    int byteSize = 38;
    auto checkpointOptions = checkpointBarrier->GetCheckpointOptions();
    std::shared_ptr<std::vector<uint8_t>> reference = checkpointOptions->GetTargetLocation()->IsDefaultReference()
                                                          ? nullptr
                                                          : checkpointOptions->GetTargetLocation()->GetReferenceBytes();
    if (reference) {
        byteSize += reference->size();
    }
    ByteBuffer byteBuffer = ByteBuffer(byteSize);
    byteBuffer.putInt(CHECKPOINT_BARRIER_EVENT);
    byteBuffer.putLong(checkpointBarrier->GetId());
    byteBuffer.putLong(checkpointBarrier->GetTimestamp());
    SnapshotType* snapshotType = checkpointOptions->GetCheckpointType();
    if (snapshotType->IsSavepoint()) {
        SavepointType* savepointType = dynamic_cast<SavepointType*>(snapshotType);
        EncodeSavepointType(savepointType, byteBuffer);
    } else if (*snapshotType == *(CheckpointType::CHECKPOINT)) {
        byteBuffer.putByte(CHECKPOINT_TYPE_CHECKPOINT);
    } else if (*snapshotType == *(CheckpointType::FULL_CHECKPOINT)) {
        byteBuffer.putByte(CHECKPOINT_TYPE_FULL_CHECKPOINT);
    } else {
        throw std::runtime_error("Unknown checkpoint type.");
    }
    if (reference == nullptr) {
        byteBuffer.putInt(-1);
    } else {
        byteBuffer.putInt(reference->size());
        byteBuffer.putBytes(reference->data(), reference->size());
    }
    uint8_t alignmentOrdinal = static_cast<uint8_t>(checkpointOptions->GetAlignment());
    byteBuffer.putByte(alignmentOrdinal);
    byteBuffer.putLong(checkpointOptions->GetAlignedCheckpointTimeout());
    uint8_t* arr = new uint8_t[byteSize];
    memcpy_s(arr, byteSize, byteBuffer.getValue(), byteSize);
    return new MemorySegment(arr, byteSize);
}

void EventSerializer::EncodeSavepointType(SavepointType* savepointType, ByteBuffer& byteBuffer)
{
    switch (savepointType->getPostCheckpointAction()) {
        case SavepointType::PostCheckpointAction::NONE: byteBuffer.putByte(CHECKPOINT_TYPE_SAVEPOINT); break;
        case SavepointType::PostCheckpointAction::SUSPEND: byteBuffer.putByte(CHECKPOINT_TYPE_SAVEPOINT_SUSPEND); break;
        case SavepointType::PostCheckpointAction::TERMINATE:
            byteBuffer.putByte(CHECKPOINT_TYPE_SAVEPOINT_TERMINATE);
            break;
        default: throw std::runtime_error("Unknown savepoint type");
    }
    switch (savepointType->getFormatType()) {
        case SavepointFormatType::CANONICAL: byteBuffer.putByte(SAVEPOINT_FORMAT_CANONICAL); break;
        case SavepointFormatType::NATIVE: byteBuffer.putByte(SAVEPOINT_FORMAT_NATIVE); break;
        case SavepointFormatType::COMPATIBLE: byteBuffer.putByte(SAVEPOINT_FORMAT_COMPATIBLE); break;
        default: throw std::runtime_error("Unknown savepoint format type");
    }
}

std::shared_ptr<CheckpointBarrier> EventSerializer::DeserializeCheckpointBarrier(ByteBuffer& buffer)
{
    // Read id and timestamp
    int64_t id = buffer.getLong();
    int64_t timestamp = buffer.getLong();
    // Read the checkpoint type code
    uint8_t checkpointTypeCode = buffer.getByte();
    // Determine the snapshot type based on the checkpoint type code
    std::shared_ptr<SnapshotType> snapshotType = nullptr;
    if (checkpointTypeCode == CHECKPOINT_TYPE_CHECKPOINT) {
        snapshotType = CheckpointType::CHECKPOINT;
    } else if (checkpointTypeCode == CHECKPOINT_TYPE_FULL_CHECKPOINT) {
        snapshotType = CheckpointType::FULL_CHECKPOINT;
    } else if (
        checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT || checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT_SUSPEND ||
        checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT_TERMINATE) {
        snapshotType = DecodeSavepointType(checkpointTypeCode, buffer);
    } else {
        throw std::runtime_error("Unknown checkpoint type code: " + std::to_string(checkpointTypeCode));
    }

    // Read the location reference
    int locationRefLen = buffer.getIntBigEndian();
    std::shared_ptr<CheckpointStorageLocationReference> locationRef = nullptr;
    if (locationRefLen == -1) {
        locationRef = CheckpointStorageLocationReference::GetDefault();
    } else {
        std::shared_ptr<std::vector<uint8_t>> bytes = std::make_shared<std::vector<uint8_t>>(locationRefLen);
        buffer.getBytes(bytes->data(), locationRefLen);
        locationRef = std::make_shared<CheckpointStorageLocationReference>(bytes);
    }
    // Read the alignment type ordinal and convert it to the enum type
    uint8_t alignmentOrdinal = buffer.getByte();
    CheckpointOptions::AlignmentType alignmentType = static_cast<CheckpointOptions::AlignmentType>(alignmentOrdinal);
    // Read the alignment timeout
    int64_t alignmentTimeout = buffer.getLong();
    // Build the CheckpointOptions instance
    std::shared_ptr<CheckpointOptions> checkpointOptions =
        std::make_shared<CheckpointOptions>(snapshotType, locationRef, alignmentType, alignmentTimeout);
    // Construct and return the CheckpointBarrier
    return std::make_shared<CheckpointBarrier>(id, timestamp, checkpointOptions);
}

std::shared_ptr<SnapshotType> EventSerializer::DecodeSavepointType(uint8_t checkpointTypeCode, ByteBuffer& buffer)
{
    uint8_t formatTypeCode = buffer.getByte();
    SavepointFormatType formatType;
    if (formatTypeCode == EventSerializer::SAVEPOINT_FORMAT_CANONICAL) {
        formatType = SavepointFormatType::CANONICAL;
    } else if (formatTypeCode == EventSerializer::SAVEPOINT_FORMAT_NATIVE) {
        formatType = SavepointFormatType::NATIVE;
    } else if (formatTypeCode == EventSerializer::SAVEPOINT_FORMAT_COMPATIBLE) {
        formatType = SavepointFormatType::COMPATIBLE;
    } else {
        throw std::runtime_error("Unknown savepoint format type code: " + std::to_string(formatTypeCode));
    }
    if (checkpointTypeCode == EventSerializer::CHECKPOINT_TYPE_SAVEPOINT) {
        return SavepointType::savepoint(formatType);
    } else if (checkpointTypeCode == EventSerializer::CHECKPOINT_TYPE_SAVEPOINT_SUSPEND) {
        return SavepointType::suspend(formatType);
    } else if (checkpointTypeCode == EventSerializer::CHECKPOINT_TYPE_SAVEPOINT_TERMINATE) {
        return SavepointType::terminate(formatType);
    } else {
        throw std::runtime_error("Unknown savepoint type code: " + std::to_string(checkpointTypeCode));
    }
}
} // namespace omnistream
