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

#ifndef OMNISTREAM_EVENTSERIALIZER_H
#define OMNISTREAM_EVENTSERIALIZER_H

#include <memory>
#include <vector>
#include <cstdint>
#include <utility>
#include <buffer/NetworkBuffer.h>
#include <buffer/ObjectBufferConsumer.h>
#include <checkpoint/SavepointType.h>
#include <memory/MemorySegment.h>
#include <core/utils/ByteBuffer.h>

#include "buffer/NetworkBuffer.h"
#include "event/AbstractEvent.h"
#include "partition/consumer/BufferOrEvent.h"
#include "partition/consumer/InputChannel.h"
#include "io/network/api/CheckpointBarrier.h"

using ::datastream::NetworkBuffer;

namespace omnistream {
class ObjectBuffer;

class EventSerializer {
public:
    static NetworkBuffer* toBuffer(std::shared_ptr<AbstractEvent> event, bool hasPriority);
    // static std::shared_ptr<ObjectBufferConsumer> toBufferConsumer(const int event, bool hasPriority) ;
    static std::shared_ptr<BufferConsumer> ToBufferConsumer(std::shared_ptr<AbstractEvent> event, bool hasPriority);
    static std::shared_ptr<AbstractEvent> fromBuffer(Buffer* buffer);
    static std::shared_ptr<AbstractEvent> fromBufferNotRecycle(Buffer* buffer);
    static std::shared_ptr<AbstractEvent> fromBuffer_V2(const std::shared_ptr<Buffer>& buffer);

    static MemorySegment* ToSerializedEvent(std::shared_ptr<AbstractEvent> event);
    static std::shared_ptr<AbstractEvent> fromSerializedEvent(Buffer* buffer, bool recycleEvent = true);
    static std::shared_ptr<AbstractEvent> fromSerializedEvent_V2(std::shared_ptr<Buffer> buffer);

    //        static std::shared_ptr<BufferOrEvent> fromNetworkBuffer(
    //            std::shared_ptr<::datastream::NetworkBuffer> networkBuffer,
    //            std::shared_ptr<Buffer> buffer,
    //            bool moreAvailable,
    //            std::shared_ptr<InputChannel> currentChannel,
    //            bool morePriorityEvents);

    // static std::shared_ptr<CheckpointBarrier> deserializeBarrier(std::shared_ptr<ByteBuffer> buffer);
    static MemorySegment* SerializeCheckpointBarrier(std::shared_ptr<CheckpointBarrier> checkpointBarrier);
    static void EncodeSavepointType(SavepointType* savepointType, ByteBuffer& buffer);
    static std::shared_ptr<CheckpointBarrier> DeserializeCheckpointBarrier(ByteBuffer& buffer);
    static std::shared_ptr<SnapshotType> DecodeSavepointType(uint8_t checkpointTypeCode, ByteBuffer& buffer);

public:
    static const int INVALID_EVENT;
    ;
    static const int END_OF_PARTITION_EVENT;
    static const int CHECKPOINT_BARRIER_EVENT;
    static const int END_OF_SUPERSTEP_EVENT;
    static const int OTHER_EVENT;
    static const int CANCEL_CHECKPOINT_MARKER_EVENT;
    static const int END_OF_CHANNEL_STATE_EVENT;
    static const int ANNOUNCEMENT_EVENT;
    static const int VIRTUAL_CHANNEL_SELECTOR_EVENT;
    static const int END_OF_USER_RECORDS_EVENT;
    static const int INNER_RECOVER_EVENT;
    static constexpr uint8_t CHECKPOINT_TYPE_CHECKPOINT = 0x00;
    static constexpr uint8_t CHECKPOINT_TYPE_SAVEPOINT = 0x01;
    static constexpr uint8_t CHECKPOINT_TYPE_SAVEPOINT_SUSPEND = 0x02;
    static constexpr uint8_t CHECKPOINT_TYPE_SAVEPOINT_TERMINATE = 0x03;
    static constexpr uint8_t CHECKPOINT_TYPE_FULL_CHECKPOINT = 0x04;
    static constexpr uint8_t SAVEPOINT_FORMAT_CANONICAL = 0x00;
    static constexpr uint8_t SAVEPOINT_FORMAT_NATIVE = 0x01;
    static constexpr uint8_t SAVEPOINT_FORMAT_COMPATIBLE = 0x02;
};
} // namespace omnistream

#endif // OMNISTREAM_EVENTSERIALIZER_H
