/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * OmniStream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "runtime/watermark/WatermarkStatus.h"
#include "ObjectBufferBuilder.h"
#include <algorithm>
#include <sstream>
#include <climits>
#include <atomic>
#include <memory>

#include "table/utils/VectorBatchDeserializationUtils.h"
#include "VectorBatchBuffer.h"
#include "streaming/runtime/streamrecord/StreamElement.h"

namespace omnistream {

ObjectBufferBuilder::ObjectBufferBuilder(ObjectSegment* objSegment, std::shared_ptr<BufferRecycler> recycler)
    : BufferBuilder(new VectorBatchBuffer(objSegment, recycler)),
      objSegment(objSegment)
{
}

std::shared_ptr<BufferConsumer> ObjectBufferBuilder::createBufferConsumerFromBeginning()
{
    return createBufferConsumer(0);
}

std::shared_ptr<BufferConsumer> ObjectBufferBuilder::createBufferConsumer(int currentReaderPosition)
{
    if (bufferConsumerCreated) {
        throw std::runtime_error("Two BufferConsumer shouldn't exist for one BufferBuilder");
    }
    bufferConsumerCreated = true;
    positionMarker->addRef();
    return std::make_shared<ObjectBufferConsumer>(
        dynamic_cast<VectorBatchBuffer*>(buffer->RetainBuffer()), positionMarker, currentReaderPosition);
}

int ObjectBufferBuilder::appendAndCommit(void* source)
{
    int writtenBytes = append(source);
    commit();
    return writtenBytes;
}

int ObjectBufferBuilder::append(void* source)
{
    if (isFinished()) {
        throw std::runtime_error("BufferBuilder is finished");
    }
    LOG_PART(
        " Put a record to buffer builder :" << this << " at positionMarker->getCached()"
                                            << positionMarker->getCached());

    objSegment->putObject(positionMarker->getCached(), reinterpret_cast<StreamElement*>(source));
    LOG("put source to objSegment");
    positionMarker->move(1);
    return 1;
}

int ObjectBufferBuilder::appendSerializedObjectSegment(const uint8_t* source, int length)
{
    auto appendResult = ObjectSegmentChannelStateSerde::AppendSerializedObjectSegment(
        source, length, objSegment, positionMarker->getCached(), getWritableBytes());
    positionMarker->move(appendResult.elementsWritten);
    commit();
    return appendResult.bytesConsumed;
}

StreamElement* ObjectBufferBuilder::getObject(int index)
{
    return objSegment->getObject(index);
}

std::string ObjectBufferBuilder::toString()
{
    std::stringstream ss;
    ss << "ObjectBufferBuilder{maxCapacity=" << maxCapacity << ", committedBytes=" << positionMarker->getCached()
       << ", finished=" << isFinished() << "}";
    return ss.str();
}

Segment* ObjectBufferBuilder::GetSegment()
{
    return objSegment;
}

ObjectSegmentChannelStateSerde::AppendResult ObjectSegmentChannelStateSerde::AppendSerializedObjectSegment(
    const uint8_t* source, int length, ObjectSegment* target, int targetOffset, int writableElements)
{
    // 1. 参数检查
    // source 非空，length >= sizeof(int32_t)，builder 未 finished

    uint8_t* cursor = const_cast<uint8_t*>(source);
    uint8_t* end = cursor + length;

    // 2. 读取 elementNum
    int32_t elementNum;
    memcpy_s(&elementNum, sizeof(int32_t), cursor, sizeof(int32_t));
    cursor += sizeof(int32_t);

    // 3. 检查 ObjectSegment 剩余槽位
    if (elementNum < 0 || elementNum > writableElements) {
        throw std::runtime_error("ObjectSegment restore element count exceeds capacity");
    }

    // 4. 逐个元素反序列化
    const int initialTargetOffset = targetOffset;
    int32_t written = 0;
    try {
        for (int32_t i = 0; i < elementNum; i++) {
            int8_t dataType;
            memcpy_s(&dataType, sizeof(int8_t), cursor, sizeof(int8_t));
            cursor += sizeof(int8_t);

            StreamElementTag tag = static_cast<StreamElementTag>(dataType);
            StreamElement* element = nullptr;

            switch (tag) {
                case StreamElementTag::TAG_UNKNOWN: element = new StreamElement(); break;

                case StreamElementTag::TAG_WATERMARK: {
                    long timestamp = VectorBatchDeserializationUtils::derializeWatermark(cursor);
                    element = new Watermark(timestamp);
                    break;
                }

                case StreamElementTag::VECTOR_BATCH: {
                    std::unique_ptr<VectorBatch> vb(VectorBatchDeserializationUtils::deserializeVectorBatch(cursor));
                    element = new StreamRecord(vb.get());
                    vb.release();
                    break;
                }
                case StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP: {
                    std::unique_ptr<VectorBatch> vb(VectorBatchDeserializationUtils::deserializeVectorBatch(cursor));
                    element = new StreamRecord(vb.get());
                    vb.release();
                    break;
                }
                case StreamElementTag::TAG_REC_WITH_TIMESTAMP: {
                    long timeStamp;
                    memcpy_s(&timeStamp, sizeof(long), cursor, sizeof(long));
                    cursor += sizeof(long);
                    std::unique_ptr<VectorBatch> vb(VectorBatchDeserializationUtils::deserializeVectorBatch(cursor));
                    element = new StreamRecord(vb.get(), timeStamp);
                    vb.release();
                    break;
                }

                case StreamElementTag::TAG_STREAM_STATUS: {
                    int32_t status;
                    memcpy_s(&status, sizeof(status), cursor, sizeof(status));
                    cursor += sizeof(status);
                    element = new WatermarkStatus(status);
                    break;
                }
                case StreamElementTag::TAG_RECORD_ATTRIBUTES:
                case StreamElementTag::TAG_LATENCY_MARKER:
                case StreamElementTag::TAG_INTERNAL_WATERMARK: {
                    ERROR_RELEASE(
                        "ObjectSegment channel-state deserialization does not support StreamElement tag "
                        << static_cast<int>(tag));
                    throw std::runtime_error(
                        "ObjectSegment channel-state deserialization does not support StreamElement tag " +
                        std::to_string(static_cast<int>(tag)));
                }

                default:
                    ERROR_RELEASE(
                        "ObjectSegment channel-state deserialization encountered unknown StreamElement tag "
                        << static_cast<int>(tag));
                    throw std::runtime_error(
                        "ObjectSegment channel-state deserialization encountered unknown StreamElement tag " +
                        std::to_string(static_cast<int>(tag)));
            }

            target->putObject(targetOffset++, element);
            written++;
        }
    } catch (...) {
        target->ReleaseObjects(initialTargetOffset, written);
        throw;
    }

    // 此处可校验 cursor == end
    // 如果不等，说明 writer/reader 协议不一致
    return {static_cast<int>(cursor - source), elementNum};
}
} // namespace omnistream
