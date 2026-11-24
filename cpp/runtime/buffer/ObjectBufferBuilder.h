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

#ifndef OMNISTREAM_BUFFERBUILDER_H
#define OMNISTREAM_BUFFERBUILDER_H

#include <cstdint>
#include <memory>
#include <string>

#include "BufferBuilder.h"
#include "ObjectBufferConsumer.h"
#include "ObjectSegment.h"

namespace omnistream {

class ObjectBufferBuilder : public BufferBuilder {
public:
    ObjectBufferBuilder(ObjectSegment* objSegment, std::shared_ptr<BufferRecycler> recycler);
    ~ObjectBufferBuilder() override = default;

    int appendAndCommit(void* source) override;
    int append(void* source);
    int appendSerializedObjectSegment(const uint8_t* source, int length);

    using BufferBuilder::createBufferConsumer;
    std::shared_ptr<BufferConsumer> createBufferConsumerFromBeginning() override;
    std::shared_ptr<BufferConsumer> createBufferConsumer(int currentReaderPosition) override;

    std::string toString() override;

    // for test
    StreamElement* getObject(int index);
    Segment* GetSegment() override;

private:
    ObjectSegment* objSegment;
};

class ObjectSegmentChannelStateSerde {
public:
    struct AppendResult {
        int bytesConsumed;
        int elementsWritten;
    };

    static AppendResult AppendSerializedObjectSegment(
        const uint8_t* source, int length, ObjectSegment* target, int targetOffset, int writableElements);
};
} // namespace omnistream

#endif // OMNISTREAM_BUFFERBUILDER_H
