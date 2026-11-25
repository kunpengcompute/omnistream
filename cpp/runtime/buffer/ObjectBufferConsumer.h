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

#ifndef OMNISTREAM_BUFFERCONSUMER_H
#define OMNISTREAM_BUFFERCONSUMER_H

#include <memory>
#include <string>
#include "ObjectBuffer.h"
#include "ObjectBufferDataType.h"

#include "PositionMarker.h"
#include "VectorBatchBuffer.h"
#include "BufferConsumer.h"

namespace omnistream {

class ObjectBufferConsumer : public BufferConsumer {
public:
    ObjectBufferConsumer(VectorBatchBuffer* buffer, int size);
    ObjectBufferConsumer(VectorBatchBuffer* buffer, PositionMarker* currentWriterPosition, int currentReaderPosition);
    ~ObjectBufferConsumer() override;

    Buffer* build() override;

    std::shared_ptr<BufferConsumer> copy() override;
    std::shared_ptr<BufferConsumer> copyWithReaderPosition(int readerPosition) override;

    VectorBatchBuffer* buildVectorBatchBuffer();

    bool isStartOfDataBuffer() const override;
    std::string toDebugString(bool includeHash) override;
    std::string toString() override;
};

} // namespace omnistream

#endif // OMNISTREAM_BUFFERCONSUMER_H
