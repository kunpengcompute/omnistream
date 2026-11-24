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

#ifndef MEMORYBUFFERBUIDLER_H
#define MEMORYBUFFERBUIDLER_H

#include <memory>
#include <string>

#include "core/memory/MemorySegment.h"
#include "NetworkBuffer.h"
#include "BufferBuilder.h"
#include "MemoryBufferConsumer.h"
#include "api/common/TimerThreadPool.h"

namespace omnistream::datastream {

// check
class MemoryBufferBuilder : public BufferBuilder {
public:
    MemoryBufferBuilder(MemorySegment* memorySegment, std::shared_ptr<BufferRecycler> recycler);
    // delete in BufferWritingResultPartition
    ~MemoryBufferBuilder() override;

    int appendAndCommit(void* source) override;

    int append(void* source);

    int appendRawBytes(const uint8_t* source, int length);

    // using BufferBuilder::createBufferConsumer;
    std::shared_ptr<BufferConsumer> createBufferConsumerFromBeginning() override;
    std::shared_ptr<BufferConsumer> createBufferConsumer(int currentReaderPosition) override;

    std::string toString() override;
    Segment* GetSegment() override;

private:
    MemorySegment* memorySegment;
    int commitCount = 0;
    TimerThreadPool::TaskId taskId;
    const int MAX_COMMIT_COUNT = 1000;
};

} // namespace omnistream::datastream

#endif // MEMORYBUFFERBUIDLER_H
