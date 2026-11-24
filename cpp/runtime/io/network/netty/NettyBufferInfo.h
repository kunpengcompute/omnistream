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

#ifndef NETTY_BUFFER_INFO_H
#define NETTY_BUFFER_INFO_H
#include <cstdint>
#include <memory>
#include "core/include/common.h"
#include "NettyMemorySegment.h"
namespace omnistream {
class NettyBufferInfo {
public:
    static int elementNumBytes;
    NettyBufferInfo(std::shared_ptr<NettyMemorySegment> nettyMemorySegment);
    NettyBufferInfo(uint8_t* buffer, int capacity);
    ~NettyBufferInfo();
    uint8_t*& GetPosition();
    uint8_t* GetOriginalAddress();
    uint8_t* GetDataAddress();
    int GetSize();
    bool operator==(const NettyBufferInfo& other) const;
    int GetWrittenBytes() const;
    void SetWrittenBytes(int bytes);
    void ResetBuffer();
    bool Useable(int newSize);
    void IncrementElementNum();
    int GetElementNum() const;
    void MarkElementNumWritten();

private:
    volatile int writtenBytes_ = 0;
    uint8_t* originalAddress_;
    uint8_t* dataAddress_;
    volatile int size_;
    uint8_t* position;
    volatile int elementNum = 0;
    std::shared_ptr<NettyMemorySegment> nettyMemorySegment_;
};
} // namespace omnistream
#endif // NETTY_BUFFER_INFO_H
