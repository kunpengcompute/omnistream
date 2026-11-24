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

#ifndef OMNI_BUFFER_H
#define OMNI_BUFFER_H

#include <memory>
// todo: modify ObjectBufferDataType to BufferDataType later
#include "ObjectBufferDataType.h"
#include "BufferRecycler.h"

namespace omnistream {
class Buffer {
public:
    virtual ~Buffer() = default;

    virtual bool isBuffer() const = 0;
    virtual std::shared_ptr<BufferRecycler> GetRecycler() = 0;
    virtual void RecycleBuffer() = 0;
    virtual Buffer* RetainBuffer() = 0;
    virtual Buffer* ReadOnlySlice() = 0;
    virtual Buffer* ReadOnlySlice(int index, int length) = 0;
    virtual int GetMaxCapacity() const = 0;
    virtual int GetReaderIndex() const = 0;
    virtual void SetReaderIndex(int readerIndex) = 0;
    virtual int GetSize() const = 0;
    virtual void SetSize(int writerIndex) = 0;
    virtual int ReadableObjects() const = 0;

    virtual bool IsCompressed() const = 0;
    virtual void SetCompressed(bool isCompressed) = 0;
    virtual ObjectBufferDataType GetDataType() const = 0;
    virtual void SetDataType(ObjectBufferDataType dataType) = 0;
    virtual int GetBufferType() = 0;
    virtual int RefCount() const = 0;
    virtual std::string ToDebugString(bool includeHash) const = 0;

    // temp added
    virtual int EventType() const = 0;
    virtual Segment* GetSegment() = 0;
    virtual int GetOffset() const = 0;
};
} // namespace omnistream
#endif // OMNI_BUFFER_H
