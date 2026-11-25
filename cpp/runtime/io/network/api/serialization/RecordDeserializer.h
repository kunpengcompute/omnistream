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

#ifndef FLINK_TNEL_RECORDDESERIALIZER_H
#define FLINK_TNEL_RECORDDESERIALIZER_H

#include <cstdint>
#include <cstddef>
#include <buffer/ReadOnlySlicedNetworkBuffer.h>

#include "DeserializationResult.h"
#include "../../../../../core/io/IOReadableWritable.h"
#include "../../../../../core/typeutils/TypeSerializer.h"

using namespace ::datastream;
class StreamRecord;
namespace omnistream::datastream {
class RecordDeserializer : public TypeSerializer {
public:
    RecordDeserializer() = default;
    virtual DeserializationResult& getNextRecord(IOReadableWritable& target)
    {
        return DeserializationResult_PARTIAL_RECORD;
    }

    virtual void setNextBuffer(const uint8_t* buffer, int size)
    {
    }

    virtual void clear()
    {
    }
    virtual void SetNextBuffer(ReadOnlySlicedNetworkBuffer* buffer)
    {
    }
    virtual std::vector<omnistream::Buffer*> GetUnconsumedBuffer()
    {
        return {};
    }

    // SQL object buffers are already deserialized and cannot go through getNextRecord().
    // Rescaling deserializers override this hook to split a batch by row ownership.
    virtual ::StreamRecord* FilterRecordForSql(::StreamRecord& record)
    {
        return &record;
    }

    /**
     * Gets the unconsumed buffer_ which needs to be persisted in unaligned checkpoint scenario.
     *
     * <p>Note that the unconsumed buffer_ might be null if the whole buffer_ was already consumed
     * before and there are no partial length or data remained in the end_ of buffer_.
     */
    // CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException;
};
} // namespace omnistream::datastream

#endif
