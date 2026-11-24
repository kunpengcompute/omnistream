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

#ifndef FLINK_TNEL_SPILLINGADAPTIVESPANNINGRECORDDESERIALIZER_H
#define FLINK_TNEL_SPILLINGADAPTIVESPANNINGRECORDDESERIALIZER_H

#include <buffer/ReadOnlySlicedNetworkBuffer.h>

#include "SpanningWrapper.h"
#include "NonSpanningWrapper.h"
#include "RecordDeserializer.h"

namespace omnistream::datastream {
class SpillingAdaptiveSpanningRecordDeserializer : public RecordDeserializer {
public:
    SpillingAdaptiveSpanningRecordDeserializer();

    DeserializationResult& getNextRecord(IOReadableWritable& target) override;

    void setNextBuffer(const uint8_t* buffer, int size) override;

    ~SpillingAdaptiveSpanningRecordDeserializer() override;

    void clear() override;

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    const char* getName() const override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::INVALID_BK;
    };
    void SetNextBuffer(ReadOnlySlicedNetworkBuffer* buffer);
    std::vector<omnistream::Buffer*> GetUnconsumedBuffer() override;

private:
    DeserializationResult& readNextRecord(IOReadableWritable& target);
    DeserializationResult& readNonSpanningRecord(IOReadableWritable& target);

    NonSpanningWrapper* nonSpanningWrapper;
    SpanningWrapper* spanningWrapper;
    ReadOnlySlicedNetworkBuffer* currentBuffer = nullptr;
};
} // namespace omnistream::datastream

#endif
