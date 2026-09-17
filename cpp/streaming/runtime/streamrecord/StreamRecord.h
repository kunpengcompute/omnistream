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

#ifndef FLINK_TNEL_STREAMRECORD_H
#define FLINK_TNEL_STREAMRECORD_H

#include <cstdint>
#include <functional>
#include "StreamElement.h"
enum class RecordValueType : uint8_t {
    UNKNOWN = 0,
    VECTOR_BATCH = 1,
    OBJECT = 2,
    BINARY_ROW_DATA = 3,
    JOINED_ROW_DATA = 4,
    EXTERNAL_ROW = 5
};
class StreamRecord : public StreamElement {
public:
    StreamRecord();
    explicit StreamRecord(void* value);
    StreamRecord(void* value, long timestamp);

    long getTimestamp() const;
    void setTimestamp(long timestamp);
    bool hasTimestamp() const;

    StreamRecord* replace(void* value, long timestamp);
    StreamRecord* replace(void* value);

    void eraseTimestamp();

    bool operator==(const StreamRecord& other) const
    {
        return value_ == other.value_ && timestamp_ == other.timestamp_;
    }

    void setExternalRow(bool hasExternalRow)
    {
        this->hasExternalRow_ = hasExternalRow;
    }

    bool hasExternalRow()
    {
        return this->hasExternalRow_;
    }
    RecordValueType getValueType() const
    {
        return valueType_;
    }
    void setValueType(RecordValueType valueType)
    {
        valueType_ = valueType;
    }

private:
    long timestamp_;
    RecordValueType valueType_ = RecordValueType::VECTOR_BATCH;
    bool hasExternalRow_ = false;
};

namespace std {
template <>
struct hash<StreamRecord> {
    size_t operator()(const StreamRecord& record) const
    {
        // Implement a hash function for StreamRecord
        return std::hash<void*>()(record.getValue());
    }
};
} // namespace std

#endif
