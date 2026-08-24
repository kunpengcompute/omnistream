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

#ifndef FLINK_TNEL_STREAMELEMENT_H
#define FLINK_TNEL_STREAMELEMENT_H

#include "basictypes/Object.h"

enum class StreamElementTag {
    // the integer value_ for each tag should be exact same with flink Stream element_ tag
    // see StreamElementSerializer
    TAG_UNKNOWN = -1,
    TAG_REC_WITH_TIMESTAMP = 0,
    TAG_REC_WITHOUT_TIMESTAMP = 1,
    TAG_WATERMARK = 2,
    TAG_LATENCY_MARKER = 3,
    TAG_STREAM_STATUS = 4,
    TAG_RECORD_ATTRIBUTES = 5,
    TAG_INTERNAL_WATERMARK = 6,
    BINARY_ROW_DATA =7,
    VECTOR_BATCH = 127
};

class StreamElement : public Object {
public:
    explicit StreamElement(StreamElementTag tag = StreamElementTag::TAG_UNKNOWN) : tag_(tag) {};
    virtual ~StreamElement() = default;

    StreamElementTag getTag() const
    {
        return tag_;
    }
    void setTag(StreamElementTag tag)
    {
        tag_ = tag;
    }

    void* getValue() const
    {
        return value_;
    }

    void setValue(void* value)
    {
        value_ = value;
    }

protected:
    void* value_ = nullptr;

private:
    StreamElementTag tag_;
};

#endif // FLINK_TNEL_STREAMELEMENT_H
