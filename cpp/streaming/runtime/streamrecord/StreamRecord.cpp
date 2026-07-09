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

#include <limits>
#include "StreamRecord.h"

StreamRecord::StreamRecord()
    : StreamElement(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP),
      timestamp_(0)
{
}

StreamRecord::StreamRecord(void* value)
    : StreamElement(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP),
      timestamp_(0)
{
    setValue(value);
}

StreamRecord::StreamRecord(void* value, long timestamp)
    : StreamElement(StreamElementTag::TAG_REC_WITH_TIMESTAMP),
      timestamp_(timestamp)
{
    setValue(value);
}

long StreamRecord::getTimestamp() const
{
    if (getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP) {
        return timestamp_;
    } else {
        return std::numeric_limits<int64_t>::min();
    }
}

void StreamRecord::setTimestamp(long timestamp)
{
    timestamp_ = timestamp;
    setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
}

bool StreamRecord::hasTimestamp() const
{
    return getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP;
}

StreamRecord* StreamRecord::replace(void* value, long timestamp)
{
    this->value_ = value;
    this->timestamp_ = timestamp;
    setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
    return this;
}

StreamRecord* StreamRecord::replace(void* value)
{
    this->value_ = value;
    return this;
}

void StreamRecord::eraseTimestamp()
{
    setTag(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);
}
