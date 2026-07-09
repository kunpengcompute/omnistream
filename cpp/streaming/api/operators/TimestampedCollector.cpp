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

#include "TimestampedCollector.h"
#include "basictypes/Object.h"

TimestampedCollector::TimestampedCollector(Output* output, bool isDataStream, RecordValueType valueType)
    : output_(output),
      isDataStream(isDataStream),
      valueType_(valueType)
{
    reuse = new StreamRecord();
    tag_ = StreamElementTag::TAG_REC_WITH_TIMESTAMP;
    timestamp_ = 0L;
}

TimestampedCollector::~TimestampedCollector()
{
    if (isDataStream) {
        delete reuse;
    }
}

void TimestampedCollector::collect(void* value)
{
    LOG(">>>>>>>");
    // The default TAG is TAG_REC_WITH_TIMESTAMP
    if (isDataStream) {
        auto* obj = static_cast<Object*>(value);
        obj->getRefCount();
        reuse->replace(obj, timestamp_);
        output_->collect(reuse);
    } else {
        auto* record = new StreamRecord(value);
        record->setTimestamp(timestamp_);
        output_->collect(record);
    }
}

void TimestampedCollector::collectExternalRow(void* value)
{
    LOG("collect for external row.");
    // The default TAG is TAG_REC_WITH_TIMESTAMP
    auto* record = new StreamRecord(value);
    record->setTimestamp(timestamp_);
    record->setExternalRow(true);
    output_->collect(record);
}

void TimestampedCollector::setTimestamp(StreamRecord* timestampBase)
{
    if (timestampBase->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP) {
        timestamp_ = timestampBase->getTimestamp();
        tag_ = StreamElementTag::TAG_REC_WITH_TIMESTAMP;
    } else if (timestampBase->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP) {
        this->eraseTimestamp();
    }
}
void TimestampedCollector::close()
{
    output_->close();
}

void TimestampedCollector::emitWatermark(Watermark* watermark)
{
    output_->emitWatermark(watermark);
}

void TimestampedCollector::emitWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    output_->emitWatermarkStatus(watermarkStatus);
}

void TimestampedCollector::setAbsoluteTimestamp(int64_t timestamp)
{
    reuse->setTimestamp(timestamp);
}

void TimestampedCollector::eraseTimestamp()
{
    tag_ = StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP;
}
