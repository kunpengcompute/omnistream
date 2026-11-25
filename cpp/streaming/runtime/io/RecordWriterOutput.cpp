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

#include "RecordWriterOutput.h"
#include "streaming/runtime/streamrecord/StreamElementSerializer.h"

namespace omnistream::datastream {
RecordWriterOutput::RecordWriterOutput(
    TypeSerializer* outSerializer, omnistream::datastream::RecordWriter* recordWriter)
    : recordWriter_(recordWriter),
      announcedStatus(new WatermarkStatus(WatermarkStatus::activeStatus))
{
    LOG(">>>>>");
    if (outSerializer != nullptr) {
        serializationDelegate_ = new SerializationDelegate(new StreamElementSerializer(outSerializer));
    }
}

RecordWriterOutput::~RecordWriterOutput()
{
    delete reusableWatermark_;
}

void RecordWriterOutput::collect(void* record)
{
    LOG(">>>>>>>");
    auto* streamRecord = static_cast<StreamRecord*>(record);
    auto* value = static_cast<Object*>(streamRecord->getValue());
    collectAndCheckIfChained(streamRecord);
    if (!value->isPool) {
        value->putRefCount();
    } else {
        (dynamic_cast<Long*>(value))->putRefCount();
    }
}

void RecordWriterOutput::close()
{
    recordWriter_->close();
}

void RecordWriterOutput::emitWatermark(Watermark* mark)
{
    if (announcedStatus->IsIdle()) {
        return;
    }

    if (reusableWatermark_ == nullptr) {
        reusableWatermark_ = new InternalWatermark(0);
    }
    watermarkGauge.setCurrentwatermark(mark->getTimestamp());
    reusableWatermark_->setTimestamp(mark->getTimestamp());
    serializationDelegate_->setInstance(reusableWatermark_);

    recordWriter_->emit(serializationDelegate_);
}

void RecordWriterOutput::emitWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    if (!announcedStatus->Equals(watermarkStatus)) {
        announcedStatus = watermarkStatus;
        serializationDelegate_->setInstance(watermarkStatus);
        recordWriter_->emit(serializationDelegate_);
    }
}
} // namespace omnistream::datastream
