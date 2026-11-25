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

#ifndef FLINK_TNEL_RECORDWRITEROUTPUT_H
#define FLINK_TNEL_RECORDWRITEROUTPUT_H

#include "core/include/common.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"
#include "runtime/io/network/api/writer/RecordWriter.h"
#include "runtime/plugable/SerializationDelegate.h"
#include "core/typeutils/TypeSerializer.h"
#include "../streamrecord/StreamRecord.h"
#include "streaming/runtime/metrics/WatermarkGauge.h"
#include "streaming/api/watermark/InternalWatermark.h"
#include "basictypes/Long.h"

namespace omnistream::datastream {
class RecordWriterOutput : public WatermarkGaugeExposingOutput {
public:
    RecordWriterOutput(TypeSerializer* outSerializer, omnistream::datastream::RecordWriter* recordWriter);
    ~RecordWriterOutput() override;

    void collect(void* record) override;

    __attribute__((always_inline)) bool collectAndCheckIfChained(StreamRecord* record)
    {
        LOG(">>>>>>>>");
        pushToRecordWriter(record);
        return true;
    }

    __attribute__((always_inline)) void pushToRecordWriter(StreamRecord* record)
    {
        LOG(">>>>>>>");
        serializationDelegate_->setInstance(record);
        recordWriter_->emit(serializationDelegate_);
    }

    void close() override;
    void emitWatermark(Watermark* watermark) override;
    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override;

private:
    WatermarkGauge watermarkGauge;

    /// RecordWriter<SerializationDelegate<StreamElement>> recordWriter_;
    omnistream::datastream::RecordWriter* recordWriter_;

    // SerializationDelegate<StreamElement> serializationDelegate;
    SerializationDelegate* serializationDelegate_;

    InternalWatermark* reusableWatermark_ = nullptr;

    WatermarkStatus* announcedStatus;
};
} // namespace omnistream::datastream

#endif // FLINK_TNEL_RECORDWRITEROUTPUT_H
