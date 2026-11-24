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

#ifndef FLINK_TNEL_RECORDWRITEROUTPUT_V2_H
#define FLINK_TNEL_RECORDWRITEROUTPUT_V2_H

#include <runtime/plugable/SerializationDelegate.h>

#include "../include/common.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/runtime/metrics/WatermarkGauge.h"
#include "streaming/api/watermark/InternalWatermark.h"
#include "runtime/io/network/api/writer/V2/RecordWriterV2.h"

namespace omnistream {
class RecordWriterOutputV2 : public WatermarkGaugeExposingOutput {
public:
    explicit RecordWriterOutputV2(
        RecordWriterV2* recordWriter,
        TypeSerializer* outSerializer = nullptr,
        bool supportsUnalignedCheckpoints = true);
    ~RecordWriterOutputV2() override;

    bool collectAndCheckIfChained(StreamRecord* record);
    void pushToRecordWriter(StreamRecord* record);

    void collect(void* record) override;
    void close() override;
    void emitWatermark(Watermark* watermark) override;
    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override;
    void broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent);

    void setSerializationDelegate(SerializationDelegate* serializationDelegate);

private:
    // WatermarkGauge watermarkGauge;
    SerializationDelegate* serializationDelegate_;

    /// RecordWriter<SerializationDelegate<StreamElement>> recordWriter_;
    RecordWriterV2* recordWriter_;
    WatermarkGauge watermarkGauge_;
    bool supportsUnalignedCheckpoints_;
};
} // namespace omnistream

#endif
