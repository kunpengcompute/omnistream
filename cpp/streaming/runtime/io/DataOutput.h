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

#ifndef FLINK_TNEL_DATAOUTPUT_H
#define FLINK_TNEL_DATAOUTPUT_H

#include "core/include/common.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/watermark/Watermark.h"

class DataOutput {
public:
    // the ownership of streamRecord is transferred to DataOutput as well. DataOutput has to delete it or transfer
    // it to others
    // note on performance:  mem allocation for all New streamRecord can be optimized later
    virtual void emitRecord(StreamRecord* streamRecord) = 0;

    /**
    void emitWatermark(Watermark watermark);

    void emitWatermarkStatus(WatermarkStatus watermarkStatus);

    void emitLatencyMarker(LatencyMarker latencyMarker);

    void emitRecordAttributes(RecordAttributes recordAttributes);
     **/
    virtual void emitWatermark(Watermark* watermark) = 0;
};

#endif
