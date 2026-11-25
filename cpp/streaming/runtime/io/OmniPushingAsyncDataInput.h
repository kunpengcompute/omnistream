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

#ifndef OMNISTREAM_OMNIPUSHINGASYNCDATAINPUT_H
#define OMNISTREAM_OMNIPUSHINGASYNCDATAINPUT_H

#include "runtime/io/AvailabilityProvider.h"
#include <streaming/runtime/io/DataInputStatus.h>
#include "streaming/runtime/io/DataOutput.h"
#include "streaming/api/watermark/Watermark.h"
#include "watermark/WatermarkStatus.h"
#include <streaming/runtime/streamrecord/StreamRecord.h>

namespace omnistream {

class OmniPushingAsyncDataInput : public AvailabilityProvider {
public:
    class OmniDataOutput {
    public:
        virtual void emitRecord(StreamRecord* streamRecord) = 0;

        virtual void emitWatermark(Watermark* watermark) = 0;

        virtual void emitWatermarkStatus(WatermarkStatus* watermarkStatus) = 0;

        virtual void close() {};

        virtual ~OmniDataOutput() = default;

        // temp fix for taskType, should be removed in the future (taskType should not be set in Runtime)
        virtual void setTaskType(int taskType)
        {
        }
    };

    virtual DataInputStatus emitNext(OmniDataOutput* output) = 0;
};

} // namespace omnistream

#endif
