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

#include "OmniAsyncDataOutputToOutput.h"

namespace omnistream {
OmniAsyncDataOutputToOutput::OmniAsyncDataOutputToOutput(Output* output_, bool isDataStream)
    : output(output_),
      isDataStream(isDataStream)
{
}

void OmniAsyncDataOutputToOutput::emitRecord(StreamRecord* streamRecord)
{
    LOG(">>>> OmniAsyncDataOutputToOutput.emitRecord");
    if (isDataStream) {
        output->collect(streamRecord);
    } else {
        auto newRecord = new StreamRecord(streamRecord->getValue(), streamRecord->getTimestamp());
        newRecord->setValueType(streamRecord->getValueType());
        if (streamRecord->hasTimestamp()) {
            newRecord->setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
        } else {
            newRecord->setTag(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);
        }
        output->collect(newRecord);
    }
}

void OmniAsyncDataOutputToOutput::emitWatermark(Watermark* watermark)
{
    LOG(">>>> OmniAsyncDataOutputToOutput.emitWatermark");
    output->emitWatermark(watermark);
}

void OmniAsyncDataOutputToOutput::emitWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    LOG(">>>> OmniAsyncDataOutputToOutput.emitWatermarkStatus");
    output->emitWatermarkStatus(watermarkStatus);
}
} // namespace omnistream
