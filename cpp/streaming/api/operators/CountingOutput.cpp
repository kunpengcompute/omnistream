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

#include "CountingOutput.h"
#include "table/utils/VectorBatchSerializationUtils.h"

CountingOutput::CountingOutput(
    WatermarkGaugeExposingOutput* output,
    const std::shared_ptr<omnistream::TaskMetricGroup>& metricGroup,
    omnistream::OperatorPOD& opConfig)
    : output(output)
{
    if (metricGroup != nullptr) {
        auto recordsPtr = metricGroup->GetInternalOperatorIOMetric(opConfig.getName(), "numRecordsOut");
        LOG("numRecordsOut add" << reinterpret_cast<long>(recordsPtr.get()));
        numRecordsOut = std::static_pointer_cast<omnistream::SimpleCounter>(recordsPtr);
    } else {
        numRecordsOut = nullptr;
    }
}

void CountingOutput::emitWatermark(Watermark* mark)
{
    LOG("CountingOutput::emitWatermark: " << mark->getTimestamp());
    output->emitWatermark(mark);
}

void CountingOutput::collect(void* record)
{
    if (numRecordsOut != nullptr) {
        numRecordsOut->Inc(
            reinterpret_cast<omnistream::VectorBatch*>(reinterpret_cast<StreamRecord*>(record)->getValue())
                ->GetRowCount());
    }
    output->collect(record);
}

void CountingOutput::close()
{
    output->close();
}

void CountingOutput::emitWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    output->emitWatermarkStatus(watermarkStatus);
}

BytesCountingOutput::BytesCountingOutput(
    WatermarkGaugeExposingOutput* output,
    const std::shared_ptr<omnistream::TaskMetricGroup>& metricGroup,
    omnistream::OperatorPOD& opConfig)
    : output(output)
{
    if (metricGroup != nullptr) {
        auto bytesPtr = metricGroup->GetInternalOperatorIOMetric(opConfig.getName(), "numBytesOut");
        LOG("numBytesOut add" << reinterpret_cast<long>(bytesPtr.get()));
        numBytesOut = std::static_pointer_cast<omnistream::SimpleCounter>(bytesPtr);
    } else {
        numBytesOut = nullptr;
    }
}

void BytesCountingOutput::emitWatermark(Watermark* mark)
{
    output->emitWatermark(mark);
}

void BytesCountingOutput::collect(void* record)
{
    if (numBytesOut != nullptr) {
        long numBytesOutCount = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(
            reinterpret_cast<omnistream::VectorBatch*>(reinterpret_cast<StreamRecord*>(record)->getValue()));
        numBytesOut->Inc(numBytesOutCount);
    }
    output->collect(record);
}

void BytesCountingOutput::close()
{
    output->close();
}

void BytesCountingOutput::emitWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    output->emitWatermarkStatus(watermarkStatus);
}
