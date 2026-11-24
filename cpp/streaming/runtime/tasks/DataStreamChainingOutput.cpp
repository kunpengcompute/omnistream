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

#include "DataStreamChainingOutput.h"
#include "basictypes/Object.h"

namespace omnistream::datastream {
DataStreamChainingOutput::DataStreamChainingOutput(Input* op) : operator_(op)
{
    watermarkGauge = new WatermarkGauge();
}

void DataStreamChainingOutput::collect(void* record)
{
    LOG(" ChainingOutput collect >>>>>>>>");
    auto streamRecord = reinterpret_cast<StreamRecord*>(record);
    operator_->processElement(streamRecord);
}

void DataStreamChainingOutput::close()
{
    // do nothing
}

void DataStreamChainingOutput::emitWatermark(Watermark* mark)
{
    LOG("ChainingOutput::emitWatermark: " << mark->getTimestamp() << " name: " << operator_->getName());
    watermarkGauge->setCurrentwatermark(mark->getTimestamp());
}

void DataStreamChainingOutput::emitWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    NOT_IMPL_EXCEPTION;
}

void DataStreamChainingOutput::disableFree()
{
    shouldFree = false;
}

DataStreamChainingOutput::~DataStreamChainingOutput()
{
    delete watermarkGauge;
}
} // namespace omnistream::datastream
