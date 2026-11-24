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

#include "StreamTaskNetworkOutput.h"
#include "runtime/metrics/SimpleCounter.h"
#include "../../api/operators/OneInputStreamOperator.h"
#include "../../api/operators/TwoInputStreamOperator.h"

// StreamTaskNetworkOutput::StreamTaskNetworkOutput(StreamOperator *op) : operator_(op)
// {
// }

namespace omnistream::datastream {
void StreamTaskNetworkOutput::emitRecord(StreamRecord* record)
{
    LOG("emitRecord >>>>>>>>");
    counter_->Inc();
    LOG("stream record address  " + std::to_string(reinterpret_cast<long>(record)));
    recordProcessfuncPtr(record);
}

StreamTaskNetworkOutput::StreamTaskNetworkOutput(StreamOperator* streamOperator, int operatorMethodIndicator)
    : streamOperator(streamOperator),
      operatorMethodIndicator(operatorMethodIndicator)
{
    std::cout << "StreamTaskNetworkOutput::StreamTaskNetworkOutput operatorMethodIndicator :" << operatorMethodIndicator
              << std::endl;
    if (operatorMethodIndicator == 0) {
        OneInputStreamOperator* oneInputStreamOperator = static_cast<OneInputStreamOperator*>(streamOperator);
        if (oneInputStreamOperator->isSetKeyContextElement()) {
            recordProcessfuncPtr = [oneInputStreamOperator](StreamRecord* record) {
                LOG("operatorMethodIndicator=0, setKey");
                oneInputStreamOperator->setKeyContextElement(record);
                LOG("operatorMethodIndicator=0, processElement");
                oneInputStreamOperator->processElement(record);
            };
        } else {
            recordProcessfuncPtr = [oneInputStreamOperator](StreamRecord* record) {
                LOG("operatorMethodIndicator=0, processElement");
                oneInputStreamOperator->processElement(record);
            };
        }
    } else if (operatorMethodIndicator == 1) {
        TwoInputStreamOperator* twoInputStreamOperator = static_cast<TwoInputStreamOperator*>(streamOperator);

        if (twoInputStreamOperator->isSetKeyContextElement1()) {
            recordProcessfuncPtr = [twoInputStreamOperator](StreamRecord* record) {
                twoInputStreamOperator->setKeyContextElement1(record);
                twoInputStreamOperator->processElement1(record);
            };
        } else {
            recordProcessfuncPtr = [twoInputStreamOperator](StreamRecord* record) {
                twoInputStreamOperator->processElement1(record);
            };
        }
    } else if (operatorMethodIndicator == 2) {
        TwoInputStreamOperator* twoInputStreamOperator = static_cast<TwoInputStreamOperator*>(streamOperator);
        if (twoInputStreamOperator->isSetKeyContextElement2()) {
            recordProcessfuncPtr = [twoInputStreamOperator](StreamRecord* record) {
                twoInputStreamOperator->setKeyContextElement2(record);
                twoInputStreamOperator->processElement2(record);
            };
        } else {
            recordProcessfuncPtr = [twoInputStreamOperator](StreamRecord* record) {
                twoInputStreamOperator->processElement2(record);
            };
        }
    }
    counter_ = new SimpleCounter();
}

void StreamTaskNetworkOutput::emitWatermark(Watermark* watermark)
{
    if (operatorMethodIndicator == oneinputstreamop) {
        LOG("processWatermark");
        OneInputStreamOperator* oneInputStreamOperator = static_cast<OneInputStreamOperator*>(streamOperator);
        oneInputStreamOperator->ProcessWatermark(watermark);
    } else if (operatorMethodIndicator == leftTwoinputstreamop) {
        LOG("processWatermark1");
        TwoInputStreamOperator* twoInputStreamOperator = static_cast<TwoInputStreamOperator*>(streamOperator);
        twoInputStreamOperator->ProcessWatermark1(watermark);
    } else if (operatorMethodIndicator == rightTwoinputstreamop) {
        LOG("processWatermark2");
        TwoInputStreamOperator* twoInputStreamOperator = static_cast<TwoInputStreamOperator*>(streamOperator);
        twoInputStreamOperator->ProcessWatermark2(watermark);
    }
}
} // namespace omnistream::datastream
