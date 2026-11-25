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

#ifndef FLINK_TNEL_ABSTRACTTWOINPUTSTREAMOPERATOR_H
#define FLINK_TNEL_ABSTRACTTWOINPUTSTREAMOPERATOR_H

#include "StreamOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "core/include/common.h"
#include "runtime/watermark/WatermarkStatus.h"

class TwoInputStreamOperator : public StreamOperator {
public:
    ~TwoInputStreamOperator() override = default;

    virtual void processBatch1(StreamRecord* element)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    virtual void processBatch2(StreamRecord* element)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    virtual void processElement1(StreamRecord* element)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    virtual void processElement2(StreamRecord* element)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
    virtual void ProcessWatermark1(Watermark* watermark)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
    virtual void ProcessWatermark2(Watermark* watermark)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
    virtual void processWatermarkStatus1(WatermarkStatus* watermarkStatus)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
    virtual void processWatermarkStatus2(WatermarkStatus* watermarkStatus)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    std::string getTypeName() override
    {
        return "TwoInputStreamOperator";
    }
};
#endif
