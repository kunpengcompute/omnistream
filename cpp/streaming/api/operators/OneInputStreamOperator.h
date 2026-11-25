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

#ifndef FLINK_TNEL_ONEINPUTSTREAMOPERATOR_H
#define FLINK_TNEL_ONEINPUTSTREAMOPERATOR_H

#include "Input.h"
#include "StreamOperator.h"
#include "core/include/common.h"

class OneInputStreamOperator : public StreamOperator, public Input {
public:
    ~OneInputStreamOperator() override = default;

    const char* getName() override
    {
        // NOT IMPLEMENTED
        return "OneInputStreamOperator";
    }

    std::string getTypeName() override
    {
        return "OneInputStreamOperator";
    }

    void processBatch(StreamRecord* element) override
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    void processElement(StreamRecord* element) override
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    void ProcessWatermark(Watermark* watermark) override
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
};

#endif
