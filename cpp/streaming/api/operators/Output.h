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

#ifndef FLINK_TNEL_OUTPUT_H
#define FLINK_TNEL_OUTPUT_H

#include "functions/Collector.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/watermark/Watermark.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "core/typeinfo/TypeInformation.h"

// this interface is for operator
// notice on the ownership, the element itself is always owned by this operator, the ownership of value inside the
// element should be transfer to the output. once the call is return by output, the operator can reuse this element
// which also means the output has to get the other information such as the timestamp.

class Output : public Collector {
public:
    // Pass watermark to next operator
    virtual void emitWatermark(Watermark* watermark) = 0;
    virtual void emitWatermarkStatus(WatermarkStatus* watermarkStatus) = 0;
};

#endif // FLINK_TNEL_OUTPUT_H
