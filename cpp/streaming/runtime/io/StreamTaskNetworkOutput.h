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

#ifndef FLINK_TNEL_STREAMTASKNETWORKOUTPUT_H
#define FLINK_TNEL_STREAMTASKNETWORKOUTPUT_H

#include <functional>
#include "DataOutput.h"
#include "../../api/operators/Input.h"
#include "runtime/metrics/Counter.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "../../api/operators/StreamOperator.h"
#include "streaming/api/watermark/Watermark.h"

namespace omnistream::datastream {
class StreamTaskNetworkOutput : public DataOutput {
public:
    explicit StreamTaskNetworkOutput();
    void emitRecord(StreamRecord* record) override;

    StreamTaskNetworkOutput(class StreamOperator* streamOperator, int operatorMethodIndicator);

    virtual ~StreamTaskNetworkOutput() = default;

    std::function<void(StreamRecord*)> recordProcessfuncPtr;

    void emitWatermark(Watermark* watermark) override;

private:
    // Input *operator_;
    StreamOperator* streamOperator = nullptr;

    Counter* counter_;

    int operatorMethodIndicator;

    static const int oneinputstreamop = 0;

    static const int leftTwoinputstreamop = 1;

    static const int rightTwoinputstreamop = 2;
};
} // namespace omnistream::datastream

#endif // FLINK_TNEL_STREAMTASKNETWORKOUTPUT_H
