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

#ifndef FLINK_TNEL_TIMESTAMPEDCOLLECTOR_H
#define FLINK_TNEL_TIMESTAMPEDCOLLECTOR_H

#include "ValueCollector.h"
#include "Output.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "core/include/common.h"

class TimestampedCollector : public Output {
public:
    explicit TimestampedCollector(Output* output, bool isDataStream = false);

    ~TimestampedCollector() override;

    void collect(void* value) override;

    void collectExternalRow(void* value);

    void setTimestamp(StreamRecord* timestampBase);

    void close() override;

    void emitWatermark(Watermark* watermark) override;

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override;

    void setAbsoluteTimestamp(int64_t timestamp);

    void eraseTimestamp();

private:
    Output* output_;
    StreamRecord* reuse;
    // These three information used to be hold by the reUsableElement
    StreamElementTag tag_ = StreamElementTag::TAG_REC_WITH_TIMESTAMP;
    long timestamp_;
    bool hasTimestamp_ = true;
    bool isDataStream = false;
    // StreamRecord *reusableElement_;
};

#endif // FLINK_TNEL_TIMESTAMPEDCOLLECTOR_H
