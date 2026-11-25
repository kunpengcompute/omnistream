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

#ifndef FLINK_TNEL_SINKFUNCTION_H
#define FLINK_TNEL_SINKFUNCTION_H

#include "table/data/RowData.h"
#include "streaming/api/watermark/Watermark.h"
#include "include/functions/Function.h"

enum SinkInputValueType {
    ROW_DATA = 1,
    VEC_BATCH = 2
};
template <typename IN>
class SinkFunction : public omnistream::Function {
public:
    virtual ~SinkFunction() = default;
    virtual void invoke(IN value, SinkInputValueType valueType) = 0;
    virtual void writeWatermark(Watermark* watermark) = 0;
    virtual void finish() = 0;
};

#endif // FLINK_TNEL_SINKFUNCTION_H
