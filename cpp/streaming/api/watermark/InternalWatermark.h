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

#ifndef FLINK_TNEL_INTERNALWATERMARK_H
#define FLINK_TNEL_INTERNALWATERMARK_H

#include <cstdint>
#include "streaming/api/watermark/Watermark.h"

class InternalWatermark : public Watermark {
public:
    explicit InternalWatermark(int64_t timestamp) : Watermark(timestamp) {};

private:
    int32_t subpartitionIndex;
};

#endif // FLINK_TNEL_INTERNALWATERMARK_H
